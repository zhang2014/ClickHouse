#include <Storages/StorageMySQL.h>

#if USE_MYSQL

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Dictionaries/MySQLBlockInputStream.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Context.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Common/parseAddress.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <mysqlxx/Transaction.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


StorageMySQL::StorageMySQL(const std::string & name,
    mysqlxx::Pool && pool,
    const std::string & remote_database_name,
    const std::string & remote_table_name,
    const bool replace_query,
    const std::string & on_duplicate_clause,
    const ColumnsDescription & columns_,
    const Context & context)
    : IStorage{columns_}
    , name(name)
    , remote_database_name(remote_database_name)
    , remote_table_name(remote_table_name)
    , replace_query{replace_query}
    , on_duplicate_clause{on_duplicate_clause}
    , pool(std::move(pool))
    , context(context)
{
}


BlockInputStreams StorageMySQL::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned)
{
    check(column_names);
    String query = transformQueryForExternalDatabase(
        *query_info.query, getColumns().ordinary, IdentifierQuotingStyle::Backticks, remote_database_name, remote_table_name, context);

    Block sample_block;
    for (const String & name : column_names)
    {
        auto column_data = getColumn(name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return { std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size) };
}


class StorageMySQLBlockOutputStream : public IBlockOutputStream
{
public:
    explicit StorageMySQLBlockOutputStream(const StorageMySQL & storage,
        const std::string & remote_database_name,
        const std::string & remote_table_name ,
        const mysqlxx::PoolWithFailover::Entry & entry,
        const size_t & mysql_max_rows_to_insert)
        : storage{storage}
        , remote_database_name{remote_database_name}
        , remote_table_name{remote_table_name}
        , entry{entry}
        , max_batch_rows{mysql_max_rows_to_insert}
    {
    }

    Block getHeader() const override { return storage.getSampleBlock(); }

    void write(const Block & block) override
    {
        auto blocks = splitBlocks(block, max_batch_rows);
        mysqlxx::Transaction trans(entry);
        try
        {
            for (const Block & batch_data : blocks)
            {
                writeBlockData(batch_data);
            }
            trans.commit();
        }
        catch (...)
        {
            trans.rollback();
            throw;
        }
    }

    void writeBlockData(const Block & block)
    {
        WriteBufferFromOwnString sqlbuf;
        sqlbuf << (storage.replace_query ? "REPLACE" : "INSERT") << " INTO ";
        sqlbuf << backQuoteIfNeed(remote_database_name) << "." << backQuoteIfNeed(remote_table_name);
        sqlbuf << " (" << dumpNamesWithBackQuote(block) << ") VALUES ";

        auto writer = FormatFactory::instance().getOutput("Values", sqlbuf, storage.getSampleBlock(), storage.context);
        writer->write(block);

        if (!storage.on_duplicate_clause.empty())
            sqlbuf << " ON DUPLICATE KEY " << storage.on_duplicate_clause;

        sqlbuf << ";";

        auto query = this->entry->query(sqlbuf.str());
        query.execute();
    }

    Blocks splitBlocks(const Block & block, const size_t & max_rows) const
    {
        /// Avoid Excessive copy when block is small enough
        if (block.rows() <= max_rows)
            return Blocks{std::move(block)};

        const size_t splited_block_size = ceil(block.rows() * 1.0 / max_rows);
        Blocks splitted_blocks(splited_block_size);

        for (size_t idx = 0; idx < splited_block_size; ++idx)
            splitted_blocks[idx] = block.cloneEmpty();

        const size_t columns = block.columns();
        const size_t rows = block.rows();
        size_t offsets = 0;
        size_t limits = max_batch_rows;
        for (size_t idx = 0; idx < splited_block_size; ++idx)
        {
            /// For last batch, limits should be the remain size
            if (idx == splited_block_size - 1) limits = rows - offsets;
            for (size_t col_idx = 0; col_idx < columns; ++col_idx)
            {
                splitted_blocks[idx].getByPosition(col_idx).column = block.getByPosition(col_idx).column->cut(offsets, limits);
            }
            offsets += max_batch_rows;
        }

        return splitted_blocks;
    }

    std::string dumpNamesWithBackQuote(const Block & block) const
    {
        WriteBufferFromOwnString out;
        for (auto it = block.begin(); it != block.end(); ++it)
        {
            if (it != block.begin())
                out << ", ";
            out << backQuoteIfNeed(it->name);
        }
        return out.str();
    }


private:
    const StorageMySQL & storage;
    std::string remote_database_name;
    std::string remote_table_name;
    mysqlxx::PoolWithFailover::Entry entry;
    size_t max_batch_rows;
};


BlockOutputStreamPtr StorageMySQL::write(
    const ASTPtr & /*query*/, const Settings & settings)
{
    return std::make_shared<StorageMySQLBlockOutputStream>(*this, remote_database_name, remote_table_name, pool.Get(), settings.mysql_max_rows_to_insert);
}

void registerStorageMySQL(StorageFactory & factory)
{
    factory.registerStorage("MySQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        MySQLSettings settings;
        settings.loadFromConfig(args.context.getConfigRef(), args.database_name, args.table_name);
        settings.loadFromEngineArguments(engine_args, args.local_context);
        settings.loadFromQuery(*args.storage_def);

        if (settings.remote_address.value.empty() || settings.remote_database.value.empty() ||
            settings.remote_table_name.value.empty() || settings.user.value.empty() || settings.password.value.empty())
            throw Exception(
                "Storage MySQL requires 5-7 parameters: MySQL('host:port', database, table, 'user', 'password'[, replace_query, 'on_duplicate_clause']).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (settings.replace_query && !settings.on_duplicate_clause.value.empty())
            throw Exception("Only one of 'replace_query' and 'on_duplicate_clause' can be specified, or none of them",
                            ErrorCodes::BAD_ARGUMENTS);

        std::stringstream initialize_query_stream;
        IAST::FormatSettings format_settings(initialize_query_stream, true);
        format_settings.always_quote_identifiers = true;
        format_settings.identifier_quoting_style = IdentifierQuotingStyle::Backticks;

        settings.set_variables_query->format(format_settings);
        String initialize_query = initialize_query_stream.str();
        const auto host_and_port = parseAddress(settings.remote_address.value, 3306);
        mysqlxx::Pool pool(settings.remote_database.value, host_and_port.first, settings.user.value, settings.password.value,
                           host_and_port.second, settings.set_variables_query->changes.empty() ?  "" : initialize_query);

        return StorageMySQL::create(args.table_name, std::move(pool), settings.remote_database.value, settings.remote_table_name.value,
                                    bool(settings.replace_query.value), settings.on_duplicate_clause.value, args.columns, args.context);
    });
}

}

#endif
