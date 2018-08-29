#include <QingCloud/Interpreters/ClusterProxy/QingCloudSelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Common/typeid_cast.h>
#include <DataStreams/RemoteBlockInputStream.h>

namespace DB
{

ClusterProxy::QingCloudSelectStreamFactory::QingCloudSelectStreamFactory(
    QueryProcessingStage::Enum processed_stage_, QualifiedTableName main_table_,
    const Tables & external_tables_, UInt64 query_version_, UInt64 query_shard_number_)
    : processed_stage{processed_stage_}, main_table(std::move(main_table_)), table_func_ptr{nullptr},
      external_tables{external_tables_}, query_version(query_version_), query_shard_number(query_shard_number_)
{
}

void ClusterProxy::QingCloudSelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info, const String & query, const ASTPtr & query_ast, const Context & context,
    const ThrottlerPtr & throttler, BlockInputStreams & res)
{
    if (!query_shard_number || shard_info.shard_num == query_shard_number)
    {
        /// TODO: Lazy init new_context;
        Settings new_settings = context.getSettingsRef();

        new_settings.internal_query = true;
        new_settings.query_version = query_version;
        new_settings.query_shard_index = shard_info.shard_num;

        Context new_context{context};
        new_context.setSettings(new_settings);

        auto interpreter = InterpreterSelectQuery(query_ast, new_context, Names{}, processed_stage);

        Block header = interpreter.getSampleBlock();
        auto stream = makeOriginRemoteBlockInputStream(header, shard_info, query, new_context, throttler);
        auto qingcloud_stream = std::make_shared<QingCloudRemoteBlockInputStream>(stream, query_version, shard_info.shard_num, new_settings);
        res.emplace_back(std::move(qingcloud_stream));
    }
}

BlockInputStreamPtr ClusterProxy::QingCloudSelectStreamFactory::makeOriginRemoteBlockInputStream(
    Block &header, const Cluster::ShardInfo &shard_info, const String &query, const Context &context, const ThrottlerPtr &throttler)
{
    auto stream = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, header, context, nullptr, throttler, external_tables, processed_stage);

    stream->setPoolMode(DB::PoolMode::GET_MANY);
    stream->setMainTable(main_table);
    return std::move(stream);
}

ClusterProxy::QingCloudRemoteBlockInputStream::QingCloudRemoteBlockInputStream(
    const BlockInputStreamPtr & input, UInt64 & version_, UInt64 shard_num_, Settings & settings_)
    : version(version_), shard_num(shard_num_), settings(settings_)
{
    children.push_back(input);
}

Block ClusterProxy::QingCloudRemoteBlockInputStream::readImpl()
{
    settings.internal_query = true;
    settings.query_version = version;
    settings.query_shard_index = shard_num;
    return children.back()->read();
}

void ClusterProxy::QingCloudRemoteBlockInputStream::readPrefixImpl()
{
    settings.internal_query = true;
    settings.query_version = version;
    settings.query_shard_index = shard_num;
}

void ClusterProxy::QingCloudRemoteBlockInputStream::readSuffixImpl()
{
    settings.internal_query = true;
    settings.query_version = version;
    settings.query_shard_index = shard_num;
}

Block ClusterProxy::QingCloudRemoteBlockInputStream::getHeader() const
{
    return children.back()->getHeader();
}

String ClusterProxy::QingCloudRemoteBlockInputStream::getName() const { return "QingCloudRemote"; }

}
