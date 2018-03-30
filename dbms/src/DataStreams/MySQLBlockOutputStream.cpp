#include <DataStreams/MySQLBlockOutputStream.h>
#include <Dictionaries/ExternalResultDescription.h>

namespace DB
{

using ValueType = ExternalResultDescription::ValueType;

MySQLBlockOutputStream::MySQLBlockOutputStream(const mysqlxx::PoolWithFailover::Entry &entry, const std::string &query_str)
    : entry{entry}, prepared_query{this->entry->preparedQuery(query_str)}
{
}

void MySQLBlockOutputStream::write(const Block &block)
{
    block.checkNumberOfRows();

    /// TODO:开启事物
    for (size_t row_num = 0; row_num < block.rows(); ++row_num)
    {
        for (size_t column_num = 0; column_num < block.columns(); ++column_num)
        {
            Field field;
            const ColumnWithTypeAndName & column = block.safeGetByPosition(column_num);
            column.column->get(row_num, field);

            //*column.column->
            /// TODO: attach bind
            writeData(column.name, *column.type, *column.column, marks, written_streams);
        }
        /// TODO: execute
    }
    /// TODO: 提交事物

}

}

