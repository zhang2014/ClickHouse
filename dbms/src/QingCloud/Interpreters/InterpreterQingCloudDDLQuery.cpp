#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Parsers/queryToString.h>
#include <Client/ConnectionPool.h>
#include <QingCloud/Interpreters/MultiplexedVersionCluster.h>
#include <QingCloud/Interpreters/InterpreterQingCloudDDLQuery.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>


namespace DB
{

QingCloudDDLBlockInputStream::QingCloudDDLBlockInputStream(const String & ddl_query, Context & context)
    : ddl_query(ddl_query), context(context), synchronism(context.getDDLSynchronism())
{
}

BlockIO InterpreterQingCloudDDLQuery::execute()
{
    BlockIO res;
    res.in = std::make_shared<QingCloudDDLBlockInputStream>(queryToString(query), context);
    return res;
}

InterpreterQingCloudDDLQuery::InterpreterQingCloudDDLQuery(Context & context, ASTPtr & query)
    : context(context), query(query)
{
}

Block QingCloudDDLBlockInputStream::readImpl()
{
    if (!is_enqueue)
    {
        is_enqueue = true;
        if (UInt64 entity_id = synchronism->enqueue(ddl_query, [this](){ return isCancelled();}))
        {
            synchronism->waitNotify(entity_id, [this](){ return isCancelled();});
            /// TODO: 监听队列查看完成的情况

            /// TODO
            /// host          result          message
            /// other         success
            /// 192.168.1.1   failure         table already exists.
        }
    }

    return {};
}

String QingCloudDDLBlockInputStream::getName() const
{
    return "QingCloudDDLBlockInputStream";
}

Block QingCloudDDLBlockInputStream::getHeader() const
{
    return Block{
        {ColumnString::create(), std::make_shared<DataTypeString>(), "host"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "execute_result"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "message"}
    };
}

}
