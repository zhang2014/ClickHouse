#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Parsers/queryToString.h>
#include <Client/ConnectionPool.h>
#include <QingCloud/Interpreters/MultiplexedVersionCluster.h>
#include <QingCloud/Interpreters/InterpreterQingCloudDDLQuery.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include "InterpreterQingCloudDDLQuery.h"


namespace DB
{

QingCloudDDLBlockInputStream::QingCloudDDLBlockInputStream(const String & ddl_query, Context & context, QingCloudDDLSynchronismPtr & synchronism)
    : ddl_query(ddl_query), context(context), synchronism(synchronism)
{
}

BlockIO InterpreterQingCloudDDLQuery::execute()
{
    BlockIO res;
    res.in = std::make_shared<QingCloudDDLBlockInputStream>(queryToString(query), context, context.getDDLSynchronism());
    return res;
}

InterpreterQingCloudDDLQuery::InterpreterQingCloudDDLQuery(std::unique_ptr<IInterpreter> local_interpreter_, Context & context, ASTPtr & query)
    : context(context), query(query)
{
    local_interpreter = std::move(local_interpreter_);
}

Block QingCloudDDLBlockInputStream::readImpl()
{
    if (!is_enqueue)
    {
        is_enqueue = true;
        if (synchronism->enqueue(ddl_query, [this](){ return isCancelled();}))
        {
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
