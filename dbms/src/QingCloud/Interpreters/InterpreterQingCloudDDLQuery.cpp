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
            const auto wait_res_ptr = synchronism->getWaitApplyRes(entity_id);
            synchronism->getWaitApplyRes(entity_id)->wait(std::chrono::milliseconds(180000));
            synchronism->releaseApplyRes(entity_id);

            Block res = getHeader();

            MutableColumns columns = res.cloneEmptyColumns();
            for (const auto & wait_res : wait_res_ptr->paxos_res)
            {
                UInt64 res_state = std::get<0>(wait_res);
                const String & from = std::get<2>(wait_res);
                const String & exception_message = std::get<1>(wait_res);

                columns[0]->insert(from);
                columns[1]->insert(res_state ? "failure" : "success");
                columns[2]->insert(exception_message);
            }

            return res.cloneWithColumns(std::move(columns));
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
