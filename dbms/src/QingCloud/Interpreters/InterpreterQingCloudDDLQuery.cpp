#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Parsers/queryToString.h>
#include <Client/ConnectionPool.h>
#include <QingCloud/Interpreters/MultiplexedVersionCluster.h>
#include <QingCloud/Interpreters/InterpreterQingCloudDDLQuery.h>
#include <DataStreams/OneBlockInputStream.h>

namespace DB
{

BlockIO InterpreterQingCloudDDLQuery::execute()
{
    String query_string = queryToString(query);

    const auto ddl_synchronism = context.getDDLSynchronism();
    ddl_synchronism->enqueue(query_string);
//    std::vector<IConnectionPool::Entry> connections;
//    for (const auto & address_and_connections : address_and_connection_pools)
//    {
//        if (!address_and_connections.first.is_local)
//        {
//            settings.internal_query = true;
//            connections.emplace_back(address_and_connections.second->get(&settings));
//        }
//    }
//
//    BlockIO res = local_interpreter->execute();
//
//    if (res.in)
//    header = res.in->getHeader();
//
//    BlockInputStreams streams;
//    for (auto & connection : connections)
//    {
//        auto stream = std::make_shared<RemoteBlockInputStream>(*connection, query_string, header, context, &settings)
//        streams.emplace_back(stream);
//    }


    /// TODO make ddl block input stream
    /// header  :
    /// host          result          message
    /// other         success
    /// 192.168.1.1   failure         table already exists.
//    res.in = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, 1);

    return {};
}

InterpreterQingCloudDDLQuery::InterpreterQingCloudDDLQuery(std::unique_ptr<IInterpreter> local_interpreter_, Context & context, ASTPtr & query)
    : context(context), query(query)
{
    local_interpreter = std::move(local_interpreter_);
}

}
