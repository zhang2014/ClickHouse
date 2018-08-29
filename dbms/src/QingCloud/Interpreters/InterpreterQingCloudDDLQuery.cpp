#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Parsers/queryToString.h>
#include <QingCloud/Interpreters/InterpreterQingCloudDDLQuery.h>

namespace DB
{

BlockIO InterpreterQingCloudDDLQuery::execute()
{
    Settings & settings = context.getSettingsRef();
    const auto multiplexed_version_cluster = context.getMultiplexedVersion();
    const auto address_and_connection_pools = multiplexed_version_cluster->getAddressesAndConnections();

    Block header;
    String query_string = queryToString(query);

    BlockIO res = local_interpreter->execute();

    if (res.in)
        header = res.in->getHeader();

    BlockInputStreams streams;
    for (const auto & address_and_connections : address_and_connection_pools)
    {
        if (!address_and_connections.first.is_local)
        {
            settings.internal_query = true;
            const auto connections = address_and_connections.second;
            ConnectionPool::Entry connection = connections->get(&settings);
            auto stream = std::make_shared<RemoteBlockInputStream>(*connection, query_string, header, context, &settings);
            streams.emplace_back(std::move(stream));
        }
    }
    /// TODO make ddl block input stream
    /// header  :
    /// host          result          message
    /// other         success
    /// 192.168.1.1   failure         table already exists.
//    res.in = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, 1);

    return res;
}

InterpreterQingCloudDDLQuery::InterpreterQingCloudDDLQuery(std::unique_ptr<IInterpreter> local_interpreter_, Context & context, ASTPtr & query)
    : context(context), query(query)
{
    local_interpreter = std::move(local_interpreter_);
}

}
