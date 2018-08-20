#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Parsers/queryToString.h>
#include <QingCloud/Interpreters/InterpreterQingCloudDDLQuery.h>

DB::BlockIO DB::InterpreterQingCloudDDLQuery::execute()
{
    const auto cluster = context.getCluster("QingCloudCluster");

    if (!local_interpreter || !cluster)
        return {};

    auto each_all_replica_of_shard = [&cluster](BlockInputStreamPtr (*func)(const ConnectionPoolPtr &)) {
        BlockInputStreams streams;
        for (const auto & shard_info : cluster->getShardsInfo())
            for (const auto & replica_pool : shard_info.per_replica_pools)
                streams.emplace_back(std::move(func(replica_pool)));

        return streams;
    };


    BlockIO res = local_interpreter->execute();

    if (res.in)
    {
        BlockInputStreamPtr input = res.in;

        /// TODO: thread pool
        BlockInputStreams streams = each_all_replica_of_shard([&input](const ConnectionPoolPtr & replica_pool) -> BlockInputStreamPtr {
            /// TODO: retries
            Settings settings = context.getSettings();
            ConnectionPool::Entry connection = replica_pool->get(&settings);
            auto stream = std::make_shared<RemoteBlockInputStream>(connection, query, input->getHeader(), context, nullptr);
            stream->setPoolMode(PoolMode::GET_MANY);
            return stream;
        });

        streams.emplace_back(std::move(input));
        res.in = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, 1);
    }
    return res;
}
