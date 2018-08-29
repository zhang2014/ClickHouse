/**
#include <QingCloud/Datastream/QingCloudBlockOutputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>
#include <Parsers/queryToString.h>
#include <Common/MemoryTracker.h>
#include <Common/setThreadName.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <DataStreams/RemoteBlockOutputStream.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/DirectoryIterator.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Common/ClickHouseRevision.h>
#include <Interpreters/createBlockSelector.h>
#include <IO/Operators.h>

namespace CurrentMetrics
{
extern const Metric DistributedSend;
}

namespace DB
{

namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
}

QingCloudBlockOutputStream::QingCloudBlockOutputStream(StorageQingCloud & storage_, UInt64 current_writing_version, ClusterPtr &cluster_,
                                                       const ASTPtr & query_ast_,
                                                       const Context & context_, const Settings &settings_)
    : storage(storage_), current_writing_version(current_writing_version), cluster(cluster_), query_ast(query_ast_), context(context_), settings(settings_)
{
}

void QingCloudBlockOutputStream::write(const Block & block)
{
    const auto & shards_info = cluster->getShardsInfo();
    const auto & addresses_with_failovers = cluster->getShardsAddresses();
    size_t num_shards = shards_info.size();

    if (!pool)
        initWritingJob(block, shards_info, addresses_with_failovers);

    if (num_shards > 1)
    {
        auto current_selector = createSelector(block);

        /// Prepare row numbers for each shard
        for (size_t shard_index : ext::range(0, num_shards))
            per_shard_jobs[shard_index].shard_current_block_permuation.resize(0);

        for (size_t i = 0; i < block.rows(); ++i)
            per_shard_jobs[current_selector[i]].shard_current_block_permuation.push_back(i);
    }

    watch_current_block.restart();
    finished_jobs_count = 0;

    for (size_t shard_index : ext::range(0, shards_info.size()))
    {
        Cluster::Addresses shard_addresses = addresses_with_failovers[shard_index];

        for (JobReplica & job : per_shard_jobs[shard_index].replicas_jobs)
        {
            Cluster::Address replica_address = shard_addresses.at(job.replica_index);

            initializeReplicaJob(job, replica_address, shards_info[shard_index]);
            pool->schedule(createWritingJob(job, per_shard_jobs[shard_index], block, replica_address, num_shards));
        }
    }

    try
    {
        pool->wait();
    }
    catch (Exception & exception)
    {
//        exception.addMessage(getCurrentStateDescription());
        throw;
    }

    inserted_blocks += 1;
    inserted_rows += block.rows();
}

void QingCloudBlockOutputStream::initWritingJob(
    const Block & first_block, const Cluster::ShardsInfo & shards_info, const Cluster::AddressesWithFailover & addresses_with_failovers)
{
    size_t num_shards = shards_info.size();

    jobs_count = 0;
    per_shard_jobs.resize(shards_info.size());

    for (size_t shard_index : ext::range(0, shards_info.size()))
    {
        auto & shard_info = shards_info[shard_index];
        auto & shard_jobs = per_shard_jobs[shard_index];

        auto & replicas = addresses_with_failovers[shard_index];

        for (size_t replica_index : ext::range(0, replicas.size()))
        {
            Settings new_settings = settings;
            new_settings.writing_version = current_writing_version;
            new_settings.writing_shard_index = shard_info.shard_num;
            shard_jobs.replicas_jobs.emplace_back(shard_index, replica_index, replicas[replica_index].is_local, first_block);
            shard_jobs.replicas_jobs.back().local_context = std::make_shared<Context>(context);
            shard_jobs.replicas_jobs.back().local_context->setSettings(new_settings);
            ++jobs_count;
        }

        if (num_shards > 1)
            shard_jobs.shard_current_block_permuation.reserve(first_block.rows());
    }

    pool.emplace(jobs_count);
    query_string = queryToString(query_ast);

    if (!throttler && (settings.max_network_bandwidth || settings.max_network_bytes))
        throttler = std::make_shared<Throttler>(settings.max_network_bandwidth, settings.max_network_bytes, "Network bandwidth limit for a query exceeded.");
}

ThreadPool::Job QingCloudBlockOutputStream::createWritingJob(QingCloudBlockOutputStream::JobReplica &replica_job,
                                                             QingCloudBlockOutputStream::JobShard &shard_job, const Block &current_block,
                                                             const Cluster::Address &replica_address, size_t num_shards)
{
    auto memory_tracker = current_memory_tracker;
    return [this, memory_tracker, &replica_job, &current_block, &replica_address, num_shards, &shard_job]()
    {
        if (!current_memory_tracker)
        {
            current_memory_tracker = memory_tracker;
            setThreadName("QingCloudOutputStreamProc");
        }

        ++replica_job.blocks_started;

        SCOPE_EXIT({
               ++finished_jobs_count;
               UInt64 elapsed_time_for_block_ms =  watch_current_block.elapsedMilliseconds();
               replica_job.elapsed_time_ms += elapsed_time_for_block_ms;
               replica_job.max_elapsed_time_for_block_ms = std::max(replica_job.max_elapsed_time_for_block_ms, elapsed_time_for_block_ms);
        });

        if (num_shards > 1)
        {
            auto & shard_permutation = shard_job.shard_current_block_permuation;
            size_t num_shard_rows = shard_permutation.size();

            for (size_t j = 0; j < current_block.columns(); ++j)
            {
                auto & src_column = current_block.getByPosition(j).column;
                auto & dst_column = replica_job.current_shard_block.getByPosition(j).column;

                /// Zero permutation size has special meaning in IColumn::permute
                if (num_shard_rows)
                    dst_column = src_column->permute(shard_permutation, num_shard_rows);
                else
                    dst_column = src_column->cloneEmpty();
            }
        }

        const Block & shard_block = (num_shards > 1) ? replica_job.current_shard_block : current_block;

        if (synchro_insert)
        {
            try
            {
                if (!replica_job.is_local_job)
                    CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

                replica_job.stream->write(shard_block);
            }
            catch (...)
            {
                if (replica_job.is_local_job)
                    throw;

                tryLogCurrentException("QingCloudBlockOutputStreamBck");
                String file_name = asynchronism.writeTempBlock(query_string, shard_block);
                asynchronism.writeToReplica(current_writing_version, replica_job.shard_index, replica_address, file_name);
            };
        }
    };
}

void QingCloudBlockOutputStream::writeSuffix()
{
    for (auto & shard_jobs : per_shard_jobs)
        for (JobReplica & job : shard_jobs.replicas_jobs)
        {
            if (job.stream)
            {
                pool->schedule([&job] ()
                               {
                                   job.stream->writeSuffix();
                               });
            }
        }

    try
    {
        pool->wait();
    }
    catch (Exception & exception)
    {
        /// TODO:
//        exception.addMessage(getCurrentStateDescription());
        throw;
    }
}

void QingCloudBlockOutputStream::initializeReplicaJob(QingCloudBlockOutputStream::JobReplica & replica_job, Cluster::Address & replica_address,
                                                      const Cluster::ShardInfo & shard_info)
{
    if (replica_job.is_local_job)
    {
        ASTPtr ast_insert = query_ast->clone();
        static_cast<ASTInsertQuery *>(ast_insert.get())->select = {};
        InterpreterInsertQuery interp(ast_insert, *replica_job.local_context);
        replica_job.stream = interp.execute().out;
        replica_job.stream->writePrefix();
    }
    else
    {
        Settings & current_settings = replica_job.local_context->getSettingsRef();
        const ConnectionPoolPtr & connection_pool = shard_info.per_replica_pools.at(replica_job.replica_index);
        if (!connection_pool)
            throw Exception("Connection pool for replica " + replica_address.readableString() + " does not exist", ErrorCodes::LOGICAL_ERROR);

        replica_job.connection_entry = connection_pool->get(&current_settings);
        if (replica_job.connection_entry.isNull())
            throw Exception("Got empty connection for replica" + replica_address.readableString(), ErrorCodes::LOGICAL_ERROR);

        if (throttler)
            replica_job.connection_entry.operator->().setThrottler(throttler);

        replica_job.stream = std::make_shared<RemoteBlockOutputStream>(*replica_job.connection_entry, query_string, &current_settings);
        replica_job.stream->writePrefix();
    }
}

Block QingCloudBlockOutputStream::getHeader() const
{
    return storage.getSampleBlock();
}

IColumn::Selector QingCloudBlockOutputStream::createSelector(const Block & block)
{
    Block current_block_with_sharding_key_expr = block;
    storage.sharding_key_expr->execute(current_block_with_sharding_key_expr);

    const auto & key_column = current_block_with_sharding_key_expr.getByName(storage.sharding_key_column_name);
    const auto & slot_to_shard = cluster->getSlotToShard();

 #define CREATE_FOR_TYPE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(key_column.type.get())) \
        return createBlockSelector<TYPE>(*key_column.column, slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}

}
*/