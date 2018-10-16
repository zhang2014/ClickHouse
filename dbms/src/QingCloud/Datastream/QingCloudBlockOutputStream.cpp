#include <QingCloud/Datastream/QingCloudBlockOutputStream.h>
#include <Storages/Distributed/DirectoryMonitor.h>

#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/RemoteBlockOutputStream.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/createBlockSelector.h>

#include <DataTypes/DataTypesNumber.h>
#include <Common/setThreadName.h>
#include <Common/ClickHouseRevision.h>
#include <Common/CurrentMetrics.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/MemoryTracker.h>
#include <Common/escapeForFileName.h>
#include <common/logger_useful.h>
#include <ext/range.h>
#include <ext/scope_guard.h>

#include <Poco/DirectoryIterator.h>

#include <future>
#include <condition_variable>
#include <mutex>


namespace CurrentMetrics
{
extern const Metric DistributedSend;
}

namespace ProfileEvents
{
extern const Event DistributedSyncInsertionTimeoutExceeded;
}

namespace DB
{


namespace ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
extern const int TYPE_MISMATCH;
}


QingCloudBlockOutputStream::QingCloudBlockOutputStream(
    const Context & context, QingCloudAsynchronism & asynchronism, const ASTPtr & query_ast, const ClusterPtr & cluster_,
    const Settings & settings_, bool insert_sync_, UInt64 insert_timeout_, const String &sharding_column_name_,
    const ExpressionActionsPtr &sharding_key_expr, const Block &header, const String & current_writing_version_)
    : context(context), asynchronism(asynchronism), query_ast(query_ast), cluster(cluster_),
      current_writing_version(current_writing_version_), header(header), settings(settings_), sharding_column_name(sharding_column_name_),
      sharding_key_expr(sharding_key_expr), insert_sync(insert_sync_), insert_timeout(insert_timeout_),
      log(&Logger::get("QingCloudBlockOutputStream"))
{
}

Block QingCloudBlockOutputStream::getHeader() const
{
    return header;
}


void QingCloudBlockOutputStream::writePrefix()
{
}


void QingCloudBlockOutputStream::write(const Block & block)
{
    if (insert_sync)
        writeSync(block);
    else
        writeAsync(block);
}


void QingCloudBlockOutputStream::writeAsync(const Block & block)
{
    if (sharding_key_expr && (cluster->getShardsInfo().size() > 1))
        return writeSplitAsync(block);

    writeAsyncImpl(block);
    ++inserted_blocks;
}


std::string QingCloudBlockOutputStream::getCurrentStateDescription()
{
    std::stringstream buffer;
    const auto & addresses = cluster->getShardsAddresses();

    buffer << "Insertion status:\n";
    for (auto & shard_jobs : per_shard_jobs)
        for (JobReplica & job : shard_jobs.replicas_jobs)
        {
            buffer << "Wrote " << job.blocks_written << " blocks and " << job.rows_written << " rows"
                   << " on shard " << job.shard_index << " replica " << job.replica_index
                   << ", " << addresses[job.shard_index][job.replica_index].readableString();

            /// Performance statistics
            if (job.blocks_started > 0)
            {
                buffer << " (average " << job.elapsed_time_ms / job.blocks_started << " ms per block";
                if (job.blocks_started > 1)
                    buffer << ", the slowest block " << job.max_elapsed_time_for_block_ms << " ms";
                buffer << ")";
            }

            buffer << "\n";
        }

    return buffer.str();
}


void QingCloudBlockOutputStream::initWritingJobs(const Block & first_block)
{
    const auto & addresses_with_failovers = cluster->getShardsAddresses();
    const auto & shards_info = cluster->getShardsInfo();
    size_t num_shards = shards_info.size();

    remote_jobs_count = 0;
    local_jobs_count = 0;
    per_shard_jobs.resize(shards_info.size());

    for (size_t shard_index : ext::range(0, shards_info.size()))
    {
        const auto & shard_info = shards_info[shard_index];
        auto & shard_jobs = per_shard_jobs[shard_index];

        const auto & replicas = addresses_with_failovers[shard_index];

        for (size_t replica_index : ext::range(0, replicas.size()))
        {
            Settings new_settings = settings;
            new_settings.writing_version = current_writing_version;
            new_settings.writing_shard_index = shard_info.shard_num;
            shard_jobs.replicas_jobs.emplace_back(shard_index, replica_index, replicas[replica_index].is_local, first_block);
            shard_jobs.replicas_jobs.back().local_context = std::make_shared<Context>(context);
            shard_jobs.replicas_jobs.back().local_context->setSettings(new_settings);

            if (replicas[replica_index].is_local)
                ++local_jobs_count;
            else
                ++remote_jobs_count;
        }

        if (num_shards > 1)
            shard_jobs.shard_current_block_permuation.reserve(first_block.rows());
    }
}


void QingCloudBlockOutputStream::waitForJobs()
{
    pool->wait();

    if (insert_timeout)
    {
        if (static_cast<UInt64>(watch.elapsedSeconds()) > insert_timeout)
        {
            ProfileEvents::increment(ProfileEvents::DistributedSyncInsertionTimeoutExceeded);
            throw Exception("Synchronous distributed insert timeout exceeded.", ErrorCodes::TIMEOUT_EXCEEDED);
        }
    }

    size_t jobs_count = remote_jobs_count + local_jobs_count;
    size_t num_finished_jobs = finished_jobs_count;

    if (num_finished_jobs < jobs_count)
        LOG_WARNING(log, "Expected " << jobs_count << " writing jobs, but finished only " << num_finished_jobs);
}


ThreadPool::Job QingCloudBlockOutputStream::runWritingJob(QingCloudBlockOutputStream::JobReplica & job, const Block & current_block)
{
    auto memory_tracker = current_memory_tracker;
    return [this, memory_tracker, &job, &current_block]()
    {
        ++job.blocks_started;

        SCOPE_EXIT({
                       ++finished_jobs_count;

            UInt64 elapsed_time_for_block_ms = watch_current_block.elapsedMilliseconds();
            job.elapsed_time_ms += elapsed_time_for_block_ms;
            job.max_elapsed_time_for_block_ms = std::max(job.max_elapsed_time_for_block_ms, elapsed_time_for_block_ms);
                   });

        if (!current_memory_tracker)
        {
            current_memory_tracker = memory_tracker;
            setThreadName("DistrOutStrProc");
        }

        const auto & shard_info = cluster->getShardsInfo()[job.shard_index];
        size_t num_shards = cluster->getShardsInfo().size();
        auto & shard_job = per_shard_jobs[job.shard_index];
        const auto & addresses = cluster->getShardsAddresses();

        /// Generate current shard block
        if (num_shards > 1)
        {
            auto & shard_permutation = shard_job.shard_current_block_permuation;
            size_t num_shard_rows = shard_permutation.size();

            for (size_t j = 0; j < current_block.columns(); ++j)
            {
                auto & src_column = current_block.getByPosition(j).column;
                auto & dst_column = job.current_shard_block.getByPosition(j).column;

                /// Zero permutation size has special meaning in IColumn::permute
                if (num_shard_rows)
                    dst_column = src_column->permute(shard_permutation, num_shard_rows);
                else
                    dst_column = src_column->cloneEmpty();
            }
        }

        const Block & shard_block = (num_shards > 1) ? job.current_shard_block : current_block;

        if (!job.is_local_job)
        {
            if (!job.stream)
            {
                Settings & current_settings = job.local_context->getSettingsRef();
                const auto & replica = addresses.at(job.shard_index).at(job.replica_index);

                const ConnectionPoolPtr & connection_pool = shard_info.per_replica_pools.at(job.replica_index);
                if (!connection_pool)
                    throw Exception("Connection pool for replica " + replica.readableString() + " does not exist", ErrorCodes::LOGICAL_ERROR);

                job.connection_entry = connection_pool->get(&current_settings);
                if (job.connection_entry.isNull())
                    throw Exception("Got empty connection for replica" + replica.readableString(), ErrorCodes::LOGICAL_ERROR);

                if (throttler)
                    job.connection_entry->setThrottler(throttler);

                job.stream = std::make_shared<RemoteBlockOutputStream>(*job.connection_entry, query_string, &current_settings);
                job.stream->writePrefix();
            }

            CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};
            job.stream->write(shard_block);
        }
        else
        {
            if (!job.stream)
            {
                /// Forward user settings
                ASTPtr ast_insert = query_ast->clone();
                static_cast<ASTInsertQuery *>(ast_insert.get())->select = {};
                InterpreterInsertQuery interp(ast_insert, *job.local_context);
                job.stream = interp.execute().out;
                job.stream->writePrefix();
            }

            job.stream->write(shard_block);
        }

        job.blocks_written += 1;
        job.rows_written += shard_block.rows();
    };
}


void QingCloudBlockOutputStream::writeSync(const Block & block)
{
    const auto & shards_info = cluster->getShardsInfo();
    size_t num_shards = shards_info.size();

    if (!pool)
    {
        /// Deferred initialization. Only for sync insertion.
        initWritingJobs(block);

        pool.emplace(remote_jobs_count + local_jobs_count);
        query_string = queryToString(query_ast);

        if (!throttler && (settings.max_network_bandwidth || settings.max_network_bytes))
        {
            throttler = std::make_shared<Throttler>(settings.max_network_bandwidth, settings.max_network_bytes,
                                                    "Network bandwidth limit for a query exceeded.");
        }

        watch.restart();
    }

    watch_current_block.restart();

    if (num_shards > 1)
    {
        auto current_selector = createSelector(block);

        /// Prepare row numbers for each shard
        for (size_t shard_index : ext::range(0, num_shards))
            per_shard_jobs[shard_index].shard_current_block_permuation.resize(0);

        for (size_t i = 0; i < block.rows(); ++i)
            per_shard_jobs[current_selector[i]].shard_current_block_permuation.push_back(i);
    }

    /// Run jobs in parallel for each block and wait them
    finished_jobs_count = 0;
    for (size_t shard_index : ext::range(0, shards_info.size()))
        for (JobReplica & job : per_shard_jobs[shard_index].replicas_jobs)
            pool->schedule(runWritingJob(job, block));

    try
    {
        waitForJobs();
    }
    catch (Exception & exception)
    {
        exception.addMessage(getCurrentStateDescription());
        throw;
    }

    inserted_blocks += 1;
    inserted_rows += block.rows();
}


void QingCloudBlockOutputStream::writeSuffix()
{
    auto log_performance = [this] ()
    {
        double elapsed = watch.elapsedSeconds();
        LOG_DEBUG(log, "It took " << std::fixed << std::setprecision(1) << elapsed << " sec. to insert " << inserted_blocks << " blocks"
                                  << ", " << std::fixed << std::setprecision(1) << inserted_rows / elapsed << " rows per second"
                                  << ". " << getCurrentStateDescription());
    };

    if (insert_sync && pool)
    {
        finished_jobs_count = 0;
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
            log_performance();
        }
        catch (Exception & exception)
        {
            log_performance();
            exception.addMessage(getCurrentStateDescription());
            throw;
        }
    }
}


IColumn::Selector QingCloudBlockOutputStream::createSelector(const Block & source_block)
{
    Block current_block_with_sharding_key_expr = source_block;
    sharding_key_expr->execute(current_block_with_sharding_key_expr);

    const auto & key_column = current_block_with_sharding_key_expr.getByName(sharding_column_name);
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


Blocks QingCloudBlockOutputStream::splitBlock(const Block & block)
{
    auto selector = createSelector(block);

    /// Split block to num_shard smaller block, using 'selector'.

    const size_t num_shards = cluster->getShardsInfo().size();
    Blocks splitted_blocks(num_shards);

    for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
        splitted_blocks[shard_idx] = block.cloneEmpty();

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        MutableColumns splitted_columns = block.getByPosition(col_idx_in_block).column->scatter(num_shards, selector);
        for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
            splitted_blocks[shard_idx].getByPosition(col_idx_in_block).column = std::move(splitted_columns[shard_idx]);
    }

    return splitted_blocks;
}


void QingCloudBlockOutputStream::writeSplitAsync(const Block & block)
{
    Blocks splitted_blocks = splitBlock(block);
    const size_t num_shards = splitted_blocks.size();

    for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
        if (splitted_blocks[shard_idx].rows())
            writeAsyncImpl(splitted_blocks[shard_idx], shard_idx);

    ++inserted_blocks;
}


void QingCloudBlockOutputStream::writeAsyncImpl(const Block & block, const size_t shard_id)
{
    bool has_remote_replica = false;
    const auto & shard_info = cluster->getShardsInfo()[shard_id];

    for (const auto & address : cluster->getShardsAddresses()[shard_id])
    {
        has_remote_replica |= !address.is_local;
        if (address.is_local)
        {
            Context current_context = context;
            current_context.getSettingsRef().writing_version = current_writing_version;
            current_context.getSettingsRef().writing_shard_index = shard_info.shard_num;
            writeToLocal(block, 1, current_context);
        }

    }

    if (has_remote_replica)
    {
        String file_name = asynchronism.writeTempBlock(query_string, block);
        asynchronism.writeToShard(current_writing_version, shard_info.shard_num, file_name);
//        asynchronism.destroyTempBlock(file_name);
    }
}


void QingCloudBlockOutputStream::writeToLocal(const Block &block, const size_t repeats, const Context &context)
{
    /// Async insert does not support settings forwarding yet whereas sync one supports
    InterpreterInsertQuery interp(query_ast, context);

    auto block_io = interp.execute();
    block_io.out->writePrefix();

    for (size_t i = 0; i < repeats; ++i)
        block_io.out->write(block);

    block_io.out->writeSuffix();
}

}
