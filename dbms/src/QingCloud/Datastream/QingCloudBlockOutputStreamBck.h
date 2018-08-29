/**
#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Cluster.h>
#include <common/ThreadPool.h>
#include <Parsers/IAST.h>
#include <Interpreters/Context.h>
#include <QingCloud/Storages/StorageQingCloud.h>
#include "QingCloudAsynchronism.h"

namespace DB
{

class QingCloudBlockOutputStreamBck : public IBlockOutputStream
{

public:
    QingCloudBlockOutputStreamBck(StorageQingCloud & storage_, UInt64 current_writing_version, ClusterPtr & cluster, const ASTPtr & query_ast,
                               const Context &context, const Settings &settings);

    void writeSuffix() override;

    Block getHeader() const override;

    void write(const Block & block) override;

protected:
    IColumn::Selector createSelector(const Block & block);

private:
    StorageQingCloud & storage;
    UInt64 current_writing_version;
    ClusterPtr cluster;
    const ASTPtr & query_ast;
    const Context & context;
    const Settings & settings;
    QingCloudAsynchronism & asynchronism;
    bool synchro_insert;


    struct JobReplica
    {
        JobReplica() = default;
        JobReplica(size_t shard_index, size_t replica_index, bool is_local_job, const Block & sample_block)
            : shard_index(shard_index), replica_index(replica_index), is_local_job(is_local_job), current_shard_block(sample_block.cloneEmpty()) {}

        size_t shard_index = 0;
        size_t replica_index = 0;
        bool is_local_job = false;

        Block current_shard_block;

        ConnectionPool::Entry connection_entry;
        std::shared_ptr<Context> local_context;
        BlockOutputStreamPtr stream;

        UInt64 blocks_written = 0;
        UInt64 rows_written = 0;

        UInt64 blocks_started = 0;
        UInt64 elapsed_time_ms = 0;
        UInt64 max_elapsed_time_for_block_ms = 0;
    };

    struct JobShard
    {
        std::list<JobReplica> replicas_jobs;
        IColumn::Permutation shard_current_block_permuation;
    };

    std::vector<JobShard> per_shard_jobs;

    void initWritingJob(const Block &first_block, const Cluster::ShardsInfo &shards_info,
                        const Cluster::AddressesWithFailover &addresses_with_failovers);

    size_t jobs_count;
    size_t finished_jobs_count;

    String query_string;
    std::optional<ThreadPool> pool;
    std::shared_ptr<Throttler> throttler;
    Stopwatch watch_current_block;

    size_t inserted_rows;
    size_t inserted_blocks;

    ThreadPool::Job createWritingJob(QingCloudBlockOutputStreamBck::JobReplica &replica_job,
                                         QingCloudBlockOutputStreamBck::JobShard &shard_job, const Block &current_block,
                                         const Cluster::Address &replica_address, size_t num_shards);

    void initializeReplicaJob(JobReplica &replica_job, Cluster::Address & replica_address, const Cluster::ShardInfo &shard_info);
};

}
*/