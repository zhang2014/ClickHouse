#pragma once

#include <Interpreters/ClusterProxy/IStreamFactory.h>
#include <Core/QueryProcessingStage.h>
#include <Storages/IStorage.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

namespace ClusterProxy
{

class QingCloudSelectStreamFactory final : public IStreamFactory
{
public:
    QingCloudSelectStreamFactory(
        QueryProcessingStage::Enum processed_stage_, QualifiedTableName main_table_,
        const Tables & external_tables_, String query_version_, UInt64 query_shard_number_);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const String & query, const ASTPtr & query_ast,
        const Context & context, const ThrottlerPtr & throttler,
        BlockInputStreams & res) override;

    BlockInputStreamPtr makeOriginRemoteBlockInputStream(
        Block &header, const Cluster::ShardInfo &shard_info, const String &query, const Context &context, const ThrottlerPtr &throttler);

private:
    QueryProcessingStage::Enum processed_stage;
    QualifiedTableName main_table;
    ASTPtr table_func_ptr;
    Tables external_tables;
    String query_version;
    UInt64 query_shard_number;
};

class QingCloudRemoteBlockInputStream : public IProfilingBlockInputStream
{
public:
    QingCloudRemoteBlockInputStream(const BlockInputStreamPtr & input, const String & version_, const UInt64 & shard_num_, Settings & settings_);

private:
    Block readImpl() override;

    void readPrefixImpl() override;

    void readSuffixImpl() override;

    String getName() const override;

    Block getHeader() const override;


private:
    const String version;
    const UInt64 shard_num;
    Settings settings;
};

}

}
