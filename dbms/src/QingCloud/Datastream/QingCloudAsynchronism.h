#pragma once

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Common/SimpleIncrement.h>
#include <QingCloud/Datastream/QingCloudDirectoryMonitor.h>

namespace DB
{

class QingCloudAsynchronism
{
public:
    QingCloudAsynchronism(const String &data_path, Context &context);

    void writeToShard(const UInt64 version, const UInt64 shard_number, const String &file_name);

    void writeToReplica(const UInt64 version, const UInt64 shard_number, const Cluster::Address & address, const String &file_name);

    void destroyDirectoryMonitor(String dir_name);

    String writeTempBlock(String & query_string, const Block & block);

private:
    String path;
    Context & context;
    SimpleIncrement increment;
    std::shared_ptr<MultiplexedVersionCluster> cluster;

    std::unordered_map<std::string, std::shared_ptr<QingCloudDirectoryMonitor>> cluster_nodes_data;
    std::mutex cluster_nodes_mutex;

    void requireDirectoryMonitor(const Cluster::Address & address, const String & dir_name);

    struct ReplicasDirectoryInfo
    {
        Cluster::Address address;
        UInt64 current_writing_version;
        UInt64 current_writing_shard_number;

        ReplicasDirectoryInfo(const String & dir_name);
    };
};

}