#pragma once

#include <Interpreters/Cluster.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/RWLockFIFO.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromFile.h>
#include <QingCloud/Storages/UpgradeProgress.h>
#include <QingCloud/Interpreters/SafetyPointFactory.h>

namespace DB
{

class MultiplexedVersionCluster
{
public:
    String getCurrentVersion();

    ClusterPtr getCluster(const String & cluster_name);

    MultiplexedVersionCluster(const Poco::Util::AbstractConfiguration & configuration, const Settings & settings, const std::string & config_prefix);

    ConnectionPoolPtr getOrCreateConnectionPools(const Cluster::Address & address, const Settings & settings);

    void updateMultiplexedVersionCluster(const Poco::Util::AbstractConfiguration & configuration, const Settings & settings, const std::string & config_prefix);

    void checkBeforeUpgrade(const String & origin_version, const String & upgrade_version);

    void executionUpgradeVersion(const String &origin_version, const String &upgrade_version,
                                 const std::vector<DatabaseAndTableName> &databases_and_tables_name, const Context &context,
                                 const SafetyPointWithClusterPtr & safety_point_sync);

    void redirect(const String &origin_version, const String &upgrade_version, const Context &context, const SafetyPointWithClusterPtr &safety_point_sync);

    void rebalance(const String &origin_version, const String &upgrade_version, const Context &context, const SafetyPointWithClusterPtr &safety_point_sync);
private:
    std::mutex mutex;
    std::atomic<bool> is_running;
    std::pair<String, String> version;

    std::map<String, ClusterPtr> all_version_and_cluster;
    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> address_and_connection_pool_cache;

    struct UpgradeJobInfo
    {
    public:
        UpgradeJobInfo(const String & path, const String & current_version);

        void initializeDatabaseAndProgress(const std::vector<DatabaseAndTableName> & databases_and_tables_name);

        void recordUpgradeStatus(const String & database_name, const String & table_name, ProgressEnum progress_enum);

        String data_path;
        std::mutex mutex;
        std::pair<String, String> version_info;
        std::shared_ptr<WriteBufferFromFile> buffer;
        std::map<DatabaseAndTableName, ProgressEnum> databases_with_progress;
        std::vector<std::pair<DatabaseAndTableName, ProgressEnum>> database_with_progress;
    };

    UpgradeJobInfo upgrade_job_info;
};

class DummyCluster : public Cluster
{
public:
    ~DummyCluster() override = default;

    DummyCluster(const Poco::Util::AbstractConfiguration & configuration, const std::string & configuration_prefix,
                 MultiplexedVersionCluster * multiplexed_version_cluster, const Settings & settings);

    const AddressesWithFailover & getShardsAddresses() const override;

    size_t getShardCount() const override;

    const ShardsInfo & getShardsInfo() const override;

private:
    Cluster::AddressesWithFailover addresses;
    ShardsInfo shards_info;
    Cluster::SlotToShard slot_to_shard;
    size_t remote_shard_count = 0;
    size_t local_shard_count = 0;

public:
    const SlotToShard & getSlotToShard() const override;

    size_t getLocalShardCount() const override;

    size_t getRemoteShardCount() const override;

    Cluster::Address createAddresses(const Poco::Util::AbstractConfiguration &configuration, const String &replica_key) const;

    ShardInfo
    createShardInfo(size_t shard_number, Addresses shard_addresses, Addresses shard_local_addresses, ConnectionPoolPtrs shard_connections,
                        UInt32 i, const Settings &settings);
};

using MultiplexedClusterPtr = std::shared_ptr<MultiplexedVersionCluster>;

}
