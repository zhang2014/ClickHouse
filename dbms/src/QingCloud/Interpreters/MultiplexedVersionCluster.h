#pragma once

#include <Interpreters/Cluster.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/RWLockFIFO.h>
//#include <QingCloud/Interpreters/QingCloudDDLSynchronism.h>

namespace DB
{

class MultiplexedVersionCluster
{
public:
    String getCurrentWritingVersion();

    std::vector<String> getReadableVersions();

    std::map<String, ClusterPtr> getAllVersionsCluster();

    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> getAddressesAndConnections();

//    QingCloudDDLSynchronismPtr getDDLSynchronism();

    MultiplexedVersionCluster(const Poco::Util::AbstractConfiguration & configuration, const Settings & settings, const std::string & config_prefix);

    template <typename T> T getPropertyOrChildValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & configuration_key,
                                                      String property_or_child_name);

    template <typename T> T getPropertyOrChildValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & configuration_key,
                                                    String property_or_child_name,const T & default_value);

    void setAddressAndConnections(Cluster::ShardInfo & shard_info,
                                  const Poco::Util::AbstractConfiguration & configuration,
                                  const std::string & replica_config_prefix,
                                  const Settings & settings, Cluster::Addresses & addresses);

    RWLockFIFO::LockHandler getConfigurationLock();

    void updateMultiplexedVersionCluster(const Poco::Util::AbstractConfiguration & configuration, const Settings & settings, const std::string & config_prefix);
private:
    std::map<String, ClusterPtr> all_version_and_cluster;
    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> address_and_connection_pool_cache;
    mutable RWLockFIFOPtr configuration_lock = RWLockFIFO::create();
    std::mutex synchronism_mutex;
//    QingCloudDDLSynchronismPtr synchronism;

    Cluster::Address createAddress(const Poco::Util::AbstractConfiguration & configuration, const std::string & replica_config_prefix);

    template <typename T> struct TypeToEnum {
        inline static T & getValue(const Poco::Util::AbstractConfiguration &, const std::string &)
        {
            throw Exception("");
        }
    };
};

class DummyCluster : public Cluster
{
public:
    bool is_readable;
    bool is_writeable;

    DummyCluster(const Poco::Util::AbstractConfiguration & configuration, const std::string & configuration_prefix,
                 MultiplexedVersionCluster * multiplexed_version_cluster, const Settings & settings, bool is_readable, bool is_writeable);

    const AddressesWithFailover & getShardsAddresses() const override;

    size_t getShardCount() const override;

    const ShardsInfo & getShardsInfo() const override;

    ~DummyCluster() = default;

private:
    Cluster::AddressesWithFailover addresses;
    ShardsInfo shards_info;
    Cluster::SlotToShard slot_to_shard;

public:
    const SlotToShard & getSlotToShard() const override;
};

template <> struct MultiplexedVersionCluster::TypeToEnum<String>  {
    inline static String getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return configuration.getString(key);
    }
};

template <> struct MultiplexedVersionCluster::TypeToEnum<UInt64>  {
    inline static UInt64 getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return configuration.getUInt64(key);
    }
};

template <> struct MultiplexedVersionCluster::TypeToEnum<UInt16>  {
    inline static UInt16 getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return (UInt16) configuration.getUInt(key);
    }
};

template <> struct MultiplexedVersionCluster::TypeToEnum<UInt32>  {
    inline static UInt32 getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return configuration.getUInt(key);
    }
};

template <> struct MultiplexedVersionCluster::TypeToEnum<bool>  {
    inline static bool getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return configuration.getBool(key);
    }
};

using MultiplexedClusterPtr = std::shared_ptr<MultiplexedVersionCluster>;

}
