#pragma once

#include <map>
#include <Core/Types.h>
#include <ext/singleton.h>
#include <Interpreters/Cluster.h>

namespace DB
{

class SafetyPointWithCluster;
using SafetyPointWithClusterPtr = std::shared_ptr<SafetyPointWithCluster>;

class SafetyPointFactory : public ext::singleton<SafetyPointFactory>
{
public:
    void releaseSafetyPoint(const String & name);

    bool waitOther(const String & name, const UInt64 & reentry);

    bool checkAllNotify(const String &name, const UInt64 &reentry);

    void expected(const String & action_name, const UInt64 & reentry, const std::vector<String> & expected);
    void receiveActionNotify(const String & action_name, const UInt64 & reentry, const String & from);

    SafetyPointWithClusterPtr createSafetyPoint(const String & name, const ClusterPtr & cluster, const Context & context);
private:
    struct ActionEntity
    {
        std::vector<String> from;
        std::vector<String> expected;
        std::mutex wait_for_mutex;
        std::condition_variable cond;
    };

    struct SafetyPointEntity
    {
        size_t wait_size = 0;
        std::mutex wait_for_mutex;
        std::condition_variable cond;
    };

    std::mutex action_mutex;
    std::mutex safety_mutex;
    std::map<String, ActionEntity> action_entities;
    std::map<String, SafetyPointEntity> safety_entities;
};

class SafetyPointWithCluster
{
public:
    ~SafetyPointWithCluster();

    SafetyPointWithCluster(const String & name, const ClusterPtr & cluster, const Context & context);

    void sync(size_t check_size = 1);

    void sendQueryWithAddresses(const std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> &addresses_with_connections,
                                const String &query_string);
private:
    const Context & context;

    String name;
    ClusterPtr cluster;
    std::atomic<UInt64> count;
};

}

