#pragma once

#include <map>
#include <Core/Types.h>
#include <ext/singleton.h>
#include <Interpreters/Cluster.h>

namespace DB
{

class SafetyPointWithCluster
{
public:
    ~SafetyPointWithCluster();

    SafetyPointWithCluster(const String &name, const Context &context, const std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> & connections);

    void broadcast(const String & query_string);

    void broadcastSync(const String & action_name, size_t check_size = 1);

    void notifyActionWaiter(const String &action_name, const UInt64 &reentry, const String &from);

private:
    const Context & context;

    String sync_name;
    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> connections;
    std::atomic<UInt64> count = 0;

    std::mutex mutex;
    String exception_message;
    std::condition_variable cond;
    std::map<String, std::pair<String, UInt64>> actual_arrival;         /// from -> (action_name, reentry)

    bool checkAlreadyConsistent();

    bool checkAlreadyInconsistent(const String & action_name, const UInt64 & reentry);
};

using SafetyPointWithClusterPtr = std::shared_ptr<SafetyPointWithCluster>;

class SafetyPointFactory : public ext::singleton<SafetyPointFactory>
{
public:
    void releaseSafetyPoint(const String & name);

    void receiveActionNotify(const String & sync_name, const String & action_name, const UInt64 & reentry, const String & from);

    SafetyPointWithClusterPtr createSafetyPoint(
        const String & sync_name, const Context & context, const std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> & connections);

private:
    std::mutex mutex;

    std::map<String, SafetyPointWithClusterPtr> syncs;
    std::map<String, std::vector<std::tuple<String, UInt64, String>>> actions;
};

}

