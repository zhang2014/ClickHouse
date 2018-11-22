#include <QingCloud/Interpreters/SafetyPointFactory.h>
#include <QingCloud/Common/differentClusters.h>
#include <Interpreters/Context.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Common/getMultipleKeysFromConfig.h>
#include "SafetyPointFactory.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int SAFETY_TIMEOUT;
extern const int SAFETY_UNEXPECTED;
}

void SafetyPointFactory::releaseSafetyPoint(const String & name)
{
    std::unique_lock<std::mutex> lock(mutex);

    if (actions.count(name))
        actions.erase(name);

    if (syncs.count(name))
        syncs.erase(name);
}

void SafetyPointFactory::receiveActionNotify(const String & sync_name, const String & action_name, const UInt64 & reentry, const String & from)
{
    std::unique_lock<std::mutex> lock(mutex);
    if (!syncs.count(sync_name))
        actions[sync_name].emplace_back(std::tuple(action_name, reentry, from));
    else
        syncs[sync_name]->notifyActionWaiter(action_name, reentry, from);
}

SafetyPointWithClusterPtr SafetyPointFactory::createSafetyPoint(const String & sync_name, const Context &context,
                                                                const std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> &connections)
{
    std::unique_lock<std::mutex> lock(mutex);

    if (syncs.count(sync_name))
        throw Exception("Already exists " + sync_name + " SafetyPointWithCluster.", ErrorCodes::LOGICAL_ERROR);
    else
    {
        syncs[sync_name] = std::make_shared<SafetyPointWithCluster>(sync_name, context, connections);
        if (actions.count(sync_name))
        {
            for (const std::tuple<String, UInt64, String> ele : actions[sync_name])
                syncs[sync_name]->notifyActionWaiter(std::get<0>(ele), std::get<1>(ele), std::get<2>(ele));

            actions.erase(sync_name);
        }

        return syncs[sync_name];
    }
}

SafetyPointWithCluster::SafetyPointWithCluster(
    const String &name, const Context &context, const std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> &connections)
    : context(context), sync_name(name), connections(connections)
{
}

SafetyPointWithCluster::~SafetyPointWithCluster()
{
//    SafetyPointFactory::instance().releaseSafetyPoint(sync_name);
}

void SafetyPointWithCluster::broadcast(const String &query_string)
{
    BlockInputStreams streams;
    const Settings settings = context.getSettingsRef();
    for (const auto &address_with_connections : connections)
    {
        if (!address_with_connections.first.is_local)
        {
            ConnectionPoolPtrs failover_connections;
            failover_connections.emplace_back(address_with_connections.second);

            ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
                failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

            streams.emplace_back(std::make_shared<RemoteBlockInputStream>(shard_pool, query_string, Block{}, context));
        }
    }

    if (!streams.empty())
    {
        BlockInputStreamPtr union_stream = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, streams.size());
        std::make_shared<SquashingBlockInputStream>(union_stream, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())->read();
    }
}

void SafetyPointWithCluster::broadcastSync(const String &action_name, size_t check_size)
{

    LOG_DEBUG(&Logger::get("SafetyPointWithCluster"), "Execute SafetyPointWithCluster Sync Name : " + sync_name + ", Action Name:" + action_name);

    std::unique_lock<std::mutex> lock(mutex);

    /// 循环检查,可以有效屏蔽半成功问题
    for (size_t index = 0; index < check_size; ++index)
    {
        size_t reentry = ++count;
        if (checkAlreadyInconsistent(action_name, reentry))
            throw Exception("Cannot broadcast sync, because other server is inconsistent.", ErrorCodes::SAFETY_UNEXPECTED);

        std::vector<std::string> listen_hosts = getMultipleValuesFromConfig(context.getConfigRef(), "", "listen_host");
        broadcast("ACTION NOTIFY '" + action_name + "' REENTRY " + toString(reentry) + " FROM ('" + listen_hosts[0] + "', '" + sync_name + "')");

        if (!checkAlreadyConsistent())
        {
            if (cond.wait_for(lock, std::chrono::milliseconds(180000)) == std::cv_status::timeout || !exception_message.empty())
                throw Exception(exception_message.empty() ? "Cannot wait other server safety point, because time is out of time."
                                                          : exception_message,
                                exception_message.empty() ? ErrorCodes::SAFETY_TIMEOUT : ErrorCodes::SAFETY_UNEXPECTED);

            actual_arrival.clear();
            exception_message = "";
        }
    }

    LOG_DEBUG(&Logger::get("SafetyPointWithCluster"), "Successfully SafetyPointWithCluster : " + action_name);
}

void SafetyPointWithCluster::notifyActionWaiter(const String &action_name, const UInt64 &reentry, const String &from)
{
    try
    {
        std::unique_lock<std::mutex> lock(mutex);

        actual_arrival[from] = std::pair(action_name, reentry);

        if (!checkAlreadyInconsistent(action_name, reentry))
            throw Exception("Cannot broadcast sync, because other server is inconsistent.", ErrorCodes::SAFETY_UNEXPECTED);

        if (checkAlreadyConsistent())
        {
            lock.unlock();
            cond.notify_one();
        }
    }
    catch (Exception e)
    {
        exception_message = getExceptionMessage(e, false);
        cond.notify_one();
    }
}


bool SafetyPointWithCluster::checkAlreadyConsistent()
{
    auto exists = [&, this](const String & host_name)
    {
        for (const auto & actual : actual_arrival)
            if (actual.first == host_name)
                return true;

        return false;
    };

    for (const auto & connection : connections)
        if (!exists(connection.first.host_name))
            return false;

    return true;
}

bool SafetyPointWithCluster::checkAlreadyInconsistent(const String &action_name, const UInt64 &reentry)
{
    for (const auto & from_and_action : actual_arrival)
        if (from_and_action.second.first != action_name
            || from_and_action.second.second != reentry)
            return true;
    return false;
}

}
