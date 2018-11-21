#include <QingCloud/Interpreters/SafetyPointFactory.h>
#include <QingCloud/Common/differentClusters.h>
#include <Interpreters/Context.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Common/getMultipleKeysFromConfig.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SAFETY_TIMEOUT;
}

void SafetyPointFactory::releaseSafetyPoint(const String & name)
{
    safety_mutex.lock();
    SafetyPointFactory::SafetyPointEntity & entity = safety_entities[name];
    if(--entity.wait_size != 0)
        entity.cond.notify_one();
    safety_mutex.unlock();
}

void SafetyPointFactory::receiveActionNotify(const String &action_name, const UInt64 &reentry, const String &from)
{
    std::lock_guard<std::mutex> lock(action_mutex);
    SafetyPointFactory::ActionEntity & entity = action_entities[action_name + "_" + toString(reentry)];
    entity.from.emplace_back(from);
    /// TODO: 比较内容
    if (entity.from.size() == entity.expected.size())
    {
        entity.from.clear();
        entity.expected.clear();
        entity.cond.notify_one();
    }

}

SafetyPointWithClusterPtr SafetyPointFactory::createSafetyPoint(const String & name, const ClusterPtr & cluster, const Context & context)
{
    safety_mutex.lock();
    SafetyPointFactory::SafetyPointEntity & entity = safety_entities[name];

    if (entity.wait_size++ == 0)
    {
        safety_mutex.unlock();
        return std::make_shared<SafetyPointWithCluster>(name, cluster, context);
    }

    std::unique_lock<std::mutex> lock{entity.wait_for_mutex};
    safety_mutex.unlock();

    if (entity.cond.wait_for(lock, std::chrono::milliseconds(180000)) == std::cv_status::timeout)
        throw Exception("Cannot get SafetyPointWithCluster, because time is out of time.", ErrorCodes::SAFETY_TIMEOUT);

    return std::make_shared<SafetyPointWithCluster>(name, cluster, context);
}

bool SafetyPointFactory::waitOther(const String & name, const UInt64 & reentry)
{
    action_mutex.lock();
    SafetyPointFactory::ActionEntity & entity = action_entities[name + "_" + toString(reentry)];
    std::unique_lock<std::mutex> lock(entity.wait_for_mutex);
    action_mutex.unlock();
    return entity.cond.wait_for(lock, std::chrono::milliseconds(180000)) == std::cv_status::no_timeout;
}

bool SafetyPointFactory::checkAllNotify(const String & action_name, const UInt64 & reentry)
{
    std::lock_guard<std::mutex> lock(action_mutex);
    SafetyPointFactory::ActionEntity & entity = action_entities[action_name + "_" + toString(reentry)];
    /// TODO: 比较内容
    if (entity.from.size() == entity.expected.size())
    {
        entity.from.clear();
        entity.expected.clear();
        return true;
    }
    return false;
}

void SafetyPointFactory::expected(const String &action_name, const UInt64 &reentry, const std::vector<String> & expected)
{
    std::lock_guard<std::mutex> lock(action_mutex);
    SafetyPointFactory::ActionEntity & entity = action_entities[action_name + "_" + toString(reentry)];
    for (const auto & expect : expected)
        entity.expected.emplace_back(expect);
}

SafetyPointWithCluster::SafetyPointWithCluster(const String &name, const ClusterPtr &cluster, const Context &context)
    : context(context), name(name), cluster(cluster)
{
}

SafetyPointWithCluster::~SafetyPointWithCluster()
{
    SafetyPointFactory::instance().releaseSafetyPoint(name);
}

void SafetyPointWithCluster::sendQueryWithAddresses(
    const std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> &addresses_with_connections, const String &query_string)
{
    BlockInputStreams streams;
    const Settings settings = context.getSettingsRef();
    for (const auto & address_with_connections : addresses_with_connections)
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

void SafetyPointWithCluster::sync(size_t check_size)
{
    if (cluster->getLocalShardCount() > 0)
    {
        const auto connections = getConnectionPoolsFromClusters(cluster);

        LOG_DEBUG(&Logger::get("SafetyPointWithCluster"), "Execute SafetyPointWithCluster : " + name);

        /// 循环检查,可以有效屏蔽半成功问题
        for (size_t index = 0; index < check_size; ++index)
        {
            size_t reentry = ++count;
            std::vector<std::string> listen_hosts = getMultipleValuesFromConfig(context.getConfigRef(), "", "listen_host");
            sendQueryWithAddresses(connections, "ACTION NOTIFY '" + name + " REENTRY " + toString(reentry) + " FROM '" + listen_hosts[0] + "'");

            std::vector<String> expected;
            for (const auto & address_with_connection : connections)
                expected.emplace_back(address_with_connection.first.host_name);

            SafetyPointFactory::instance().expected(name, reentry, expected);
            if (!SafetyPointFactory::instance().checkAllNotify(name, reentry) && !SafetyPointFactory::instance().waitOther(name, reentry))
                throw Exception("Cannot wait other server safety point, because time is out of time.", ErrorCodes::SAFETY_TIMEOUT);
        }

        LOG_DEBUG(&Logger::get("SafetyPointWithCluster"), "Successfully SafetyPointWithCluster : " + name);
    }
}

}
