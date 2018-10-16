#include <QingCloud/Datastream/QingCloudAsynchronism.h>
#include <port/unistd.h>
#include <Common/Exception.h>
#include <QingCloud/Datastream/QingCloudDirectoryMonitor.h>
#include <IO/Operators.h>
#include <boost/filesystem/operations.hpp>
#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <Common/ClickHouseRevision.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <boost/algorithm/string.hpp>
#include <Common/isLocalAddress.h>
#include <Common/DNSResolver.h>
#include <Poco/File.h>
#include <Common/escapeForFileName.h>


namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_FILE_NAME;
}

static inline bool addressEquals(Cluster::Address expect, Cluster::Address actual)
{
    if (expect.host_name != actual.host_name)
        return false;
    if (expect.port != actual.port)
        return false;
    if (expect.user != actual.user)
        return false;
    if (expect.password != actual.password)
        return false;
    if (expect.secure != actual.secure)
        return false;

    return expect.compression == actual.compression;
}

static String writeVersionAndShardText(const String & version, UInt64 shard_number, String suffix)
{
    WriteBufferFromOwnString text_string;
    text_string << version << "_" << shard_number << "_" << suffix;
    return text_string.str();
}

void QingCloudAsynchronism::writeToShard(const String & version, const UInt64 shard_number, const String &file_name)
{
    auto multiplexed_context = context.getMultiplexedVersion();
    auto all_version_cluster = multiplexed_context->getAllVersionsCluster();
    const auto shards_addresses = all_version_cluster.at(version)->getShardsAddresses();

    for (const auto replica_address : shards_addresses[shard_number])
        if (!replica_address.is_local)
            writeToReplica(version, shard_number, replica_address, file_name);
}

void QingCloudAsynchronism::writeToReplica(const String & version, const UInt64 shard_number,
                                           const Cluster::Address &address, const String &file_name)
{
    String source_file_path;
    String dir_name = writeVersionAndShardText(version, shard_number, address.toStringFull());

    Poco::File(path + dir_name).createDirectory();
    if (link(source_file_path.data(), ((path + dir_name) + "/" + toString(increment.get()) + ".bin").data()))
        throwFromErrno("Could not link " + file_name + " to " + source_file_path);

    requireDirectoryMonitor(address, dir_name);
}

void QingCloudAsynchronism::destroyDirectoryMonitor(String dir_name)
{
    std::lock_guard lock(cluster_nodes_mutex);
    if (cluster_nodes_data.count(dir_name))
        cluster_nodes_data.erase(dir_name);
}

void QingCloudAsynchronism::requireDirectoryMonitor(const Cluster::Address & address, const String & dir_name)
{
    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> addresses_and_connections;

    {
        auto config_lock = cluster->getConfigurationLock();
        addresses_and_connections = cluster->getAddressesAndConnections();
    }

    for (const auto address_and_connections : addresses_and_connections)
    {
        Cluster::Address add = address_and_connections.first;
        if (addressEquals(address, add))
        {
            std::lock_guard lock(cluster_nodes_mutex);
            if (!cluster_nodes_data.count(dir_name))
            {
                ReplicasDirectoryInfo info(dir_name);
                Context current_context = context;
                current_context.getSettingsRef().writing_version = info.current_writing_version;
                current_context.getSettingsRef().writing_shard_index = info.current_writing_shard_number;
                cluster_nodes_data.insert(std::pair(dir_name, std::make_shared<QingCloudDirectoryMonitor>(
                    address_and_connections.second, path, dir_name, this, current_context)));
                return;
            }
        }
    }

    /// TODO: 创建一个临时的连接池, 用完既回收掉
    throw Exception("Cannot found connection pool.");
}

UInt64 getMaximumFileNumber(const std::string &path)
{
    UInt64 res = 0;

    boost::filesystem::recursive_directory_iterator begin(path);
    boost::filesystem::recursive_directory_iterator end;
    for (auto it = begin; it != end; ++it)
    {
        const auto &path = it->path();

        if (it->status().type() != boost::filesystem::regular_file || !endsWith(path.filename().string(), ".bin"))
            continue;

        UInt64 num = 0;
        try
        {
            num = parse<UInt64>(path.filename().stem().string());
        }
        catch (Exception &e)
        {
            e.addMessage("Unexpected file name " + path.filename().string() + " found at " + path.parent_path().string() +
                         ", should have numeric base name.");
            throw;
        }

        if (num > res)
            res = num;
    }

    return res;
}

QingCloudAsynchronism::QingCloudAsynchronism(const String &data_path, Context &context)
    : path(data_path), context(context), cluster(context.getMultiplexedVersion())
{
    if (path.empty())
        return;

    Poco::File{path}.createDirectory();
    increment.set(getMaximumFileNumber(data_path));

    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> addresses_and_connections;

    {
        auto config_lock = cluster->getConfigurationLock();
        addresses_and_connections = cluster->getAddressesAndConnections();
    }

    boost::filesystem::directory_iterator begin(path);
    boost::filesystem::directory_iterator end;
    for (auto it = begin; it != end; ++it)
    {
        if (it->status().type() == boost::filesystem::directory_file)
        {
            String dir_name = it->path().filename().string();
            ReplicasDirectoryInfo info(it->path().filename().string());

            for (const auto address_and_connections : addresses_and_connections)
            {
                Cluster::Address add = address_and_connections.first;
                if (addressEquals(info.address, add))
                {
                    std::lock_guard lock(cluster_nodes_mutex);
                    Context current_context = context;
                    current_context.getSettingsRef().writing_version = info.current_writing_version;
                    current_context.getSettingsRef().writing_shard_index = info.current_writing_shard_number;
                    cluster_nodes_data.insert(std::pair(dir_name, std::make_unique<QingCloudDirectoryMonitor>(
                        address_and_connections.second, path, dir_name, this, current_context)));
                    break;
                }
            }
        }
    }
}

String QingCloudAsynchronism::writeTempBlock(String &query_string, const Block &block)
{
    const auto &tmp_path = path + "tmp/";
    Poco::File(tmp_path).createDirectory();
    String file_name = toString(increment.get()) + ".bin";
    const auto &block_file_tmp_path = tmp_path + file_name;

    WriteBufferFromFile out{block_file_tmp_path};
    CompressedWriteBuffer compress{out};
    NativeBlockOutputStream stream{compress, ClickHouseRevision::get(), block.cloneEmpty()};

    writeStringBinary(query_string, out);

    stream.writePrefix();
    stream.write(block);
    stream.writeSuffix();

    return file_name;
}

QingCloudAsynchronism::ReplicasDirectoryInfo::ReplicasDirectoryInfo(const String &dir_name)
{
    const char *address_begin = dir_name.data();
    const char *address_end = address_begin + dir_name.size();

    if (endsWith(dir_name, "+secure"))
    {
        address_end -= strlen("+secure");
        address.secure = Protocol::Secure::Enable;
    }

    const char *writing_version_end = strchr(dir_name.data(), '_');
    const char *writing_shared_number_end = strchr(writing_version_end + 1, '_');
    current_writing_version = parse<String>(address_begin, writing_version_end - address_begin);
    current_writing_shard_number = parse<UInt64>(writing_version_end + 1, writing_shared_number_end - writing_version_end);

    const char *user_pw_end = strchr(writing_shared_number_end + 1, '@');
    const char *colon = strchr(writing_shared_number_end + 1, ':');
    if (!user_pw_end || !colon)
        throw Exception{
            "Shard address '" + dir_name + "' does not match to 'version_shard_user[:password]@host:port#default_database' pattern",
            ErrorCodes::INCORRECT_FILE_NAME};

    const bool has_pw = colon < user_pw_end;
    const char *host_end = has_pw ? strchr(user_pw_end + 1, ':') : colon;
    if (!host_end)
        throw Exception{"Shard address '" + dir_name + "' does not contain port", ErrorCodes::INCORRECT_FILE_NAME};
    const char *has_db = strchr(address_begin, '#');
    const char *port_end = has_db ? has_db : address_end;

    address.user = unescapeForFileName(std::string(writing_shared_number_end + 1, has_pw ? colon : user_pw_end));
    address.password = has_pw ? unescapeForFileName(std::string(colon + 1, user_pw_end)) : std::string();
    address.host_name = unescapeForFileName(std::string(user_pw_end + 1, host_end));
    address.port = parse<UInt16>(host_end + 1, port_end - (host_end + 1));
    address.default_database = has_db ? unescapeForFileName(std::string(has_db + 1, address_end)) : std::string();
    const auto initially_resolved_address = DNSResolver::instance().resolveAddress(address.host_name, address.port);
    address.is_local = isLocalAddress(initially_resolved_address.host());
}

}