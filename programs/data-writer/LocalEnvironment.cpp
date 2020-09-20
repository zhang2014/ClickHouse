#include <Interpreters/Context.h>
#include <Disks/registerDisks.h>
#include <Storages/registerStorages.h>
#include <Functions/registerFunctions.h>
#include <Dictionaries/registerDictionaries.h>
#include <TableFunctions/registerTableFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/System/attachSystemTables.h>
#include <Interpreters/executeQuery.h>
#include "LocalEnvironment.h"

namespace DB
{

static const String default_database_name = "default_database";

void LocalEnvironment::registerComponents()
{
    registerDisks();
    registerStorages();
    registerFunctions();
    registerDictionaries();
    registerTableFunctions();
    registerAggregateFunctions();
}

void LocalEnvironment::initializeContext(Context & context, const std::string & data_dir)
{
    context.makeGlobalContext();
    context.setApplicationType(Context::ApplicationType::LOCAL);
    context.setPath(data_dir);
    context.setUserFilesPath(""); // user's files are everywhere

    const ConfigurationPtr & configuration_ = config();
    context.setConfig(configuration_);
    context.setUsersConfig(configuration_);
    context.setMarkCache(5368709120);
    context.setDefaultProfiles(*configuration_);
    DatabaseCatalog::instance().attachDatabase(default_database_name, std::make_shared<DatabaseMemory>(default_database_name, *global_context));
    context.setCurrentDatabase(default_database_name);
}

ConfigurationPtr LocalEnvironment::config()
{
    if (!configuration)
    {
        static const char * minimal_default_user_xml = "<yandex><profiles><default></default></profiles><users><default><password></password><networks><ip>::/0</ip></networks><profile>default</profile><quota>default</quota></default></users><quotas><default></default></quotas></yandex>";
        std::stringstream ss{std::string{minimal_default_user_xml}};
        Poco::XML::InputSource input_source{ss};
        configuration = ConfigurationPtr{new Poco::Util::XMLConfiguration{&input_source}};
    }

    return configuration;
}

void LocalEnvironment::attachSystemTables(const Context & context)
{
    DatabasePtr system_database = DatabaseCatalog::instance().tryGetDatabase(DatabaseCatalog::SYSTEM_DATABASE);
    if (!system_database)
    {
        /// TODO: add attachTableDelayed into DatabaseMemory to speedup loading
        system_database = std::make_shared<DatabaseMemory>(DatabaseCatalog::SYSTEM_DATABASE, context);
        DatabaseCatalog::instance().attachDatabase(DatabaseCatalog::SYSTEM_DATABASE, system_database);
    }

    attachSystemTablesLocal(*system_database);
}

void LocalEnvironment::initializeEnvironment(const std::string & data_dir)
{
    std::unique_lock<std::mutex> lock(mutex);

    if (global_context)
        throw Exception("LOGICAL ERROR: context is initialized.", ErrorCodes::LOGICAL_ERROR);

    /// register components(storage, disk, function and others)
    registerComponents();
    shared_context = Context::createShared();
    global_context = std::make_unique<Context>(Context::createGlobal(shared_context.get()));

    initializeContext(*global_context, data_dir);
    attachSystemTables(*global_context);
}

std::shared_ptr<Context> LocalEnvironment::getQueryContext()
{
    std::shared_ptr<Context> query_context = std::make_shared<Context>(*global_context);

    query_context->makeQueryContext();
    query_context->makeSessionContext();
    query_context->setCurrentQueryId("");
    query_context->setUser("default", "", Poco::Net::SocketAddress{});
    return query_context;
}

Context & LocalEnvironment::getGlobalContext()
{
    std::unique_lock<std::mutex> lock(mutex);
    return *global_context;
}

void LocalEnvironment::destroyEnvironment()
{
    global_context->shutdown();
    global_context.reset();
}

DatabasePtr LocalEnvironment::getDefaultDatabase()
{
    return DatabaseCatalog::instance().getDatabase(default_database_name);
}

BlockIO LocalEnvironment::tryExecuteQuery(const std::string & query, Context & context)
{
    /// Use the same query_id (and thread group) for all queries
    CurrentThread::QueryScope query_scope_holder(context);
    return executeQuery(query, context, true);
}

}
