#pragma once

#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <Common/Config/ConfigProcessor.h>
#include <DataStreams/BlockIO.h>

namespace DB
{

class LocalEnvironment
{
public:
    Context & getGlobalContext();

    DatabasePtr getDefaultDatabase();

    std::shared_ptr<Context> getQueryContext();

    void destroyEnvironment();

    void initializeEnvironment(const std::string & data_dir);

    BlockIO tryExecuteQuery(const std::string & query, Context & context);
private:
    std::mutex mutex;
    SharedContextHolder shared_context;
    std::unique_ptr<Context> global_context;

    ConfigurationPtr configuration;

    ConfigurationPtr config();

    void registerComponents();

    void attachSystemTables(const Context & context);

    void initializeContext(Context & context, const std::string & data_dir);
};

}
