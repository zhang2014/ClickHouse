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

    StoragePtr getTemporaryTable();

    DatabasePtr getDefaultDatabase();

    std::shared_ptr<Context> getQueryContext();

    void destroyEnvironment();

    void initializeEnvironment(const std::string & data_dir);

    void createTemporaryTable(const NamesAndTypesList & columns_type_and_name, size_t granularity_size, const std::string & partition_by, const std::string & order_by);

    BlockIO tryExecuteQuery(const std::string & query, Context & context);
private:
    std::mutex mutex;
    SharedContextHolder shared_context;
    std::unique_ptr<Context> global_context;

    ConfigurationPtr configuration;
    StoragePtr temporary_storage;

    ConfigurationPtr config();

    void registerComponents();

    void attachSystemTables(const Context & context);

    void initializeContext(Context & context, const std::string & data_dir);
};

}
