#pragma once

#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <Interpreters/IInterpreter.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <QingCloud/Storages/StorageQingCloud.h>
#include "SafetyPointFactory.h"

namespace DB
{

class UpgradeQueryBlockInputStream : public IProfilingBlockInputStream
{
public:
    UpgradeQueryBlockInputStream(const SafetyPointWithClusterPtr &safety_point_sync, const std::vector<StoragePtr> &upgrade_storage,
                                 const String &origin_version, const String &upgrade_version);

    String getName() const override;

    Block getHeader() const override;

private:
    String origin_version;
    String upgrade_version;
    std::vector<StoragePtr> upgrade_storages;
    SafetyPointWithClusterPtr safety_point_sync;

    Block readImpl() override;

    static String progressToString(ProgressEnum progress_enum)
    {
        switch(progress_enum)
        {
            case INITIALIZE_UPGRADE_VERSION: return "INITIALIZE_UPGRADE_VERSION";
            case REDIRECT_VERSIONS_BEFORE_MIGRATE: return "REDIRECT_VERSIONS_BEFORE_MIGRATE";
            case FLUSH_OLD_VERSION_DATA: return "FLUSH_OLD_VERSION_DATA";
            case CLEANUP_UPGRADE_VERSION: return "CLEANUP_UPGRADE_VERSION";
            case MIGRATE_OLD_VERSION_DATA: return "MIGRATE_OLD_VERSION_DATA";
            case FLUSH_UPGRADE_VERSION_DATA: return "FLUSH_UPGRADE_VERSION_DATA";
            case REDIRECT_VERSIONS_AFTER_MIGRATE: return "REDIRECT_VERSIONS_AFTER_MIGRATE";
            case MIGRATE_TMP_VERSION_DATA: return "MIGRATE_TMP_VERSION_DATA";
            case REDIRECT_VERSION_AFTER_ALL_MIGRATE: return "REDIRECT_VERSION_AFTER_ALL_MIGRATE";
            case DELETE_OUTDATED_VERSIONS: return "DELETE_OUTDATED_VERSIONS";
            case NORMAL : return "NORMAL";
        }
    }
};

class InterpreterUpgradeQuery : public IInterpreter
{
public:
    InterpreterUpgradeQuery(const ASTPtr & node, const Context & context);

    BlockIO execute() override;

private:
    ASTPtr node;
    const Context & context;
};

}