#pragma once

#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <Interpreters/IInterpreter.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <QingCloud/Storages/StorageQingCloud.h>

namespace DB
{

class UpgradeQueryBlockInputStream : public IProfilingBlockInputStream
{
public:
    UpgradeQueryBlockInputStream(const std::vector<StoragePtr> & upgrade_storage);

    String getName() const override;

    Block getHeader() const override;

private:
    enum State
    {
        INITIALIZE_UPGRADE_VERSION,
        REDIRECT_VERSIONS_BEFORE_MIGRATE,
        FLUSH_OLD_VERSION_DATA,
        MIGRATE_OLD_VERSION_DATA,
        FLUSH_UPGRADE_VERSION_DATA,                   /// At this time, old version and upgrade version data is consistent
        REDIRECT_VERSIONS_AFTER_MIGRATE,
        MIGRATE_TMP_VERSION_DATA,                     /// At this time, Temp version data is detached
        REDIRECT_VERSION_AFTER_ALL_MIGRATE,
        DELETE_OUTDATED_VERSIONS,                     /// At this time, Upgrade version is Done.

        /// The end state
        SUCCESSFULLY,
        FAILURE
    };

private:
    String origin_version;
    String upgrade_version;
    std::vector<StoragePtr> upgrade_storage;

    Block readImpl() override;
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