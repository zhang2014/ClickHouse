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