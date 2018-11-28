#pragma once

#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <Interpreters/IInterpreter.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <QingCloud/Storages/StorageQingCloud.h>
#include <QingCloud/Interpreters/SafetyPoint/SafetyPointFactory.h>
#include <QingCloud/Parsers/ASTUpgradeQuery.h>

namespace DB
{
using StorageWithLock = std::vector<std::pair<StoragePtr, TableStructureReadLockPtr>>;


class InterpreterUpgradeQuery : public IInterpreter
{
public:
    InterpreterUpgradeQuery(const ASTPtr & node, const Context & context);

    BlockIO execute() override;

private:
    ASTPtr node;
    const Context & context;

    std::vector<DatabaseAndTableName> upgradeVersionForPaxos(ASTUpgradeQuery *upgrade_query, const SafetyPointWithClusterPtr &safety_point_sync);


    std::vector<DatabaseAndTableName> selectAllUpgradeStorage(const String &origin_version, const String &upgrade_version);
};

}