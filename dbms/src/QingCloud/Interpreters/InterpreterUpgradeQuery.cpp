#include <QingCloud/Interpreters/InterpreterUpgradeQuery.h>
#include <QingCloud/Parsers/ASTUpgradeQuery.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <QingCloud/Storages/StorageQingCloud.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

InterpreterUpgradeQuery::InterpreterUpgradeQuery(const ASTPtr &node, const Context &context) : node(node), context(context)
{
}


BlockIO InterpreterUpgradeQuery::execute()
{
    ASTUpgradeQuery * upgrade_query = typeid_cast<ASTUpgradeQuery *>(node.get());
    StoragePtr upgrade_storage = context.getTable(upgrade_query->database, upgrade_query->table);

    if (!dynamic_cast<StorageQingCloud *>(upgrade_storage.get()))
        throw Exception("Cannot run upgrade query for without QingCloud Storage.", ErrorCodes::LOGICAL_ERROR);

    dynamic_cast<StorageQingCloud *>(upgrade_storage.get())->upgradeVersion(upgrade_query->origin_version, upgrade_query->upgrade_version);
    return {};
}

}
