#pragma once

#include <optional>
#include <Storages/Buffer/BufferSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

class Context;
class ASTStorage;

template <typename Base>
struct StorageSettings : public Base
{
    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(ASTStorage & storage_def);

    void loadFromEngineArguments(const std::vector<ASTPtr> & arguments);
};

class StoragesSettings
{
public:
    const StorageSettings<BufferSettings> & bufferSettings(const Context & context) const;

    const StorageSettings<MergeTreeSettings> & mergeTreeSettings(const Context & context) const;

    static StoragesSettings & instance()
    {
        static StoragesSettings settings;
        return settings;
    }

private:
    mutable std::optional<StorageSettings<BufferSettings>> buffer_settings;   /// Settings of Buffer engine.
    mutable std::optional<StorageSettings<MergeTreeSettings>> merge_tree_settings;   /// Settings of MergeTree* engines.
};

}
