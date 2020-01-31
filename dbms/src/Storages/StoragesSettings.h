#pragma once

#include <optional>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

class Context;
class ASTStorage;

class StoragesSettings
{
public:
    template <typename SettingsType>
    struct StorageSettings : public SettingsType
    {
        /// NOTE: will rewrite the AST to add immutable settings.
        void loadFromQuery(ASTStorage & storage_def);
    };

    template <typename SettingsType>
    using StorageSettingsPtr = std::unique_ptr<StorageSettings<SettingsType>>;

    StorageSettingsPtr<MergeTreeSettings> mergeTreeSettings(const Context & context) const;

    const StorageSettings<MergeTreeSettings> & mergeTreeSettingsRef(const Context & context) const;

    static StoragesSettings & instance()
    {
        static StoragesSettings settings;
        return settings;
    }

private:
    mutable std::optional<StorageSettings<MergeTreeSettings>> merge_tree_settings;   /// Settings of MergeTree* engines.
};

}
