#include <Storages/StoragesSettings.h>

#include <optional>
#include <Interpreters/Context.h>
#include <Core/SettingsCollection.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/MergeTree/MergeTreeSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_SETTING;
    extern const int INVALID_CONFIG_PARAMETER;
}

template <typename T>
static StoragesSettings::StorageSettings<T> loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config, const String & key)
{
    StoragesSettings::StorageSettings<T> res;

    if (!config.has(key))
        return res;

    Poco::Util::AbstractConfiguration::Keys config_items_key;
    config.keys(key, config_items_key);

    try
    {
        for (const String & config_item_key : config_items_key)
            res.set(config_item_key, config.getString(key + "." + config_item_key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            throw Exception(e.message() + " in MergeTree config", ErrorCodes::INVALID_CONFIG_PARAMETER);
        else
            e.rethrow();
    }

    return res;
}

StoragesSettings::StorageSettingsPtr<MergeTreeSettings> StoragesSettings::mergeTreeSettings(const Context & context) const
{
    return std::make_unique<StorageSettings<MergeTreeSettings>>(mergeTreeSettingsRef(context));
}

const StoragesSettings::StorageSettings<MergeTreeSettings> & StoragesSettings::mergeTreeSettingsRef(const Context & context) const
{
    const auto lock = context.getLock();

    if (!merge_tree_settings)
        merge_tree_settings.emplace(loadSettingsFromConfig<MergeTreeSettings>(context.getConfigRef(), "merge_tree"));

    return *merge_tree_settings;
}

template<typename SettingsType>
void StoragesSettings::StorageSettings<SettingsType>::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            SettingsType::applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                throw Exception(e.message() + " for storage " + storage_def.engine->name, ErrorCodes::BAD_ARGUMENTS);
            else
                e.rethrow();
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    SettingsChanges & changes = storage_def.settings->changes;
    for (const auto & immutable_name : SettingsType::immutableSettingsName())
    {
        if (std::find_if(changes.begin(), changes.end(),
                  [&](const SettingChange & c) { return c.name == immutable_name; })
            == changes.end())
            changes.push_back(SettingChange{immutable_name, SettingsType::get(immutable_name)});
    }
}

template struct StoragesSettings::StorageSettings<MergeTreeSettings>;

}
