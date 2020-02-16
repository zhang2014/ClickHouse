#include <Storages/StoragesSettings.h>

#include <optional>
#include <Interpreters/Context.h>
#include <Core/SettingsCollection.h>
#include <Parsers/IAST.h>
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
static StorageSettings<T> loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config, const String & key)
{
    StorageSettings<T> res;

    if (!config.has(key))
        return res;

    Poco::Util::AbstractConfiguration::Keys config_items_key;
    config.keys(key, config_items_key);

    try
    {
        StorageSettings<MergeTreeSettings> ss;
        ss.changes();
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

const StorageSettings<BufferSettings> & StoragesSettings::bufferSettings(const Context & context) const
{
    const auto lock = context.getLock();

    if (!buffer_settings)
        buffer_settings.emplace(loadSettingsFromConfig<BufferSettings>(context.getConfigRef(), "storage_settings.buffer"));

    return *buffer_settings;
}

const StorageSettings<MergeTreeSettings> & StoragesSettings::mergeTreeSettings(const Context & context) const
{
    const auto lock = context.getLock();

    if (!merge_tree_settings)
    {
        /// For merge tree, we need to be compatible with the previous configuration
        const StorageSettings<MergeTreeSettings> & latest_settings =
            loadSettingsFromConfig<MergeTreeSettings>(context.getConfigRef(), "storage_settings.merge_tree");
        merge_tree_settings.emplace(loadSettingsFromConfig<MergeTreeSettings>(context.getConfigRef(), "merge_tree"));
        merge_tree_settings->applyChanges(latest_settings.changes());
    }

    return *merge_tree_settings;
}

template<typename SettingsType>
void StorageSettings<SettingsType>::loadFromQuery(ASTStorage & storage_def)
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

template<typename SettingsType>
void StorageSettings<SettingsType>::loadFromEngineArguments(const std::vector<ASTPtr> & /*arguments*/)
{

}

template struct StorageSettings<BufferSettings>;
template struct StorageSettings<MergeTreeSettings>;

}
