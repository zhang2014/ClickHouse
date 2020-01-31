#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Core/SettingsCollectionImpl.h>


namespace DB
{

IMPLEMENT_SETTINGS_COLLECTION(MergeTreeSettings, LIST_OF_MERGE_TREE_SETTINGS)

bool MergeTreeSettings::isReadonlySetting(const String & name)
{
    return name == "index_granularity" || name == "index_granularity_bytes";
}

std::vector<String> MergeTreeSettings::immutableSettingsName()
{
    return {"index_granularity"};
}

}
