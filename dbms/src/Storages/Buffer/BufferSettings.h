#pragma once

#include <Core/SettingsCollection.h>



namespace DB
{
/** Settings for the Buffer Storage engine.
  * Could be loaded from config or from a CREATE TABLE query (SETTINGS clause).
  */
struct BufferSettings : public SettingsCollection<BufferSettings>
{
#define LIST_OF_MERGE_TREE_SETTINGS(M)                                 \
    M(SettingUInt64, num_layers, 8192, "Parallelism layer. Physically, the table will be represented as num_layers of independent buffers. Recommended value: 16", 0) \

    DECLARE_SETTINGS_COLLECTION(LIST_OF_MERGE_TREE_SETTINGS)

    static std::vector<String> immutableSettingsName() { return {}; }
};


}
