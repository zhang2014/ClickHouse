#pragma once

#include <Core/Defines.h>
#include <Core/SettingsCollection.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
/** Settings for the Buffer Storage engine.
  * Could be loaded from config or from a CREATE TABLE query (SETTINGS clause).
  */
struct BufferSettings : public SettingsCollection<BufferSettings>
{
#define LIST_OF_BUFFER_SETTINGS(M)                                 \
    M(SettingUInt64, num_layers, 16, "Parallelism layer. Physically, the table will be represented as num_layers of independent buffers.", 0) \
    M(SettingUInt64, flusher_min_time, 1,  "", 0) \
    M(SettingUInt64, flusher_max_time, 10,  "", 0) \
    M(SettingUInt64, flusher_min_rows, DEFAULT_BLOCK_SIZE,  "", 0) \
    M(SettingUInt64, flusher_max_rows, DEFAULT_INSERT_BLOCK_SIZE,  "", 0) \
    M(SettingUInt64, flusher_min_bytes, 10 * 1024 * 1024,  "", 0) \
    M(SettingUInt64, flusher_max_bytes, 256 * DEFAULT_INSERT_BLOCK_SIZE,  "", 0) \


    DECLARE_SETTINGS_COLLECTION(LIST_OF_BUFFER_SETTINGS)

    static std::vector<String> immutableSettingsName() { return {}; }

    static SettingsChanges extractFromEngineArguments(const std::vector<ASTPtr> &  arguments);
};


}
