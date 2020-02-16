#include <Storages/Buffer/BufferSettings.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Common/FieldVisitors.h>
#include <Common/SettingsChanges.h>
#include <Core/SettingsCollectionImpl.h>

namespace DB
{

IMPLEMENT_SETTINGS_COLLECTION(BufferSettings, LIST_OF_BUFFER_SETTINGS)

SettingsChanges BufferSettings::extractFromEngineArguments(const std::vector<ASTPtr> & arguments)
{
    return {
        SettingChange{"num_layers", arguments[2]->as<ASTLiteral &>().value},
        SettingChange{"flusher_min_time", arguments[3]->as<ASTLiteral &>().value},
        SettingChange{"flusher_max_time", arguments[4]->as<ASTLiteral &>().value},
        SettingChange{"flusher_min_rows", arguments[5]->as<ASTLiteral &>().value},
        SettingChange{"flusher_max_rows", arguments[6]->as<ASTLiteral &>().value},
        SettingChange{"flusher_min_bytes", arguments[7]->as<ASTLiteral &>().value},
        SettingChange{"flusher_max_bytes", arguments[8]->as<ASTLiteral &>().value},
    };
}

}
