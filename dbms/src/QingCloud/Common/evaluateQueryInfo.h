#pragma once

#include <Core/Types.h>
#include <Interpreters/Context.h>

namespace DB
{

std::pair<String, UInt64> getWritingInfo(const Settings &settings, const Context &context)
{
    String current_version = settings.writing_version;

    if (current_version.empty())
        return std::pair(context.getMultiplexedVersion()->getCurrentWritingVersion(), settings.writing_shard_index);


    return std::pair(current_version, UInt64(settings.writing_shard_index));
}

std::pair<String, UInt64> getQueryInfo(const Context & context)
{
    Settings settings = context.getSettingsRef();
    return std::pair(settings.query_version.toString(), UInt64(settings.query_shard_index));
}

}