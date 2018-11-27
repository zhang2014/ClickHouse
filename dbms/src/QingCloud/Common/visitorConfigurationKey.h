#pragma once

#include <functional>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SHARD_HAS_NO_CONNECTIONS;
}

inline void visitorConfigurationKey(const Poco::Util::AbstractConfiguration &configuration, const String &configuration_prefix,
                                    const String &visitor_key, const std::function<void(const String &)> & visitor)
{
    Poco::Util::AbstractConfiguration::Keys configuration_keys;
    configuration.keys(configuration_prefix, configuration_keys);

    if (configuration_keys.empty())
        throw Exception("No elements specified in config at path " + configuration_prefix, ErrorCodes::SHARD_HAS_NO_CONNECTIONS);

    for (const auto & configuration_key : configuration_keys)
        if (startsWith(configuration_key, visitor_key))
            visitor(configuration_prefix + "." + configuration_key);
}

}