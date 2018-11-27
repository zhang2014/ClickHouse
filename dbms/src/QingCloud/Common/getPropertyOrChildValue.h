#pragma once

#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>

namespace DB
{

template <typename T>
struct ConfigurationTypeToEnum {
    inline static T & getValue(const Poco::Util::AbstractConfiguration &, const std::string &)
    {
        throw Exception("");
    }
};

template <typename T>
inline T getPropertyOrChildValue(const Poco::Util::AbstractConfiguration & configuration, const String & configuration_key, const String & property_or_child_name)
{
    if (configuration.has(configuration_key + "[@" + property_or_child_name + "]"))
        return ConfigurationTypeToEnum<std::decay_t<T>>::getValue(configuration, configuration_key + "[@" + property_or_child_name + "]");

    return ConfigurationTypeToEnum<std::decay_t<T>>::getValue(configuration, configuration_key + "." + property_or_child_name);
}

template <typename T>
inline T getPropertyOrChildValue(const Poco::Util::AbstractConfiguration & configuration, const String & configuration_key, const String & property_or_child_name, const T & default_value)
{
    if (configuration.has(configuration_key + "[@" + property_or_child_name + "]"))
        return ConfigurationTypeToEnum<std::decay_t<T>>::getValue(configuration, configuration_key + "[@" + property_or_child_name + "]");

    if (configuration.has(configuration_key + "." + property_or_child_name))
        return ConfigurationTypeToEnum<std::decay_t<T>>::getValue(configuration, configuration_key + "." + property_or_child_name);

    return default_value;
}


template <> struct ConfigurationTypeToEnum<String>  {
    inline static String getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return configuration.getString(key);
    }
};

template <> struct ConfigurationTypeToEnum<UInt64>  {
    inline static UInt64 getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return configuration.getUInt64(key);
    }
};

template <> struct ConfigurationTypeToEnum<UInt16>  {
    inline static UInt16 getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return (UInt16) configuration.getUInt(key);
    }
};

template <> struct ConfigurationTypeToEnum<UInt32>  {
    inline static UInt32 getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return configuration.getUInt(key);
    }
};

template <> struct ConfigurationTypeToEnum<bool>  {
    inline static bool getValue(const Poco::Util::AbstractConfiguration & configuration, const std::string & key)
    {
        return configuration.getBool(key);
    }
};

}

