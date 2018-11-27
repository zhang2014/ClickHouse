#include <QingCloud/Common/getPropertyOrChildValue.h>

namespace DB
{
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

}