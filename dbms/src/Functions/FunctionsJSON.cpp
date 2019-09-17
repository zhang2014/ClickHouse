#include <Functions/FunctionsJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct NameJSONHas { static constexpr auto name{"JSONHas"}; };
struct NameJSONLength { static constexpr auto name{"JSONLength"}; };
struct NameJSONKey { static constexpr auto name{"JSONKey"}; };
struct NameJSONType { static constexpr auto name{"JSONType"}; };
struct NameJSONExtractInt { static constexpr auto name{"JSONExtractInt"}; };
struct NameJSONExtractUInt { static constexpr auto name{"JSONExtractUInt"}; };
struct NameJSONExtractFloat { static constexpr auto name{"JSONExtractFloat"}; };
struct NameJSONExtractBool { static constexpr auto name{"JSONExtractBool"}; };
struct NameJSONExtractString { static constexpr auto name{"JSONExtractString"}; };
struct NameJSONExtract { static constexpr auto name{"JSONExtract"}; };
struct NameJSONExtractKeysAndValues { static constexpr auto name{"JSONExtractKeysAndValues"}; };
struct NameJSONExtractRaw { static constexpr auto name{"JSONExtractRaw"}; };

void registerFunctionsJSON(FunctionFactory & factory)
{
    factory.registerFunction<FunctionJSON<NameJSONHas, JSONHasImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONLength, JSONLengthImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONKey, JSONKeyImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONType, JSONTypeImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractInt, JSONExtractInt64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractUInt, JSONExtractUInt64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractFloat, JSONExtractFloat64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractBool, JSONExtractBoolImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractString, JSONExtractStringImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtract, JSONExtractImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractKeysAndValues, JSONExtractKeysAndValuesImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractRaw, JSONExtractRawImpl>>();
}

}
