#pragma once

#include <ext/scope_guard.h>
#include <Columns/ColumnJSONB.h>
#include <Columns/JSONBDataMark.h>
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif
#include <Fleece/Dict.hh>
#include <Fleece/Value.hh>
#include "JSONBSerialization.h"

#if !__clang__
#pragma GCC diagnostic pop
#endif

namespace DB
{

enum class JSONToken
{
    Comma, Key, String, Number, Bool, ArrayBegin, ArrayEnd, ObjectBegin, ObjectEnd,
};
/** For JSONB, it can be stored in two ways: fleece format and multiple columns.
 *  This class can easily access them in a unified way
 */
struct JSONBinaryWriter
{
public:
    template <typename Writer>
    void iterator(Writer & writer)
    {
        if (column.isMultipleColumn())
            throw Exception("Multiple Columns is not implement.", ErrorCodes::NOT_IMPLEMENTED);

        const StringRef & binary_ref = column.getDataBinary().getDataAt(offset);
        const auto & binary_object = fleece::Value::fromData(fleece::slice(binary_ref.data, binary_ref.size));

        writeValue(binary_object, writer);
    }

    static JSONBinaryWriter from(const ColumnJSONB & column, size_t offset)
    {
        return JSONBinaryWriter(column, offset);
    }

private:
    const ColumnJSONB & column;
    size_t offset;

    JSONBinaryWriter(const ColumnJSONB & column_, size_t offset_) : column(column_), offset(offset_) {}

    template <typename Writer>
    void writeNumberValue(const fleece::Value & value, const Writer & writer)
    {
        if (value.isInteger())
        {
            if (value.isUnsigned())
                writer.Uint64(value.asUnsigned());
            else
                writer.Int64(value.asInt());
        }
        else if (value.isDouble())
            writer.Double(value.asDouble());
        else
            writer.Double(value.asFloat());
    }

    template <typename Writer>
    void writeStringValue(const fleece::Value & value, const Writer & writer)
    {
        const auto & string_value = value.asString();
        writer.String(reinterpret_cast<const char *>(string_value.buf), string_value.size);
    }

    template <typename Writer>
    void writeNullableValue(const fleece::Value & /*value*/, const Writer & writer)
    {
        if (unlikely(!column.isNullable()))
            throw Exception("Cannot parse NULL, you can use Nullable(JSONB) instead of JSONB.", ErrorCodes::CANNOT_PARSE_JSON);

        writer.Null();
    }

    template <typename Writer>
    void writeBooleanValue(const fleece::Value & value, const Writer & writer)
    {
        writer.Bool(value.asBool());
    }

    template <typename Writer>
    void writeObjectValue(const fleece::Dict &value, const Writer &writer)
    {
        size_t member_size = value.count();
        fleece::Dict::iterator dict_member_iterator = value.begin();

        writer.StartObject();
        SCOPE_EXIT({ writer.EndObject(); });
        for (size_t index = 0; index < member_size; ++index, ++dict_member_iterator)
        {
            const auto & key = dict_member_iterator.key();

            if (const auto & keys_dictionary = checkAndGetColumn<ColumnString>(*column.getKeysDictionary().getNestedColumn()))
            {
                const auto & string_key = keys_dictionary->getDataAt(UInt64(key->asInt()) - UInt64(JSONBDataMark::End));
                writer.Key(string_key.data, string_key.size);
                writeValue(*dict_member_iterator.value(), writer);
            }
        }
    }

    template <typename Writer>
    void writeValue(const fleece::Value & value, Writer & writer)
    {
        switch(value.type())
        {
            case fleece::valueType::kNumber: writeNumberValue(value, writer); break;
            case fleece::valueType::kString: writeStringValue(value, writer); break;
            case fleece::valueType::kNull:  writeNullableValue(value, writer); break;
            case fleece::valueType::kBoolean: writeBooleanValue(value, writer); break;
            case fleece::valueType::kDict: writeObjectValue(value.asDict(), writer); break;
            case fleece::valueType::kData:
            case fleece::valueType::kArray: throw Exception("Cannot support JSONArray.", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
};

}
