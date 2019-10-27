#pragma once

#include <Core/Types.h>
#include <ext/scope_guard.h>
#include <rapidjson/reader.h>
#include <IO/ReadBuffer.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/JSONBinaryConverter.h>
#include <Columns/ColumnJSONB.h>
#include <Columns/ColumnString.h>
#include <Columns/JSONBDataMark.h>
#include <rapidjson/writer.h>
#include <DataTypes/JSONBinaryWriter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_PARSE_JSON;
}

struct JSONBSerialization
{
public:
    template <typename InputStream>
    static void deserialize(bool is_nullable, IColumn & column_, const std::unique_ptr<InputStream> & input_stream)
    {
        auto & column_json_binary = static_cast<ColumnJSONB &>(column_);
        JSONBinaryConverter binary_converter(is_nullable, column_json_binary.getKeysDictionary(), column_json_binary.getRelationsDictionary());

        input_stream->skipQuoted();
        rapidjson::EncodedInputStream<rapidjson::UTF8<>, InputStream> is(*input_stream);
        if (!rapidjson::Reader{}.Parse<rapidjson::kParseStopWhenDoneFlag>(is, binary_converter))
            throw Exception("Cannot parse JSON, invalid JSON characters.", ErrorCodes::CANNOT_PARSE_JSON);

        input_stream->skipQuoted();
        binary_converter.finalize(column_json_binary.getRelationsBinary(), column_json_binary.getDataBinary());
    }

    template <typename OutputStream>
    static void serialize(const IColumn & column, size_t row_num, const std::unique_ptr<OutputStream> & output_stream)
    {
        rapidjson::Writer<OutputStream> writer(*output_stream.get());
        JSONBinaryWriter::from(*checkAndGetColumn<ColumnJSONB>(column), row_num).iterator(writer);
    }

    template <typename FromType>
    static void serialize(const ColumnVector<FromType> & source_column, ColumnJSONB & to_column)
    {
        const typename ColumnVector<FromType>::Container & from_vec = source_column.getData();

        const auto & createRelations = [&](const JSONBDataMark & mark)
        {
            std::vector<UInt64> track_stack(1, UInt64(mark));
            return to_column.getRelationsDictionary().uniqueInsertData(
                reinterpret_cast<const char *>(track_stack.data()), track_stack.size() * sizeof(UInt64));
        };

        const auto serializeImpl = [&](const JSONBDataMark & mark)
        {
            fleece::Encoder encoder;
            UInt64 relations_id = createRelations(mark);
            auto & relations_binary = assert_cast<ColumnArray &>(to_column.getRelationsBinary());

            for (size_t index = 0; index < from_vec.size(); ++index)
            {
                switch (mark)
                {
                    case JSONBDataMark::Int64Mark:  encoder.writeInt(Int64(from_vec[index])); break;
                    case JSONBDataMark::UInt64Mark: encoder.writeUInt(UInt64(from_vec[index])); break;
                    case JSONBDataMark::Float64Mark: encoder.writeDouble(Float64(from_vec[index])); break;
                    default: throw Exception("LOGICAL_ERROR: expected Int64, UInt64, Float64 JSONBMark.", ErrorCodes::LOGICAL_ERROR);
                }

                relations_binary.getOffsets().push_back(relations_binary.getData().size() + 1);
                static_cast<ColumnUInt64 &>(relations_binary.getData()).getData().push_back(relations_id);

                const auto & output = encoder.extractOutput();
                to_column.getDataBinary().insertData(reinterpret_cast<const char *>(output.buf), output.size);
                encoder.reset();
            }
        };

        if constexpr (std::is_same_v<FromType, UInt8>)  serializeImpl(JSONBDataMark::UInt64Mark);
        else if constexpr (std::is_same_v<FromType, UInt16>)  serializeImpl(JSONBDataMark::UInt64Mark);
        else if constexpr (std::is_same_v<FromType, UInt32>)  serializeImpl(JSONBDataMark::UInt64Mark);
        else if constexpr (std::is_same_v<FromType, UInt64>)  serializeImpl(JSONBDataMark::UInt64Mark);
        else if constexpr (std::is_same_v<FromType, Int8>)  serializeImpl(JSONBDataMark::Int64Mark);
        else if constexpr (std::is_same_v<FromType, Int16>)  serializeImpl(JSONBDataMark::Int64Mark);
        else if constexpr (std::is_same_v<FromType, Int32>)  serializeImpl(JSONBDataMark::Int64Mark);
        else if constexpr (std::is_same_v<FromType, Int64>)  serializeImpl(JSONBDataMark::Int64Mark);
        else if constexpr (std::is_same_v<FromType, Float32>)  serializeImpl(JSONBDataMark::Float64Mark);
        else if constexpr (std::is_same_v<FromType, Float64>)  serializeImpl(JSONBDataMark::Float64Mark);
        else throw Exception("LOGICAL_ERROR: it is bug.", ErrorCodes::LOGICAL_ERROR);
    }
};

}
