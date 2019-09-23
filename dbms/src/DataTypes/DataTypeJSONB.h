#pragma once

#include <Columns/ColumnJSONB.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/JSONB/JSONBSerialization.h>
#include <DataTypes/DataTypeWithSimpleSerialization.h>

namespace DB
{

class DataTypeJSONB final : public IDataType
{
public:
    DataTypeJSONB(bool is_nullable, bool is_low_cardinality);

    Field getDefault() const override { return Null(); }

    TypeIndex getTypeId() const override { return TypeIndex::JSONB; }
    const char * getFamilyName() const override { return "JSONB"; }

    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void deserializeBinary(IColumn & /*column*/, ReadBuffer & /*istr*/) const override
    {
        throw Exception("Method deserializeBinary is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void serializeBinary(const IColumn & /*column*/, size_t /*row_num*/, WriteBuffer & /*ostr*/) const override
    {
        throw Exception("Method serializeBinary is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & reader, bool allow_add_row, bool & row_added) const override;

    /** Each sub-column in a tuple is serialized in separate stream.
      */
    void enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr) const override;

    void serializeBinaryBulkStatePrefix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings &settings, DeserializeBinaryBulkStatePtr &ptr) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column, size_t offset, size_t limit,
        SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column, size_t limit,
        DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state) const override;

    MutableColumnPtr createColumn() const override;
    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }

protected:
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

private:
    const bool is_nullable{false};
    const bool is_low_cardinality{false};
};

}
