#include <DataTypes/JSONBinaryWriter.h>
#include "JSONBinaryWriter.h"


namespace DB
{

FleeceFormatJSONBIterator::FleeceFormatJSONBIterator(
    const ColumnJSONB & column, size_t offset, const std::shared_ptr<JSONBDataIterator> & parent_ = nullptr)
{
    StringRef ref = column.getDataBinary().getDataAt(offset);
//    impl = std::make_shared<IteratorImpl>(fleece::Value::fromData(fleece::slice(ref.data, ref.size)));
}

//template<typename Fun>
//void JSONBinaryWriter::iterator(const Fun & iterator_func)
//{
//
//}

JSONToken FleeceFormatJSONBIterator::token() const
{
//    value.asDict().
//    return String;
}

std::shared_ptr<JSONBinaryWriter> FleeceFormatJSONBIterator::next() const
{
//    if (initialize)
//        impl = impl->next();
//
//    return (initialize = true);
}

bool FleeceFormatJSONBIterator::asBool() const
{
    return value.asBool();
}

Int64 FleeceFormatJSONBIterator::asSigned() const
{
    return value.asInt();
}

Float64 FleeceFormatJSONBIterator::asDouble() const
{
    return value.isDouble() ? value.asDouble() : value.asFloat();
}

UInt64 FleeceFormatJSONBIterator::asUnsigned() const
{
    return value.asUnsigned();
}

StringRef FleeceFormatJSONBIterator::asString() const
{
    fleece::slice ref = value.asString();
    return StringRef(reinterpret_cast<const char *>(ref.buf), ref.size);
}

bool FleeceFormatJSONBIterator::isDouble() const
{
//    return value.;
}

bool FleeceFormatJSONBIterator::isUnsigned() const
{
    return false;
}

JSONBinaryWriter JSONBinaryWriter::create(const ColumnJSONB & column, size_t offset)
{
    if (!column.isMultipleColumn())
        return FleeceFormatJSONBIterator(column, offset);

    throw Exception("NOT_IMPLEMENTED", ErrorCodes::NOT_IMPLEMENTED);
}

JSONBinaryWriter JSONBinaryWriter::next() const
{
    return std::make_unique<JSONBinaryWriter>(std::move(shared));
//    return JSONBinaryWriter(DB::ColumnJSONB(), 0, DB::JSONBinaryWriter());
}

JSONBinaryWriter JSONBinaryWriter::from(const ColumnJSONB & column, size_t offset)
{
    return JSONBinaryWriter(DB::ColumnJSONB(), 0);
}
}
