#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnJSONB.h>
#include <Functions/SimdJSONParser.h>
#include <Functions/RapidJSONParser.h>
#include <Columns/ColumnString.h>
#include "DummyJSONParser.h"

namespace DB
{

/// Represents a move of a JSON iterator described by a single argument passed to a JSON function.
/// For example, the call JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1)
/// contains two moves: {MoveType::ConstKey, "b"} and {MoveType::ConstIndex, 1}.
/// Keys and indices can be nonconst, in this case they are calculated for each row.
enum class MoveType
{
    Key,
    Index,
    ConstKey,
    ConstIndex,
};

struct Move
{
    Move(MoveType type_, size_t index_ = 0) : type(type_), index(index_) {}
    Move(MoveType type_, const String & key_) : type(type_), key(key_) {}
    MoveType type;
    String key;
    size_t index = 0;
    IColumn * key_or_index;
};

template<template<typename> typename Extractor>
class JSONColumnExtractor
{
public:
    JSONColumnExtractor(const ColumnWithTypeAndName & src_column)
        : source_column(static_cast<const ColumnJSONB &>(*src_column.column))
    {
        /// TODO: const column and non const column
    }

    static inline bool alwaysConstKeyAndIndex(const std::vector<Move> & moves)
    {
        for (size_t index = 0; index < moves.size(); ++index)
        {
            if (moves[index].type == MoveType::Key)
                return false;
            else if (moves[index].type == MoveType::Index)
                return false;
        }

        return true;
    }

    size_t extractWithConstKey(const char * function_name, const std::vector<Move> & moves, IColumn & dest_column, const DataTypePtr & res_type)
    {
        auto column_struct = source_column.getStruct();
        for (size_t index = 0; index < moves.size() && column_struct; ++index)
        {
            switch (moves[index].type)
            {
                case MoveType::ConstKey: column_struct = column_struct->getChildren(moves[index].key); break;
                default: throw Exception("JSONB does not support the use of index to extract elements.", ErrorCodes::NOT_IMPLEMENTED);
            }
        }

        Extractor<DummyJSONParser>::create(function_name, res_type).extract(column_struct, dest_column, 0, source_column.size());
        return source_column.size();
    }

    size_t extract(const char * function_name, const std::vector<Move> & moves, IColumn & dest_column, const DataTypePtr & res_type, size_t input_rows)
    {
        if (alwaysConstKeyAndIndex(moves))
            return extractWithConstKey(moves, dest_column);
        else
        {
            const auto & extractor = Extractor<DummyJSONParser>::create(function_name, res_type);
            for (size_t row = 0; row < input_rows; ++row)
            {
                ColumnJSONBStructPtr column_struct = source_column.getStruct();

                for (size_t index = 0; index < moves.size() && column_struct; ++index)
                {
                    switch (moves[index].type)
                    {
                        case MoveType::Key: column_struct = column_struct->getChildren((*moves[index].key_or_index)[row].get<String>()); break;
                        case MoveType::ConstKey: column_struct = column_struct->getChildren(moves[index].key); break;
                        default: throw Exception("JSONB does not support the use of index to extract elements.", ErrorCodes::NOT_IMPLEMENTED);
                    }
                }

                extractor.extract(column_struct, dest_column, row, 1);
            }
        }
    }

private:
    const ColumnJSONB & source_column;
};


template <typename Extractor, typename JSONParser>
class StringJSONColumnExtractor
{
public:
    StringJSONColumnExtractor(const ColumnWithTypeAndName & src_column)
    {
        /// TODO: chars, offsets
        /// Preallocate memory in parser if necessary.
//        JSONParser parser;
//            if (parser.need_preallocate)
//                parser.preallocate(calculateMaxSize(offsets));

    }

    size_t offsetAt(size_t index) { return source_column_offsets[index - 1]; }
    size_t lengthAt(size_t index) { return source_column_offsets[index] - source_column_offsets[index - 1] - 1; }

    size_t extract(const char * function_name, const std::vector<Move> & moves, IColumn & dest_column, const DataTypePtr & res_type, size_t input_rows)
    {
        const auto & extractor = Extractor::create(function_name, res_type);

        for (size_t index = 0; index < input_rows; ++index)
        {
            if (bool ok = parser.parse(StringRef(reinterpret_cast<const char *>(&source_column_chars[offsetAt(index)]), lengthAt(index))))
            {
                auto iterator = parser.getRoot();

                /// Perform moves.
                for (size_t j = 0; (j != moves.size()) && ok; ++j)
                {
                    switch (moves[j].type)
                    {
                        case MoveType::ConstKey:    ok = moveIteratorToElementByKey(iterator, moves[j].key);  break;
                        case MoveType::ConstIndex:  ok = moveIteratorToElementByIndex(iterator, moves[j].index);  break;
                        case MoveType::Key:   ok = moveIteratorToElementByKey(iterator, *moves[j].key_or_index, index); break;
                        case MoveType::Index: ok = moveIteratorToElementByIndex(iterator, *moves[j].key_or_index, index); break;
                    }
                }

                extractor.extract(dest_column, iterator);
            }
        }
    }

private:
    JSONParser parser;
    const ColumnString & source_column;
    const ColumnString::Chars & source_column_chars;
    const ColumnString::Offsets & source_column_offsets;

    /// Performs moves of types MoveType::Key and MoveType::ConstKey.
    static bool moveIteratorToElementByKey(typename JSONParser::Iterator & it, const String & key)
    {
        if (JSONParser::isObject(it))
            return JSONParser::objectMemberByName(it, key);
        return false;
    }

    static bool moveIteratorToElementByKey(typename JSONParser::Iterator & it, const IColumn & key, size_t row)
    {
        return moveIteratorToElementByKey(it, key[row].get<String>());
    }

    /// Performs moves of types MoveType::Index and MoveType::ConstIndex.
    static bool moveIteratorToElementByIndex(typename JSONParser::Iterator & it, int index)
    {
        if (JSONParser::isArray(it))
        {
            if (index > 0)
                return JSONParser::arrayElementByIndex(it, index - 1);
            else
                return JSONParser::arrayElementByIndex(it, JSONParser::sizeOfArray(it) + index);
        }
        if (JSONParser::isObject(it))
        {
            if (index > 0)
                return JSONParser::objectMemberByIndex(it, index - 1);
            else
                return JSONParser::objectMemberByIndex(it, JSONParser::sizeOfObject(it) + index);
        }
        return false;
    }

    /// Performs moves of types MoveType::Index and MoveType::ConstIndex.
    static bool moveIteratorToElementByIndex(typename JSONParser::Iterator & it, const IColumn & index_column, size_t row)
    {
        return moveIteratorToElementByIndex(it, index_column[row].get<UInt64>());
    }

//    static size_t calculateMaxSize(const ColumnString::Offsets & offsets)
//    {
//        size_t max_size = 0;
//        for (const auto i : ext::range(0, offsets.size()))
//            if (max_size < offsets[i] - offsets[i - 1])
//                max_size = offsets[i] - offsets[i - 1];
//
//        if (max_size < 1)
//            max_size = 1;
//        return max_size;
//    }

};

template<template<typename> typename Extractor>
using SimdJSONColumnExtractor = StringJSONColumnExtractor<Extractor<SimdJSONParser>, SimdJSONParser>;
template<template<typename> typename Extractor>
using RapidJSONColumnExtractor = StringJSONColumnExtractor<Extractor<RapidJSONParser>, RapidJSONParser>;

}
