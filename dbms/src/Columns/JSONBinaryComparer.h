#pragma once

#include <Core/Types.h>

namespace DB
{

class ColumnJSONB;

class JSONBinaryComparer
{
public:
    static inline int compareAt(size_t n, size_t m, const ColumnJSONB & lhs, const ColumnJSONB & rhs);
//    {
//        const auto & rhs_iterator = JSONBinaryWriter(rhs, m);
//        const auto & lhs_iterator = JSONBinaryWriter(*this, n);
//
//        while(rhs_iterator.next() && lhs_iterator.next())
//        {
//            if (rhs_iterator.token() != lhs_iterator.token())
//                return lhs_iterator.token() < rhs_iterator.token() ? -1 : 1;
//
//            if (rhs_iterator.token() == JSONToken::Bool)
//                return lhs_iterator.asBool() ? -1 : 1;
//
//            if (rhs_iterator.token() == JSONToken::String)
//            {
//                const auto & lhs_string = lhs_iterator.asString();
//                const auto & rhs_string = rhs_iterator.asString();
//                return memcmpSmallAllowOverflow15(lhs_string.data, lhs_string.size, rhs_string.data, rhs_string.size);
//            }
//
//            if (rhs_iterator.token() == JSONToken::Number)
//            {
//                if (!lhs_iterator.isDouble() && !rhs_iterator.isDouble())
//                {
//                    bool left_negative, right_negative;
//                    UInt64 lhs_unsigned = 0, rhs_unsigned = 0;
//
//                    left_negative = !lhs_iterator.isUnsigned() && lhs_iterator.asSigned() < 0;
//                    right_negative = !rhs_iterator.isUnsigned() && rhs_iterator.asSigned() < 0;
//                    lhs_unsigned = lhs_iterator.isUnsigned() ? lhs_iterator.asUnsigned() : std::abs(lhs_iterator.asSigned());
//                    rhs_unsigned = rhs_iterator.isUnsigned() ? rhs_iterator.asUnsigned() : std::abs(rhs_iterator.asSigned());
//                    if (left_negative != right_negative)
//                        return (left_negative && !right_negative) ? -1 : 1;
//                    else
//                        return lhs_unsigned << __builtin_clz(lhs_unsigned) < rhs_unsigned << __builtin_clz(rhs_unsigned) ? -1 : 1;
//                }
//                else
//                {
//                    /// TODO: optimize
//                    String left_string, right_string;
//
//                    left_string = lhs_iterator.isDouble() ? toString(lhs_iterator.asDouble()) :
//                                  (lhs_iterator.isUnsigned() ? toString(lhs_iterator.asUnsigned()) : toString(lhs_iterator.asSigned()));
//
//                    right_string = rhs_iterator.isDouble() ? toString(rhs_iterator.asDouble()) :
//                                   (rhs_iterator.isUnsigned() ? toString(rhs_iterator.asUnsigned()) : toString(rhs_iterator.asSigned()));
//
//                    return memcmpSmallAllowOverflow15(left_string.data(), left_string.size(), right_string.data(), right_string.size());
//                }
//            }
//        }
//
//        return lhs_iterator.next() ? -1 : 1;
//    }
};

}
