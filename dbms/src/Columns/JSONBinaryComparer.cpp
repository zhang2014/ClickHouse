#include <Columns/JSONBinaryComparer.h>
#include <Columns/ColumnJSONB.h>
#include <DataTypes/JSONBSerialization.h>

namespace DB
{

//static inline int compareJSONBStructAt(const ColumnArray & lhs, const ColumnArray & rhs, const ColumnJSONB & lhs, const ColumnJSONB & rhs)
//{
//
//}

int JSONBinaryComparer::compareAt(size_t n, size_t m, const ColumnJSONB & lhs, const ColumnJSONB & rhs)
{
    const auto & lhs_relations_binary = checkAndGetColumn<ColumnArray>(lhs.getRelationsBinary());
    const auto & rhs_relations_binary = checkAndGetColumn<ColumnArray>(rhs.getRelationsBinary());

    if (lhs_relations_binary && rhs_relations_binary)
    {
        /// For those with fewer attributes in JSON, they will always be smaller than those with more attributes.
        size_t lhs_size = lhs_relations_binary->getOffsets()[n] - lhs_relations_binary->getOffsets()[n - 1];
        size_t rhs_size = rhs_relations_binary->getOffsets()[m] - rhs_relations_binary->getOffsets()[m - 1];
        if (rhs_size != lhs_size)
            return lhs_size < rhs_size ? -1 : 1;

        const auto & lhs_unique_relations = checkAndGetColumn<ColumnUInt64>(lhs_relations_binary->getData());
        const auto & rhs_unique_relations = checkAndGetColumn<ColumnUInt64>(rhs_relations_binary->getData());
        const auto & lhs_relations_dictionary = checkAndGetColumn<ColumnArray>(*lhs.getRelationsDictionary().getNestedColumn());
        const auto & rhs_relations_dictionary = checkAndGetColumn<ColumnArray>(*rhs.getRelationsDictionary().getNestedColumn());

        if (lhs_unique_relations && rhs_unique_relations && lhs_relations_dictionary && rhs_relations_dictionary)
        {
            /// The simple structure in JSON is always smaller than the complex structure.
            size_t lhs_offset = lhs_relations_binary->getOffsets()[n - 1];
            size_t rhs_offset = rhs_relations_binary->getOffsets()[m - 1];
            for (size_t index = 0; index < lhs_size; ++index)
            {
                const auto & lhs_relation_index = lhs_unique_relations->getData()[lhs_offset + index];
                const auto & rhs_relation_index = rhs_unique_relations->getData()[rhs_offset + index];

                const auto & lhs_relations_size = lhs_relations_dictionary->getOffsets()[lhs_relation_index] - lhs_relations_dictionary->getOffsets()[lhs_relation_index - 1];
                const auto & rhs_relations_size = rhs_relations_dictionary->getOffsets()[rhs_relation_index] - rhs_relations_dictionary->getOffsets()[rhs_relation_index - 1];
                if (lhs_relations_size != rhs_relations_size)
                    return lhs_relations_size < rhs_relations_size ? -1 : 1;
            }
        }

//        const auto & lhs_keys_dictionary = checkAndGetColumn<ColumnString>(*lhs.getKeysDictionary().getNestedColumn());
//        const auto & rhs_keys_dictionary = checkAndGetColumn<ColumnString>(*rhs.getKeysDictionary().getNestedColumn());
//
////        size_t min_size = std::min(lhs_size, rhs_size);
////        for (size_t i = 0; i < min_size; ++i)
////            checkAndGetColumn<ColumnString>(*lhs.getKeysDictionary().getNestedColumn());
////            if (int res = getData().compareAt(offsetAt(n) + i, rhs.offsetAt(m) + i, *rhs.data.get(), nan_direction_hint))
////                return res;
////
////        for (size_t i = 0; i < min_size; ++i)
////            if (int res = getData().compareAt(offsetAt(n) + i, rhs.offsetAt(m) + i, *rhs.data.get(), nan_direction_hint))
////                return res;
//
//        return lhs_size < rhs_size
//               ? -1
//               : (lhs_size == rhs_size
//                  ? 0
//                  : 1);
    }
    return 0;
}

}
