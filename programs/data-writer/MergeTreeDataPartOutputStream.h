#pragma once

#include "IPartDataOutputStream.h"
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class MergeTreeDataPartOutputStream : public IPartOutputStream
{
public:
    MergeTreeDataPartOutputStream(
        const std::shared_ptr<MergeTreeDataWriter> & data_writer_ptr_, const StorageMetadataPtr & metadata_snapshot_, MergeTreeData & data_
        , const std::string & data_dir_, const std::string & output_name_);

    void writePrefix() const override;

    String writeSuffix() const override;

    Columns getSampleColumns() const override;

    static Block createBlock(const Columns & write_columns, const NamesAndTypesList & columns)
    {
        Block res;
        if (write_columns.size() != columns.size())
            throw Exception("LOGICAL ERROR: write columns size must be equals structure columns size.", ErrorCodes::LOGICAL_ERROR);

        size_t index = 0;
        for (const auto & column : columns)
        {
            res.insert(ColumnWithTypeAndName(write_columns[index], column.type, column.name));
            ++index;
        }

        return res;
    }

    void writeColumns(const Columns & write_columns) const override;

private:
    MergeTreeData & data;
    StorageMetadataPtr metadata_snapshot;
    std::shared_ptr<MergeTreeDataWriter> data_writer_ptr;
    std::string data_dir, output_name;

    mutable Row partition_key;
    mutable MergeTreeData::MutableDataPartPtr data_part;
    mutable std::shared_ptr<MergedBlockOutputStream> merged_block_out;

    MergeTreeData::MutableDataPartPtr createDataPart(const NamesAndTypesList & columns, BlockWithPartition & block_with_partition) const;

    std::shared_ptr<MergedBlockOutputStream> createMergedBlockOutputStream(
        const NamesAndTypesList & columns, MergeTreeData::MutableDataPartPtr & data_part) const;
};

}
