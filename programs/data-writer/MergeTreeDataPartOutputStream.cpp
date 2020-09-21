#include <Core/Block.h>
#include "MergeTreeDataPartOutputStream.h"
#include <Storages/StorageInMemoryMetadata.h>
#include <boost/filesystem.hpp>

namespace DB
{

MergeTreeDataPartOutputStream::MergeTreeDataPartOutputStream(
    const std::shared_ptr<MergeTreeDataWriter> & data_writer_ptr_, const StorageMetadataPtr & metadata_snapshot_, MergeTreeData & data_
    , const std::string & data_dir_, const std::string & output_name_)
    : data(data_), metadata_snapshot(metadata_snapshot_), data_writer_ptr(data_writer_ptr_), data_dir(data_dir_), output_name(output_name_)
{
}

void MergeTreeDataPartOutputStream::writePrefix() const
{
}

static void renamePart(const std::string & source_path, const std::string & destination_path)
{
    Poco::File source(source_path);

    if (source.isDirectory())
    {
        Poco::File destination(destination_path);
        destination.createDirectories();

        std::vector<std::string> files;
        source.list(files);

        for (const auto & file : files)
            renamePart(source_path + "/" + file, destination_path + "/" + file);
    }
    else
    {
        source.renameTo(destination_path);
    }
}

String MergeTreeDataPartOutputStream::writeSuffix() const
{
    merged_block_out->writeSuffixAndFinalizePart(data_part, true, nullptr, nullptr);

    if (output_name.empty())
    {
        renamePart(data_part->getFullPath(), data_dir + "/" + data_part->info.getPartName());
        return data_dir + "/" + data_part->info.getPartName();
    }

    renamePart(data_part->getFullPath(), data_dir + "/" + output_name);
    return data_dir + "/" + output_name;
}

Columns MergeTreeDataPartOutputStream::getSampleColumns() const
{
    Columns sample_columns;
    Block header = metadata_snapshot->getSampleBlock();

    sample_columns.reserve(header.columns());
    for (size_t index = 0; index < header.columns(); ++index)
        sample_columns.emplace_back(header.getByPosition(index).column);

    return sample_columns;
}

void MergeTreeDataPartOutputStream::writeColumns(const Columns & write_columns) const
{
    const NamesAndTypesList & columns = metadata_snapshot->getColumns().getAllPhysical();

    Block data_block = createBlock(write_columns, columns);
    BlocksWithPartition blocks_with_partition = data_writer_ptr->splitBlockIntoParts(
        data_block, std::numeric_limits<size_t>::max(), metadata_snapshot);

    if (blocks_with_partition.size() != 1)
        throw Exception("LOGICAL ERROR: Data of multiple partitions are written. Only one partition can be written at a time. - 1",
            ErrorCodes::LOGICAL_ERROR);

    if (!merged_block_out)
    {
        partition_key.assign(blocks_with_partition[0].partition);
        data_part = createDataPart(columns, blocks_with_partition[0]);
        merged_block_out = createMergedBlockOutputStream(columns, data_part);
        merged_block_out->writePrefix();
    }
    else if (partition_key != blocks_with_partition[0].partition)
    {
        if (partition_key.size() != blocks_with_partition[0].partition.size())
            throw Exception("LOGICAL ERROR: Data of multiple partitions are written. Only one partition can be written at a time. - 2",
                    ErrorCodes::LOGICAL_ERROR);

        for (size_t index = 0; index < partition_key.size(); ++index)
            if (partition_key[index] != blocks_with_partition[0].partition[index])
                throw Exception("LOGICAL ERROR: partition column " + toString(partition_key[index]) + " not equals " +
                    toString(blocks_with_partition[0].partition[index]), ErrorCodes::LOGICAL_ERROR);

        throw Exception("LOGICAL ERROR: Data of multiple partitions are written. Only one partition can be written at a time. - 2",
                ErrorCodes::LOGICAL_ERROR);
    }

    data_part->minmax_idx.update(blocks_with_partition[0].block, data.minmax_idx_columns);
    merged_block_out->write(blocks_with_partition[0].block);
}

MergeTreeData::MutableDataPartPtr MergeTreeDataPartOutputStream::createDataPart(
    const NamesAndTypesList & columns, BlockWithPartition & block_with_partition) const
{
    static const String TMP_PREFIX = "tmp_insert_";

    /// This will generate unique name in scope of current server process.
    Int64 temp_index = data.insert_increment.get();

    MergeTreePartition partition(std::move(block_with_partition.partition));
    MergeTreePartInfo new_part_info(partition.getID(metadata_snapshot->getPartitionKey().sample_block), temp_index, temp_index, 0);

    String part_name = new_part_info.getPartName();
    VolumePtr volume = data.getStoragePolicy()->getVolume(0);

    auto new_data_part = data.createPart(
        part_name, MergeTreeDataPartType::WIDE, new_part_info, volume, TMP_PREFIX + part_name);

    new_data_part->is_temp = true;
    new_data_part->setColumns(columns);
    new_data_part->partition = std::move(partition);
    return new_data_part;
}

std::shared_ptr<MergedBlockOutputStream> MergeTreeDataPartOutputStream::createMergedBlockOutputStream(
    const NamesAndTypesList & columns, MergeTreeData::MutableDataPartPtr & data_part_) const
{
    return std::make_shared<MergedBlockOutputStream>(data_part_, metadata_snapshot, columns,
         MergeTreeIndices{}, CompressionCodecFactory::instance().getDefaultCodec());
}


}
