#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <IO/createWriteBufferFromFileBase.h>
#include <DataTypes/NestedUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}


IMergedBlockOutputStream::IMergedBlockOutputStream(
    MergeTreeData & storage_,
    const std::string & part_path_,
    size_t min_compress_block_size_,
    size_t max_compress_block_size_,
    CompressionCodecPtr codec_,
    size_t aio_threshold_,
    bool blocks_are_granules_size_,
    const MergeTreeIndexGranularity & index_granularity_)
    : storage(storage_)
    , part_path(part_path_)
    , min_compress_block_size(min_compress_block_size_)
    , max_compress_block_size(max_compress_block_size_)
    , aio_threshold(aio_threshold_)
    , marks_file_extension(storage.canUseAdaptiveGranularity() ? getAdaptiveMrkExtension() : getNonAdaptiveMrkExtension())
    , blocks_are_granules_size(blocks_are_granules_size_)
    , index_granularity(index_granularity_)
    , compute_granularity(index_granularity.empty())
    , codec(std::move(codec_))
    , with_final_mark(storage.settings.write_final_mark && storage.canUseAdaptiveGranularity())
{
    if (blocks_are_granules_size && !index_granularity.empty())
        throw Exception("Can't take information about index granularity from blocks, when non empty index_granularity array specified", ErrorCodes::LOGICAL_ERROR);
}


IDataType::OutputStreamGetter IMergedBlockOutputStream::createStreamGetter(
    const String & name, WrittenOffsetColumns & offset_columns, IDataType::SerializeBinaryBulkSettings & settings,
    const CompressionCodecPtr & codec, size_t estimated_size, bool skip_offsets, bool filling_mark)
{
    return [&, skip_offsets] (const IDataType::SubstreamPath & substream_path) -> WriteBuffer *
    {
        if (skip_offsets && !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes)
            return nullptr;

        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (Nested::offsetSubstream(name, stream_name) && offset_columns.count(stream_name))
            return nullptr;

        return &getOrCreateColumnStream(name, stream_name, settings, codec, estimated_size, filling_mark)->compressed;
    };
}

void fillIndexGranularityImpl(
    const Block & block,
    size_t index_granularity_bytes,
    size_t fixed_index_granularity_rows,
    bool blocks_are_granules,
    size_t index_offset,
    MergeTreeIndexGranularity & index_granularity,
    bool can_use_adaptive_index_granularity)
{
    size_t rows_in_block = block.rows();
    size_t index_granularity_for_block;
    if (!can_use_adaptive_index_granularity)
        index_granularity_for_block = fixed_index_granularity_rows;
    else
    {
        size_t block_size_in_memory = block.bytes();
        if (blocks_are_granules)
            index_granularity_for_block = rows_in_block;
        else if (block_size_in_memory >= index_granularity_bytes)
        {
            size_t granules_in_block = block_size_in_memory / index_granularity_bytes;
            index_granularity_for_block = rows_in_block / granules_in_block;
        }
        else
        {
            size_t size_of_row_in_bytes = block_size_in_memory / rows_in_block;
            index_granularity_for_block = index_granularity_bytes / size_of_row_in_bytes;
        }
    }
    if (index_granularity_for_block == 0) /// very rare case when index granularity bytes less then single row
        index_granularity_for_block = 1;

    /// We should be less or equal than fixed index granularity
    index_granularity_for_block = std::min(fixed_index_granularity_rows, index_granularity_for_block);

    for (size_t current_row = index_offset; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);
}

void IMergedBlockOutputStream::fillIndexGranularity(const Block & block)
{
    fillIndexGranularityImpl(
        block,
        storage.settings.index_granularity_bytes,
        storage.settings.index_granularity,
        blocks_are_granules_size,
        index_offset,
        index_granularity,
        storage.canUseAdaptiveGranularity());
}

void IMergedBlockOutputStream::writeSingleMark(const String & name, WrittenOffsetColumns & wrote_nested_offset, size_t number_of_rows)
{
    /// Here are some hacks: for the first mark,
    /// Since no stream was created, we deferred marking writing to IMergedBlockOutputStream::fillMissingColumnStream
    if (!columns_streams.count(name))
        return;

    for (const auto & column_stream : columns_streams.at(name))
    {
        const String & substream_name = column_stream.first;

        /// Don't write offsets more than one time for Nested type.
        if (!Nested::offsetSubstream(name, substream_name) || !wrote_nested_offset.count(substream_name))
        {
            /// There could already be enough data to compress into the new block.
            if (column_stream.second->compressed.offset() >= min_compress_block_size)
                column_stream.second->compressed.next();

            writeIntBinary(column_stream.second->plain_hashing.count(), column_stream.second->marks);
            writeIntBinary(column_stream.second->compressed.offset(), column_stream.second->marks);
            if (storage.canUseAdaptiveGranularity())
                writeIntBinary(number_of_rows, column_stream.second->marks);
        }
    }
}

size_t IMergedBlockOutputStream::writeSingleGranule(
    const String & name, const IDataType & type, const IColumn & column, WrittenOffsetColumns & offset_columns,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state, IDataType::SerializeBinaryBulkSettings & serialize_settings,
    size_t from_row, size_t number_of_rows, bool write_marks)
{
    if (write_marks)
        writeSingleMark(name, offset_columns, number_of_rows);

    type.serializeBinaryBulkWithMultipleStreams(column, from_row, number_of_rows, serialize_settings, serialization_state);

    if (!columns_streams.count(name))
        throw Exception("Cannot write data to column " + name + " because it does not exist stream", ErrorCodes::LOGICAL_ERROR);

    /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
    for (const auto & column_stream : columns_streams.at(name))
    {
        const String & substream_name = column_stream.first;

        /// Don't write offsets more than one time for Nested type.
        if (!Nested::offsetSubstream(name, substream_name) || !offset_columns.count(substream_name))
            column_stream.second->compressed.nextIfAtEnd();
    }

    return from_row + number_of_rows;
}

/// column must not be empty. (column.size() !== 0)

std::pair<size_t, size_t> IMergedBlockOutputStream::writeColumn(
    const String & name,
    const IDataType & type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    bool skip_offsets,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    size_t from_mark,
    const CompressionCodecPtr & codec, size_t estimated_size)
{
    auto & settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = createStreamGetter(name, offset_columns, serialize_settings, codec, estimated_size, skip_offsets);
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part != 0;

    size_t total_rows = column.size();
    size_t current_row = 0;
    size_t current_column_mark = from_mark;
    while (current_row < total_rows)
    {
        size_t rows_to_write;
        bool write_marks = true;

        /// If there is `index_offset`, then the first mark goes not immediately, but after this number of rows.
        if (current_row == 0 && index_offset != 0)
        {
            write_marks = false;
            rows_to_write = index_offset;
            serialize_settings.filling_mark_size = current_column_mark - 1;
        }
        else
        {
            if (index_granularity.getMarksCount() <= current_column_mark)
                throw Exception(
                    "Incorrect size of index granularity expect mark " + toString(current_column_mark) + " totally have marks " + toString(index_granularity.getMarksCount()),
                    ErrorCodes::LOGICAL_ERROR);

            serialize_settings.filling_mark_size = current_column_mark;
            rows_to_write = index_granularity.getMarkRows(current_column_mark);
        }

        current_row = writeSingleGranule(
            name, type, column, offset_columns, serialization_state, serialize_settings, current_row, rows_to_write, write_marks);

        if (write_marks)
            current_column_mark++;
    }

    if (!columns_streams.count(name))
        throw Exception("Cannot write data to column " + name + " because it does not exist stream", ErrorCodes::ILLEGAL_COLUMN);

    /// Memoize offsets for Nested types, that are already written. They will not be written again for next columns of Nested structure.
    for (const auto & column_stream : columns_streams.at(name))
    {
        if (Nested::offsetSubstream(name, column_stream.first))
            offset_columns.insert(column_stream.first);
    }

    return std::make_pair(current_column_mark, current_row - total_rows);
}

void IMergedBlockOutputStream::writeFinalMark(const std::string & column_name, WrittenOffsetColumns & offset_columns)
{
    if (!columns_streams.count(column_name))
        throw Exception("", ErrorCodes::LOGICAL_ERROR);

    writeSingleMark(column_name, offset_columns, 0);

    /// Memoize information about offsets
    for (const auto & column_stream : columns_streams.at(column_name))
    {
        if (Nested::offsetSubstream(column_name, column_stream.first))
            offset_columns.insert(column_stream.first);
    }
}

IMergedBlockOutputStream::ColumnStream * IMergedBlockOutputStream::getOrCreateColumnStream(
    const String & column_name, const String & stream_name, IDataType::SerializeBinaryBulkSettings & settings,
    const CompressionCodecPtr & codec, size_t estimated_size, bool filling_mark)
{
    if (!columns_streams.count(column_name))
        columns_streams[column_name] = ColumnStreams();

    ColumnStreams & column_streams = columns_streams[column_name];
    ColumnStreams::iterator column_stream_it = column_streams.find(stream_name);

    if (column_stream_it != column_streams.end())
        return &*column_stream_it->second;

    column_streams[stream_name] = std::make_unique<ColumnStream>(
        stream_name, part_path + stream_name, DATA_FILE_EXTENSION,
        part_path + stream_name, marks_file_extension,
        codec, max_compress_block_size, estimated_size, aio_threshold);

    return filling_mark ? fillMissingColumnStream(settings, &*column_streams[stream_name]) : &*column_streams[stream_name];
}

IMergedBlockOutputStream::ColumnStream * IMergedBlockOutputStream::fillMissingColumnStream(
    IDataType::SerializeBinaryBulkSettings & settings, ColumnStream * column_stream)
{
    for (size_t index = 0, size = settings.filling_mark_size; index <= size; ++index)
    {
        writeIntBinary(column_stream->plain_hashing.count(), column_stream->marks);
        writeIntBinary(column_stream->compressed.offset(), column_stream->marks);
        if (storage.canUseAdaptiveGranularity())
            writeIntBinary(index_granularity.getMarkRows(index), column_stream->marks);
    }

    return column_stream;
}


/// Implementation of IMergedBlockOutputStream::ColumnStream.

IMergedBlockOutputStream::ColumnStream::ColumnStream(
    const String & escaped_column_name_,
    const String & data_path,
    const std::string & data_file_extension_,
    const std::string & marks_path,
    const std::string & marks_file_extension_,
    const CompressionCodecPtr & compression_codec,
    size_t max_compress_block_size,
    size_t estimated_size,
    size_t aio_threshold) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(createWriteBufferFromFileBase(data_path + data_file_extension, estimated_size, aio_threshold, max_compress_block_size)),
    plain_hashing(*plain_file), compressed_buf(plain_hashing, compression_codec), compressed(compressed_buf),
    marks_file(marks_path + marks_file_extension, 4096, O_TRUNC | O_CREAT | O_WRONLY), marks(marks_file)
{
}

void IMergedBlockOutputStream::ColumnStream::finalize()
{
    compressed.next();
    plain_file->next();
    marks.next();
}

void IMergedBlockOutputStream::ColumnStream::sync()
{
    plain_file->sync();
    marks_file.sync();
}

void IMergedBlockOutputStream::ColumnStream::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + data_file_extension].is_compressed = true;
    checksums.files[name + data_file_extension].uncompressed_size = compressed.count();
    checksums.files[name + data_file_extension].uncompressed_hash = compressed.getHash();
    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

    checksums.files[name + marks_file_extension].file_size = marks.count();
    checksums.files[name + marks_file_extension].file_hash = marks.getHash();
}

}
