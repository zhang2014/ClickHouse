#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>

namespace DB
{

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    MergeTreeData & storage_, const Block & header_, String part_path_, bool sync_,
    CompressionCodecPtr default_codec_, bool skip_offsets_,
    WrittenOffsetColumns & already_written_offset_columns,
    const MergeTreeIndexGranularity & index_granularity_)
    : IMergedBlockOutputStream(
        storage_, part_path_, storage_.global_context.getSettings().min_compress_block_size,
        storage_.global_context.getSettings().max_compress_block_size, default_codec_,
        storage_.global_context.getSettings().min_bytes_to_use_direct_io,
        false,
        index_granularity_),
    header(header_), sync(sync_), skip_offsets(skip_offsets_),
    already_written_offset_columns(already_written_offset_columns)
{
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    if (!initialized)
    {
        columns_streams.clear();
        serialization_states.clear();
        serialization_states.reserve(block.columns());
        WrittenOffsetColumns tmp_offset_columns;
        IDataType::SerializeBinaryBulkSettings settings;

        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & col = block.safeGetByPosition(i);

            const auto columns = storage.getColumns();
            const auto & effective_codec = columns.getCodecOrDefault(col.name, codec);
            serialization_states.emplace_back(nullptr);
            settings.getter = createStreamGetter(col.name, tmp_offset_columns, effective_codec, 0, skip_offsets);
            col.type->serializeBinaryBulkStatePrefix(settings, serialization_states.back());
        }

        initialized = true;
    }

    size_t rows = block.rows();
    if (!rows)
        return;

    size_t new_index_offset = 0;
    size_t new_current_mark = 0;
    WrittenOffsetColumns offset_columns = already_written_offset_columns;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto columns = storage.getColumns();
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        const auto & effective_codec = columns.getCodecOrDefault(column.name, codec);
        std::tie(new_current_mark, new_index_offset) = writeColumn(column.name, *column.type, *column.column, offset_columns, skip_offsets, serialization_states[i], current_mark, effective_codec, 0);
    }

    index_offset = new_index_offset;
    current_mark = new_current_mark;
}

void MergedColumnOnlyOutputStream::writeSuffix()
{
    throw Exception("Method writeSuffix is not supported by MergedColumnOnlyOutputStream", ErrorCodes::NOT_IMPLEMENTED);
}

MergeTreeData::DataPart::Checksums MergedColumnOnlyOutputStream::writeSuffixAndGetChecksums()
{
    /// Finish columns serialization.
    auto & settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part != 0;

    WrittenOffsetColumns offset_columns;
    for (size_t i = 0, size = header.columns(); i < size; ++i)
    {
        auto & column = header.getByPosition(i);
        const auto & columns = storage.getColumns();
        const auto & effective_codec = columns.getCodecOrDefault(column.name, codec);
        serialize_settings.getter = createStreamGetter(column.name, already_written_offset_columns, effective_codec, 0, skip_offsets);
        column.type->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[i]);


        if (with_final_mark)
            writeFinalMark(column.name, offset_columns);
    }

    MergeTreeData::DataPart::Checksums checksums;

    for (const auto & column_streams : columns_streams)
    {
        for (const auto & column_stream : column_streams.second)
        {
            column_stream.second->finalize();
            if (sync)
                column_stream.second->sync();

            column_stream.second->addToChecksums(checksums);
        }
    }

    columns_streams.clear();
    serialization_states.clear();
    initialized = false;

    return checksums;
}

}
