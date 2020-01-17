#include <Storages/Buffer/WALBlockOutputStream.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{

WALBlockOutputStream::WALBlockOutputStream(WriteBuffer & ostr_, const Block & header_)
    : ostr(ostr_), header(header_)
{
    settings.getter = [this](IDataType::SubstreamPath) -> WriteBuffer * { return &ostr; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0;

    columns_state.resize(header.columns());
}

Block WALBlockOutputStream::getHeader() const
{
    return header;
}

void WALBlockOutputStream::writePrefix()
{
    writeVarUInt(header.columns(), ostr);
    for (size_t index = 0; index < header.columns(); ++index)
    {
        ColumnWithTypeAndName & column = header.safeGetByPosition(index);
        column.type = recursiveRemoveLowCardinality(column.type);

        writeStringBinary(column.name, ostr);
        writeStringBinary(column.type->getName(), ostr);
        column.type->serializeBinaryBulkStatePrefix(settings, columns_state[index]);
    }
}

void WALBlockOutputStream::writeSuffix()
{
    /// TODO: flush
    for (size_t index = 0; index < header.columns(); ++index)
    {
        ColumnWithTypeAndName column = header.safeGetByPosition(index);
        column.type->serializeBinaryBulkStateSuffix(settings, columns_state[index]);
    }
}

void WALBlockOutputStream::write(const Block & block)
{
    /// TODO: check with header struct and maybe remove check rows.
    block.checkNumberOfRows();

    writeVarUInt(block.rows(), ostr);
    for (size_t index = 0; index < header.columns(); ++index)
    {
        ColumnWithTypeAndName data_column = block.safeGetByPosition(index);
        ColumnWithTypeAndName & header_column = header.safeGetByPosition(index);

        if (block.rows())
        {
            data_column.column = recursiveRemoveLowCardinality(data_column.column);
            ColumnPtr full_column = data_column.column->convertToFullColumnIfConst();
            header_column.type->serializeBinaryBulkWithMultipleStreams(*full_column, 0, 0, settings, columns_state[index]);
        }
    }
}

}

