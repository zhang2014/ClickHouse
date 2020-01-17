#pragma once

#include <IO/WriteBuffer.h>
#include <DataStreams/IBlockOutputStream.h>

namespace DB
{

class WALBlockOutputStream : public IBlockOutputStream
{
public:
    WALBlockOutputStream(WriteBuffer & ostr_, const Block & header_);

    void writePrefix() override;

    void write(const Block & block) override;

    void writeSuffix() override;

    Block getHeader() const override;

private:
    WriteBuffer & ostr;
    Block header;

    IDataType::SerializeBinaryBulkSettings settings;
    std::vector<IDataType::SerializeBinaryBulkStatePtr> columns_state;
};


}
