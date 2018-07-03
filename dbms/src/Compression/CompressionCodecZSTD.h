#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionSettings.h>

namespace DB {

class ZSTDCompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    ZSTDCompressedWriteBuffer(WriteBuffer &out, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE) : BufferWithOwnMemory(buf_size), out(out)
    {}

private:
    WriteBuffer &out;
    CompressionSettings compression_settings;

    PODArray<char> compressed_buffer;

    void nextImpl() override;
};


class CompressionCodecZSTD final : public ICompressionCodec
{
public:

    virtual String getName() override;

    WriteBuffer * writeImpl(WriteBuffer &upstream) override;
};

}