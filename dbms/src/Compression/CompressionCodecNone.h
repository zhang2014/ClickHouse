#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionSettings.h>

namespace DB {

class NoneCompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    NoneCompressedWriteBuffer(WriteBuffer & out, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE) : BufferWithOwnMemory(buf_size), out(out)
    {}

private:
    WriteBuffer &out;
    CompressionSettings compression_settings;

    PODArray<char> compressed_buffer;

    void nextImpl() override;

};

class CompressionCodecNone final : public ICompressionCodec
{
public:

    virtual String getName() override;

    WriteBuffer * writeImpl(WriteBuffer &upstream) override;
};

}