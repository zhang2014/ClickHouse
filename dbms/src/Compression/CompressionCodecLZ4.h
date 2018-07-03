#include <IO/WriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <IO/CompressionSettings.h>
#include <IO/BufferWithOwnMemory.h>
#include <Parsers/StringRange.h>

namespace DB {

class LZ4CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    LZ4CompressedWriteBuffer(WriteBuffer & out, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : BufferWithOwnMemory<WriteBuffer>(buf_size), out(out)
    {}

private:
    WriteBuffer &out;
    CompressionSettings compression_settings;

    PODArray<char> compressed_buffer;

    void nextImpl() override;
};

class CompressionCodecLZ4 : public ICompressionCodec
{
public:

    String getName() override;

    WriteBuffer * writeImpl(WriteBuffer &upstream) override;
};

//class CompressionCodecLZ4HC final : public CompressionCodecLZ4
//{
//public:
//    uint8_t argument = 1;
//
//    CompressionCodecLZ4HC() {}
//    CompressionCodecLZ4HC(uint8_t _argument)
//    : argument(_argument)
//    {}
//
//    std::string getName() const
//    {
//        return "LZ4HC(" + std::to_string(argument) + ")";
//    }
//    size_t compress(char *source, char *dest, size_t input_size, size_t max_output_size) override;
//
//    ~CompressionCodecLZ4HC() {}
//};

}