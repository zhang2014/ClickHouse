#include <Compression/ICompressionCodec.h>
#include <IO/LZ4_decompress_faster.h>
#include "ICompressionCodec.h"


namespace DB
{

ReadBufferPtr ICompressionCodec::liftCompressed(ReadBuffer & origin)
{
    return std::make_shared<LazyLiftCompressedReadBuffer>(*this, origin);
}

WriteBufferPtr ICompressionCodec::liftCompressed(WriteBuffer & origin)
{
    return std::make_shared<LiftedCompressedWriteBuffer>(*this, origin);
}

LazyLiftCompressedReadBuffer::LazyLiftCompressedReadBuffer(ICompressionCodec & /*codec*/, ReadBuffer & /*origin*/)
{
}

bool LazyLiftCompressedReadBuffer::nextImpl()
{
//    size_t size_decompressed;
//    size_t size_compressed_without_checksum;
//    size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum);
//    if (!size_compressed)
//        return false;
//
//    memory.resize(size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
//    working_buffer = Buffer(memory.data(), &memory[size_decompressed]);
//
//    decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);
//
    return true;
}

LiftedCompressedWriteBuffer::~LiftedCompressedWriteBuffer()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void LiftedCompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    size_t compressed_size = 0;
    size_t uncompressed_size = offset();
    compressed_buffer.emplace_back(compression_codec.getMethodByte());
    compression_codec.compress(working_buffer.begin(), uncompressed_size, compressed_buffer, compressed_size);
    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer.data(), compressed_size);

    out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));
    out.write(compressed_buffer.data(), compressed_size);
}

LiftedCompressedWriteBuffer::LiftedCompressedWriteBuffer(ICompressionCodec & compression_codec, WriteBuffer & out, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size), out(out), compression_codec(compression_codec)
{
}

}
