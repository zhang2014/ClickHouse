#include <Compression/CompressionCodecNone.h>
#include <Compression/CompressionCodecFactory.h>
#include <common/unaligned.h>

namespace DB
{

void NoneCompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    size_t uncompressed_size = offset();
    size_t compressed_size = 0;
    char * compressed_buffer_ptr = nullptr;

    static constexpr size_t header_size = 1 + sizeof (UInt32) + sizeof (UInt32);

    compressed_size = header_size + uncompressed_size;
    UInt32 uncompressed_size_32 = uncompressed_size;
    UInt32 compressed_size_32 = compressed_size;

    compressed_buffer.resize(compressed_size);

    compressed_buffer[0] = static_cast<char>(CompressionMethodByte::NONE);

    unalignedStore(&compressed_buffer[1], compressed_size_32);
    unalignedStore(&compressed_buffer[5], uncompressed_size_32);
    memcpy(&compressed_buffer[9], working_buffer.begin(), uncompressed_size);

    compressed_buffer_ptr = &compressed_buffer[0];

    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer_ptr, compressed_size);
    out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

    out.write(compressed_buffer_ptr, compressed_size);
}

String CompressionCodecNone::getName()
{
    return "Codec(None)";
}

WriteBuffer * CompressionCodecNone::writeImpl(WriteBuffer &upstream)
{
    return new NoneCompressedWriteBuffer(upstream);
}

}