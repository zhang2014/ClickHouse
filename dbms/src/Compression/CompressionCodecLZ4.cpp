#include <Compression/CompressionCodecLZ4.h>
#include <common/unaligned.h>
#include <lz4.h>
#include <lz4hc.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressionCodecFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CANNOT_DECOMPRESS;
    extern const int CANNOT_COMPRESS;
}

void LZ4CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    size_t uncompressed_size = offset();
    size_t compressed_size = 0;
    char * compressed_buffer_ptr = nullptr;

    {
        static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
        compressed_buffer.resize(header_size + LZ4_COMPRESSBOUND(uncompressed_size));
#pragma GCC diagnostic pop

        compressed_buffer[0] = static_cast<char>(CompressionMethodByte::LZ4);

        if (compression_settings.method == CompressionMethod::LZ4)
            compressed_size = header_size + LZ4_compress_default(
                working_buffer.begin(),
                &compressed_buffer[header_size],
                uncompressed_size,
                LZ4_COMPRESSBOUND(uncompressed_size));
        else
            compressed_size = header_size + LZ4_compress_HC(
                working_buffer.begin(),
                &compressed_buffer[header_size],
                uncompressed_size,
                LZ4_COMPRESSBOUND(uncompressed_size),
                compression_settings.level);

        UInt32 compressed_size_32 = compressed_size;
        UInt32 uncompressed_size_32 = uncompressed_size;

        unalignedStore(&compressed_buffer[1], compressed_size_32);
        unalignedStore(&compressed_buffer[5], uncompressed_size_32);

        compressed_buffer_ptr = &compressed_buffer[0];
    }
    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer_ptr, compressed_size);
    out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

    out.write(compressed_buffer_ptr, compressed_size);
}

String CompressionCodecLZ4::getName()
{
    return "Codec(LZ4)";
}

WriteBuffer * CompressionCodecLZ4::writeImpl(WriteBuffer &upstream)
{
    return new LZ4CompressedWriteBuffer(upstream);
}

}
