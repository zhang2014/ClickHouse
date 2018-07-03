#include <Compression/CompressionCodecZSTD.h>
#include <Compression/CompressionCodecFactory.h>

#include <zstd.h>

#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <common/unaligned.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int CANNOT_DECOMPRESS;
extern const int CANNOT_COMPRESS;
}

void ZSTDCompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    size_t uncompressed_size = offset();
    size_t compressed_size = 0;
    char *compressed_buffer_ptr = nullptr;

    static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

    compressed_buffer.resize(header_size + ZSTD_compressBound(uncompressed_size));

    compressed_buffer[0] = static_cast<char>(CompressionMethodByte::ZSTD);

    size_t res = ZSTD_compress(
        &compressed_buffer[header_size],
        compressed_buffer.size() - header_size,
        working_buffer.begin(),
        uncompressed_size,
        compression_settings.level);

    if (ZSTD_isError(res))
        throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_COMPRESS);

    compressed_size = header_size + res;

    UInt32 compressed_size_32 = compressed_size;
    UInt32 uncompressed_size_32 = uncompressed_size;

    unalignedStore(&compressed_buffer[1], compressed_size_32);
    unalignedStore(&compressed_buffer[5], uncompressed_size_32);

    compressed_buffer_ptr = &compressed_buffer[0];

    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer_ptr, compressed_size);
    out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

    out.write(compressed_buffer_ptr, compressed_size);
}

String CompressionCodecZSTD::getName()
{
    return "Codec(ZSTD)";
}

WriteBuffer * CompressionCodecZSTD::writeImpl(WriteBuffer &upstream)
{
    return new ZSTDCompressedWriteBuffer(upstream);
}

}