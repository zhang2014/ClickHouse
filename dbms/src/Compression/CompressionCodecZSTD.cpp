#include <Compression/CompressionCodecZSTD.h>
#include <IO/CompressedStream.h>
#include <Compression/CompressionFactory.h>
#include <zstd.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
}

char CompressionCodecZSTD::getMethodByte()
{
    return static_cast<char>(CompressionMethodByte::ZSTD);
}

void CompressionCodecZSTD::getCodecDesc(String & codec_desc)
{
    codec_desc = "ZSTD";
}

void CompressionCodecZSTD::compress(char * uncompressed_buf, size_t uncompressed_size, PODArray<char> & compressed_buf, size_t & compressed_size)
{
    static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

    compressed_buf.resize(header_size + ZSTD_compressBound(uncompressed_size));

    size_t res = ZSTD_compress(
        &compressed_buf[header_size],
        compressed_buf.size() - header_size,
        uncompressed_buf,
        uncompressed_size,
        level);

    if (ZSTD_isError(res))
        throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_COMPRESS);

    compressed_size = header_size + res;

    UInt32 compressed_size_32 = compressed_size;
    UInt32 uncompressed_size_32 = uncompressed_size;

    unalignedStore(&compressed_buf[1], compressed_size_32);
    unalignedStore(&compressed_buf[5], uncompressed_size_32);
}

CompressionCodecZSTD::CompressionCodecZSTD(int level)
    :level(level)
{
}

void registerCodecZSTD(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<char>(CompressionMethodByte::ZSTD);
    factory.registerCompressionCodec("ZSTD", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = 0;
        if (arguments && !arguments->children.empty())
        {
            const auto children = arguments->children;
            const ASTLiteral * literal = static_cast<const ASTLiteral *>(children[0].get());
            level = literal->value.safeGet<UInt64>();
        }

        return std::make_shared<CompressionCodecZSTD>(level);
    });
}

}
