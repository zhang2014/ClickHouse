#include <Compression/CompressionCodecLZ4.h>
#include <lz4.h>
#include <lz4hc.h>
#include <IO/CompressedStream.h>
#include <Compression/CompressionFactory.h>
#include "CompressionCodecLZ4.h"


namespace DB
{

char CompressionCodecLZ4::getMethodByte()
{
    return static_cast<char>(CompressionMethodByte::LZ4);
}

void CompressionCodecLZ4::getCodecDesc(String & codec_desc)
{
    codec_desc = "LZ4";
}

void CompressionCodecLZ4::compress(char * uncompressed_buf, size_t uncompressed_size, PODArray<char> & compressed_buf, size_t & compressed_size)
{
    static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    compressed_buf.resize(header_size + LZ4_COMPRESSBOUND(uncompressed_size));
#pragma GCC diagnostic pop

    compressed_size = header_size + LZ4_compress_default(
        uncompressed_buf,
        &compressed_buf[header_size],
        uncompressed_size,
        LZ4_COMPRESSBOUND(uncompressed_size));

    UInt32 compressed_size_32 = compressed_size;
    UInt32 uncompressed_size_32 = uncompressed_size;
    unalignedStore(&compressed_buf[1], compressed_size_32);
    unalignedStore(&compressed_buf[5], uncompressed_size_32);
}

void registerCodecLZ4(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("LZ4", static_cast<char>(CompressionMethodByte::LZ4), [&](){
        return std::make_shared<CompressionCodecLZ4>();
    });
}

}
