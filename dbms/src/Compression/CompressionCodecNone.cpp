#include <Compression/CompressionCodecNone.h>
#include <IO/CompressedStream.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

char CompressionCodecNone::getMethodByte()
{
    return static_cast<char>(CompressionMethodByte::NONE);
}

void CompressionCodecNone::getCodecDesc(String & codec_desc)
{
    codec_desc = "NONE";
}

void CompressionCodecNone::compress(char * uncompressed_buf, size_t uncompressed_size, PODArray<char> & compressed_buf, size_t & compressed_size)
{
    static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

    compressed_size = header_size + uncompressed_size;
    UInt32 uncompressed_size_32 = uncompressed_size;
    UInt32 compressed_size_32 = compressed_size;

    compressed_buf.resize(compressed_size);

    unalignedStore(&compressed_buf[1], compressed_size_32);
    unalignedStore(&compressed_buf[5], uncompressed_size_32);
    memcpy(&compressed_buf[header_size], uncompressed_buf, uncompressed_size);
}

void registerCodecNone(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("NONE", static_cast<char>(CompressionMethodByte::NONE), [&](){
        return std::make_shared<CompressionCodecNone>();
    });
}

}