#include <Compression/CompressionCodecMultiple.h>
#include <IO/CompressedStream.h>
#include <common/unaligned.h>
#include "CompressionCodecMultiple.h"


namespace DB
{

CompressionCodecMultiple::CompressionCodecMultiple(Codecs codecs)
    : codecs(codecs)
{
    /// TODO initialize inner_methods_code
    for (size_t idx = 0; idx < codecs.size(); idx++)
    {
        if (idx != 0)
            codec_desc = codec_desc + ',';

        const auto codec = codecs[idx];
        String inner_codec_desc;
        codec->getCodecDesc(inner_codec_desc);
        codec_desc = codec_desc + inner_codec_desc;
    }
}

char CompressionCodecMultiple::getMethodByte()
{
    return static_cast<char>(CompressionMethodByte::Multiple);
}

void CompressionCodecMultiple::getCodecDesc(String & codec_desc_)
{
    codec_desc_ = codec_desc;
}

size_t CompressionCodecMultiple::getCompressedReserveSize(size_t uncompressed_size)
{
    for (auto & codec : codecs)
    {
        uncompressed_size = codec->getCompressedReserveSize(uncompressed_size);
    }
    return uncompressed_size;
}

size_t CompressionCodecMultiple::compress(char * source, size_t source_size, char * dest)
{
    static constexpr size_t header_size = sizeof(UInt32) + sizeof(UInt32);
    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf(source_size);
    uncompressed_buf.insert(source, source + source_size);

    for (auto & codec : codecs)
    {
        compressed_buf.resize(header_size + codec->getCompressedReserveSize(source_size));
        size_t compressed_size = header_size + codec->compress(&uncompressed_buf[0], source_size, &compressed_buf[header_size]);
        UInt32 compressed_size_32 = compressed_size;
        UInt32 uncompressed_size_32 = source_size;

        unalignedStore(&compressed_buf[0], compressed_size_32);
        unalignedStore(&compressed_buf[4], uncompressed_size_32);
        uncompressed_buf.swap(compressed_buf);
        source_size = compressed_size;
    }

    memcpy(dest, &compressed_buf[0], source_size);
    return source_size;
}

size_t CompressionCodecMultiple::decompress(char * /*source*/, size_t /*source_size*/, char * /*dest*/, size_t /*decompressed_size*/)
{
    return 0;
}

}