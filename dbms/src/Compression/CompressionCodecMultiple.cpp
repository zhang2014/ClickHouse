#include <Compression/CompressionCodecMultiple.h>
#include <IO/CompressedStream.h>
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

void CompressionCodecMultiple::compress(char * uncompressed_buf, size_t uncompressed_size, PODArray<char> & compressed_buf, size_t & compressed_size)
{
    char reserve = 1;

    PODArray<char> un_compressed_buf(uncompressed_size);
    un_compressed_buf.emplace_back(reserve);
    un_compressed_buf.insert(uncompressed_buf, uncompressed_buf + uncompressed_size);

    for (auto & codec : codecs)
    {
        codec->compress(&un_compressed_buf[1], uncompressed_size, compressed_buf, compressed_size);
        uncompressed_size = compressed_size;
        compressed_buf.swap(un_compressed_buf);
    }
}
}