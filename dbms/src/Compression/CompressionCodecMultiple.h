#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecMultiple final : public ICompressionCodec
{
public:
    CompressionCodecMultiple(Codecs codecs);

    char getMethodByte() override;

    void getCodecDesc(String & codec_desc) override;

    void compress(char * uncompressed_buf, size_t uncompressed_size, PODArray<char> & compressed_buf, size_t & compressed_size) override;

private:
    Codecs codecs;
    String codec_desc;

};

}
