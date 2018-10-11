#pragma once

#include <IO/WriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <Parsers/StringRange.h>

namespace DB
{

class CompressionCodecZSTD : public ICompressionCodec
{
public:
    CompressionCodecZSTD(int level);

    char getMethodByte() override;

    void getCodecDesc(String & codec_desc) override;

    void compress(char * uncompressed_buf, size_t uncompressed_size, PODArray<char> & compressed_buf, size_t & compressed_size) override;

private:
    int level;
};

}