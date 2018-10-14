#pragma once

#include <memory>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/PODArray.h>
#include <DataTypes/IDataType.h>
#include <boost/noncopyable.hpp>
#include <IO/UncompressedCache.h>

namespace DB
{

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CompressionCodecPtr>;

class CompressionCodecReadBuffer;
class CompressionCodecWriteBuffer;

using CompressionCodecReadBufferPtr = std::shared_ptr<CompressionCodecReadBuffer>;
using CompressionCodecWriteBufferPtr = std::shared_ptr<CompressionCodecWriteBuffer>;

class CompressionCodecWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    CompressionCodecWriteBuffer(ICompressionCodec & compression_codec, WriteBuffer & out, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~CompressionCodecWriteBuffer() override;

private:
    void nextImpl() override;


private:
    WriteBuffer & out;
    ICompressionCodec & compression_codec;
    PODArray<char> compressed_buffer;
};

class CompressionCodecReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    UInt8 method;
    size_t size_compressed = 0;
    size_t size_decompressed = 0;
    bool loaded_compressed = false;

    CompressionCodecReadBuffer(ICompressionCodec & codec, ReadBuffer & origin);

    void loadCompressedData();

    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);

private:
    ICompressionCodec & codec;
    ReadBuffer & origin;
    char * compressed_buffer;
    PODArray<char> own_compressed_buffer;

    bool nextImpl() override;
};

/**
*
*/
class ICompressionCodec : private boost::noncopyable
{
public:
    virtual ~ICompressionCodec() = default;

    CompressionCodecReadBufferPtr liftCompressed(ReadBuffer & origin);

    CompressionCodecWriteBufferPtr liftCompressed(WriteBuffer & origin);

    virtual char getMethodByte() = 0;

    virtual void getCodecDesc(String & codec_desc) = 0;

    virtual size_t compress(char * source, size_t source_size, char * dest) = 0;

    virtual size_t decompress(char * source, size_t source_size, char * dest, size_t decompressed_size) = 0;

    virtual size_t getCompressedReserveSize(size_t uncompressed_size) { return uncompressed_size; }
};

}
