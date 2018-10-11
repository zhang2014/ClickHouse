#pragma once

#include <memory>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/PODArray.h>
#include <DataTypes/IDataType.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CompressionCodecPtr>;

class LiftedCompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    LiftedCompressedWriteBuffer(ICompressionCodec & compression_codec, WriteBuffer & out, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~LiftedCompressedWriteBuffer() override;

private:
    void nextImpl() override;


private:
    WriteBuffer & out;
    ICompressionCodec & compression_codec;
    PODArray<char> compressed_buffer;
};

class LazyLiftCompressedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
private:
    bool nextImpl() override;
public:
    LazyLiftCompressedReadBuffer(ICompressionCodec & codec, ReadBuffer & origin);
};

/**
*
*/
class ICompressionCodec : private boost::noncopyable
{
public:
    virtual ~ICompressionCodec() = default;

    ReadBufferPtr liftCompressed(ReadBuffer & origin);

    WriteBufferPtr liftCompressed(WriteBuffer & origin);

    virtual char getMethodByte() = 0;

    virtual void getCodecDesc(String & codec_desc) = 0;

    virtual void compress(char * uncompressed_buf, size_t uncompressed_size, PODArray<char> & compressed_buf, size_t & compressed_size) = 0;
};

}
