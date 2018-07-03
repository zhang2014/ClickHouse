#pragma once

#include <memory>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/PODArray.h>
#include <DataTypes/IDataType.h>
#include <boost/noncopyable.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CompressionCodecPtr>;


/**
 *
 */
class ICompressionCodec : private boost::noncopyable {

public:

    ///
    ReadBuffer * makeReadBuffer(ReadBuffer & upstream);

    WriteBuffer * makeWriteBuffer(WriteBuffer & upstream);

    ~ICompressionCodec();

    virtual String getName() = 0;

    virtual ReadBuffer * readImpl(ReadBuffer & upstream) = 0;

    virtual WriteBuffer * writeImpl(WriteBuffer & upstream) = 0;
};

}