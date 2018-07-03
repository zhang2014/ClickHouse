#include <Compression/ICompressionCodec.h>


namespace DB
{

ReadBuffer * ICompressionCodec::makeReadBuffer(ReadBuffer & upstream)
{
    /// XXX: hack
    /// TODO:读取所有的codec
    /// TPDO:读取upstream中的值选择codec
    return nullptr;
}

WriteBuffer * ICompressionCodec::makeWriteBuffer(WriteBuffer & upstream)
{
    return writeImpl(upstream);
}

ICompressionCodec::~ICompressionCodec() = default;

}
