#include <Compression/CompressionCodecDelta.h>
#include <Parsers/IAST.h>
#include <IO/CompressedStream.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeFactory.h>

#if __SSE4_2__
#include <nmmintrin.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
}

void registerCodecDelta(CompressionCodecFactory & factory)
{
    factory.registerCompressionCodec("Delta", static_cast<char>(CompressionMethodByte::Delta), [&](const ASTPtr & arguments)
    {
        if (!arguments || arguments->children.size() != 1)
            throw Exception("ZSTD codec must have 1 parameter, given " + std::to_string(arguments->children.size()), ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

        DataTypePtr delta_type = DataTypeFactory::instance().get(arguments->children[0]);

        if (!delta_type->isValueRepresentedByNumber())
            throw Exception("The Delta codec is only support number data type.", ErrorCodes::ILLEGAL_CODEC_PARAMETER);

        return std::make_shared<CompressionCodecDelta>(delta_type->getSizeOfValueInMemory());
    });
}

UInt8 CompressionCodecDelta::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Delta);
}

String CompressionCodecDelta::getCodecDesc() const
{
    return "Delta";
}

UInt32 CompressionCodecDelta::doCompressData(const char * /*source*/, UInt32 /*source_size*/, char * /*dest*/) const
{
    return 0;
}

void CompressionCodecDelta::doDecompressData(const char * /*source*/, UInt32 /*source_size*/, char * /*dest*/, UInt32 /*uncompressed_size*/) const
{

}

}
