#include <Compression/CompressionCodecFactory.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>

#include <IO/ReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int UNKNOWN_CODEC;
}

CompressionCodecPtr CompressionCodecFactory::get(const ASTPtr & ast, CompressionCodecPtr & children) const
{
    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(ast.get()))
    {
        if (func->parameters)
            throw Exception("Compression codec cannot have multiple parenthesed parameters.", ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

        if (Poco::toLower(func->name) != "codec")
            return get(func->name, func->arguments);

        for (const auto & codec_ast : func->arguments->children)
            children = get(codec_ast, children);

        return children;
    }

    throw Exception("Unexpected AST element for compression codec.", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}

CompressionCodecPtr CompressionCodecFactory::get(const String & family_name, const ASTPtr & arguments, CompressionCodecPtr & children) const
{
    CompressionCodecsCreator::const_iterator it = creators.find(family_name);
    if (creators.end() != it)
        return it->second(arguments, children);

    throw Exception("Unknown data type family: " + family_name, ErrorCodes::UNKNOWN_CODEC);
}

void CompressionCodecFactory::registerCompressedCodec(const CompressionMethod &kind, const CompressionCodecFactory::Creator & creator)
{
    creators.emplace(kind, creator);
}

void registerCodecLZ4(CompressionCodecFactory & factory);
void registerCodecNone(CompressionCodecFactory & factory);
void registerCodecZSTD(CompressionCodecFactory & factory);
//void registerCodecDelta(CompressionCodecFactory & factory);

//CompressionCodecFactory::CompressionCodecFactory()
//{
//    registerCodecLZ4(*this);
//    registerCodecNone(*this);
//    registerCodecZSTD(*this);
//    registerCodecDelta(*this);
//}

}
