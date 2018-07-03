#pragma once

#include <memory>
#include <functional>
#include <unordered_map>
#include <Compression/ICompressionCodec.h>
#include <ext/singleton.h>
#include <IO/CompressedStream.h>


namespace DB
{

class ICompressionCodec;
using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/** Creates a codec object by name of compression algorithm family and parameters, also creates codecs pipe.
  */
class CompressionCodecFactory final : public ext::singleton<CompressionCodecFactory>
{
private:
    using Creator = std::function<CompressionCodecPtr(const ASTPtr & arguments, const CompressionCodecPtr & children)>;
    using CompressionCodecsCreator = std::unordered_map<String, Creator>;

public:
    CompressionCodecPtr get(const ASTPtr & ast, CompressionCodecPtr & children = {}) const;
    CompressionCodecPtr get(const String & family_name, const ASTPtr & arguments, CompressionCodecPtr & children = {}) const;

    void registerCompressedCodec(const CompressionMethod & kind, const Creator & creator);
private:
    CompressionCodecsCreator creators;
};

}
