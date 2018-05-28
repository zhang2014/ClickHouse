#include <Compression/ICompressionCodec.h>

namespace DB {

class CompressionCodecLZ4 : public ICompressionCodec
{
public:
    CompressionCodecLZ4() {}
    CompressionCodecLZ4(uint8_t _argument)
    : argument(_argument)
    {}

    static const uint8_t bytecode = 0x84;
    static const bool is_hc = false;
    uint8_t argument = 1;

    std::string getName() const
    {
        return "LZ4(" + std::to_string(argument) + ")";
    }

    const char * getFamilyName() const override
    {
        return "LZ4";
    }

    size_t writeHeader(char* header) override;
    size_t parseHeader(const char* header);

    size_t getHeaderSize() const { return 1; };

    size_t getCompressedSize() const { return 0; };
    size_t getDecompressedSize() const { return 0; };

    size_t getMaxCompressedSize(size_t uncompressed_size) const override;

    size_t compress(char* source, char* dest, int inputSize, int maxOutputSize) override;
    size_t decompress(char *, char *, int, int) override;

    ~CompressionCodecLZ4() {}
};

class CompressionCodecLZ4HC final : public CompressionCodecLZ4
{
public:
    CompressionCodecLZ4HC() {}
    CompressionCodecLZ4HC(uint8_t _argument)
    : argument(_argument)
    {}

    static const uint8_t bytecode = 0x86;
    static const bool is_hc = true;
    uint8_t argument = 1;

    std::string getName() const
    {
        return "LZ4HC(" + std::to_string(argument) + ")";
    }

    ~CompressionCodecLZ4HC() {}
};

class CompressionCodecLZ4Old final : public CompressionCodecLZ4
{
public:
    CompressionCodecLZ4Old() {};
    static const uint8_t bytecode = 0x82;
    uint8_t argument = 1;

    std::string getName() const
    {
        return "LZ4Old(1)";
    }

    size_t writeHeader(char* header)
    {
        *header = bytecode;
        return 1;
    }

    size_t getHeaderSize() const
    {
        return 0;
    }
    size_t parseHeader(const char*)
    {
        return 0;
    }

    size_t getCompressedSize() const
    {
        return 0;
    }
    size_t getDecompressedSize() const
    {
        return 0;
    }
};
}