#include <iostream>

#include <boost/program_options.hpp>

#include <Common/Exception.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Compression/CompressionPipeline.h>

#include <Poco/String.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int TOO_LARGE_SIZE_COMPRESSED;
    }
}


namespace
{

/// Outputs sizes of uncompressed and compressed blocks for compressed file.
void checkAndWriteHeader(DB::ReadBuffer & in, DB::WriteBuffer & out)
{
    while (!in.eof())
    {
        in.ignore(16);    /// checksum

        auto pipe = DB::CompressionPipeline::get_pipe(&in);
        UInt32 size_compressed = pipe->getCompressedSize();

        if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
            throw DB::Exception("Too large size_compressed. Most likely corrupted data.", DB::ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

        auto data_sizes = pipe->getDataSizes();
        for (size_t i = 0; i < data_sizes.size(); ++i)
        {
            if (i) DB::writeChar('\t', out);
            if (i != data_sizes.size() - 1)
                DB::writeText(data_sizes[i], out);
            else
                DB::writeText(data_sizes[i] + pipe->getHeaderSize(), out);

        }
        DB::writeChar('\n', out);

        in.ignore(size_compressed);
    }
}

}


int mainEntryClickHouseCompressor(int argc, char ** argv)
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("decompress,d", "decompress")
        ("block-size,b", boost::program_options::value<unsigned>()->default_value(DBMS_DEFAULT_BUFFER_SIZE), "compress in blocks of specified size")
        ("hc", "use LZ4HC instead of LZ4")
        ("zstd", "use ZSTD instead of LZ4")
        ("level", "compression level")
        ("none", "use no compression instead of LZ4")
        ("custom", boost::program_options::value<DB::String>(), "custom compression pipeline")
        ("stat", "print block statistics of compressed data")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " [options] < in > out" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    try
    {
        bool decompress = options.count("decompress");
        bool use_lz4hc = options.count("hc");
        bool use_zstd = options.count("zstd");
        bool stat_mode = options.count("stat");
        bool use_none = options.count("none");
        bool use_custom = options.count("custom");
        unsigned block_size = options["block-size"].as<unsigned>();

        DB::CompressionMethod method = DB::CompressionMethod::LZ4;

        DB::PipePtr compression_pipe;
        if (use_lz4hc)
            method = DB::CompressionMethod::LZ4HC;
        else if (use_zstd)
            method = DB::CompressionMethod::ZSTD;
        else if (use_none)
            method = DB::CompressionMethod::NONE;
        else if (use_custom)
            compression_pipe = DB::CompressionPipeline::get_pipe(options["custom"].as<DB::String>());

        DB::CompressionSettings settings(method, options.count("level") > 0 ? options["level"].as<int>() : DB::CompressionSettings::getDefaultLevel(method),
                                         compression_pipe);

        DB::ReadBufferFromFileDescriptor rb(STDIN_FILENO);
        DB::WriteBufferFromFileDescriptor wb(STDOUT_FILENO);

        if (stat_mode)
        {
            /// Output statistic for compressed file.
            checkAndWriteHeader(rb, wb);
        }
        else if (decompress)
        {
            /// Decompression
            DB::CompressedReadBuffer from(rb);
            DB::copyData(from, wb);
        }
        else
        {
            /// Compression
            DB::CompressedWriteBuffer to(wb, settings, block_size);
            DB::copyData(rb, to);
        }
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true);
        return DB::getCurrentExceptionCode();
    }

    return 0;
}
