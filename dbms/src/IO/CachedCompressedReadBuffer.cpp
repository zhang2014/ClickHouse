#include <IO/createReadBufferFromFileBase.h>
#include <IO/CachedCompressedReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/CompressedStream.h>
#include <IO/LZ4_decompress_faster.h>
#include "CachedCompressedReadBuffer.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


void CachedCompressedReadBuffer::initInput()
{
    if (!file_in)
    {
        file_in = createReadBufferFromFileBase(path, estimated_size, aio_threshold, buf_size);
        compressed_in = &*file_in;

        if (profile_callback)
            file_in->setProfileCallback(profile_callback, clock_type);
    }
}


bool CachedCompressedReadBuffer::nextImpl()
{
    /// Let's check for the presence of a decompressed block in the cache, grab the ownership of this block, if it exists.

    UInt128 key = cache->hash(path, file_pos);
    owned_cell = cache->get(key);

    if (!owned_cell)
    {
//        /// If not, read it from the file.
//        initInput();
//        file_in->seek(file_pos);

        owned_cell = std::make_shared<UncompressedCacheCell>();

//        size_t size_decompressed;
//        size_t size_compressed_without_checksum;
//        owned_cell->compressed_size = readCompressedData(size_decompressed, size_compressed_without_checksum);

        in->loadCompressedData();
        if (in->size_compressed)
        {
            owned_cell->data.resize(in->size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
            in->buffer() = Buffer(owned_cell->data.data(), owned_cell->data.data() + owned_cell->data.size() - LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
            in->next();

            /// Put data into cache.
            cache->set(key, owned_cell);
        }
    }

    if (owned_cell->data.size() == 0)
    {
        owned_cell = nullptr;
        return false;
    }

    working_buffer = in->buffer();

//    file_pos += owned_cell->compressed_size;

    return true;
}


CachedCompressedReadBuffer::CachedCompressedReadBuffer(
    const std::string & path_, UncompressedCache * cache_, size_t estimated_size_, size_t aio_threshold_,
    size_t buf_size_)
    : ReadBuffer(nullptr, 0), path(path_), cache(cache_), buf_size(buf_size_), estimated_size(estimated_size_),
        aio_threshold(aio_threshold_), file_pos(0)
{
}


CachedCompressedReadBuffer::CachedCompressedReadBuffer(
    const std::string & path_, UncompressedCache * cache_, CompressionCodecReadBufferPtr & compressed_buffer, size_t buf_size_)
    : ReadBuffer(nullptr, 0), path(path_), cache(cache_), buf_size(buf_size_), in(compressed_buffer), file_pos(0)
{
}

void CachedCompressedReadBuffer::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    if (owned_cell &&
        offset_in_compressed_file == file_pos - owned_cell->compressed_size &&
        offset_in_decompressed_block <= working_buffer.size())
    {
        bytes += offset();
        pos = working_buffer.begin() + offset_in_decompressed_block;
        bytes -= offset();
    }
    else
    {
        bytes += offset();
        file_pos = offset_in_compressed_file;
        in->seek(offset_in_compressed_file, offset_in_decompressed_block);

        nextImpl();

        if (offset_in_decompressed_block > working_buffer.size())
            throw Exception("Seek position is beyond the decompressed block"
                " (pos: " + toString(offset_in_decompressed_block) + ", block size: " + toString(working_buffer.size()) + ")",
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

        pos = working_buffer.begin() + offset_in_decompressed_block;
        bytes -= offset();
    }
}

}
