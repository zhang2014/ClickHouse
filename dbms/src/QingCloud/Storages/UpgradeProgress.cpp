#include <QingCloud/Storages/UpgradeProgress.h>
#include <Poco/File.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include "UpgradeProgress.h"


namespace DB
{


UpgradeProgress::UpgradeProgress(const String &path, const MultiplexedClusterPtr &clusters)
    : dir(path), data_path(dir + "upgrade.info"), write_version(clusters->getCurrentWritingVersion()), read_versions({write_version}),
      retain_versions({write_version})
{
    if (Poco::File(data_path).exists())
    {
        ReadBufferFromFile version_buffer(data_path);
        readBinary(read_versions, version_buffer);
        readBinary(retain_versions, version_buffer);
        readStringBinary(write_version, version_buffer);
    }


}

void UpgradeProgress::store()
{
    Poco::File(dir).createDirectories();
    WriteBufferFromFile version_buffer(data_path);
//    writeBinary(state, version_buffer);
    writeBinary(read_versions, version_buffer);
    writeBinary(retain_versions, version_buffer);
    writeStringBinary(write_version, version_buffer);

}

}