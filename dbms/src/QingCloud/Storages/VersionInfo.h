#pragma once

#include <Core/Types.h>

namespace DB
{

enum ProgressEnum
{
    INITIALIZE_UPGRADE_VERSION  = 1,
    REDIRECT_VERSIONS_BEFORE_MIGRATE = 2,
    FLUSH_OLD_VERSION_DATA = 3,
    CLEANUP_UPGRADE_VERSION = 4,
    MIGRATE_OLD_VERSION_DATA = 5,
    FLUSH_UPGRADE_VERSION_DATA = 6,                   /// At this time, old version and upgrade version data is consistent
    REDIRECT_VERSIONS_AFTER_MIGRATE = 7,
    MIGRATE_TMP_VERSION_DATA = 8,                     /// At this time, Temp version data is detached
    REDIRECT_VERSION_AFTER_ALL_MIGRATE = 9,
    DELETE_OUTDATED_VERSIONS = 10,                     /// At this time, Upgrade version is Done.

    NORMAL = 11


};

class VersionInfo
{
public:
    VersionInfo(const String &path, const String & current_version);

    void store();
public:
    String dir;
    String data_path;
    String write_version;
    ProgressEnum state = NORMAL;
    std::vector<String> read_versions;
    std::vector<String> retain_versions;

};

}
