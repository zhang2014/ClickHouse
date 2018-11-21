#pragma once

#include <Core/Types.h>
#include <QingCloud/Interpreters/MultiplexedVersionCluster.h>

namespace DB
{

enum ProgressEnum
{
    INITIALIZE_UPGRADE_VERSION  = 1,
    REDIRECT_VERSIONS_BEFORE_MIGRATE = 2,
    FLUSH_OLD_VERSION_DATA = 3,
    MIGRATE_OLD_VERSION_DATA = 4,
    FLUSH_UPGRADE_VERSION_DATA = 5,                   /// At this time, old version and upgrade version data is consistent
    REDIRECT_VERSIONS_AFTER_MIGRATE = 6,
    MIGRATE_TMP_VERSION_DATA = 7,                     /// At this time, Temp version data is detached
    REDIRECT_VERSION_AFTER_ALL_MIGRATE = 8,
    DELETE_OUTDATED_VERSIONS = 9,                     /// At this time, Upgrade version is Done.

    NORMAL = 10
};

class UpgradeProgress
{
public:
    UpgradeProgress(const String &path, const MultiplexedClusterPtr &clusters);

    void store();
public:
    String dir;
    String data_path;
    ProgressEnum state = NORMAL;
    String write_version;
    std::vector<String> read_versions;
    std::vector<String> retain_versions;

};

}
