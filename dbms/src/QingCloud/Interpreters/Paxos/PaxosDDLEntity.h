#pragma once

#include <mutex>
#include <Core/Types.h>
#include <condition_variable>

namespace DB
{

using LogEntity = std::pair<UInt64, String>;

class DDLEntity
{
public:
    std::recursive_mutex mutex;
    UInt64 applied_paxos_id = 0;
    UInt64 applied_entity_id = 0;

    UInt64 accepted_paxos_id = 0;
    UInt64 accepted_entity_id = 0;
    String accepted_entity_value = "";
    std::condition_variable_any learning_cond;

    DDLEntity(const String & data_path);

    void store();


private:
    const String dir;
    const String data_path;
};

}
