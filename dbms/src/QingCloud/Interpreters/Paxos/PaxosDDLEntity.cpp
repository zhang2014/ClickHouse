#include <QingCloud/Interpreters/Paxos/PaxosDDLEntity.h>
#include <Poco/File.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include "PaxosDDLEntity.h"


namespace DB
{


DDLEntity::DDLEntity(const String &data_path_) : dir(data_path_), data_path(data_path_ + "paxos.info")
{
    if (Poco::File(data_path).exists())
    {
        ReadBufferFromFile buffer(data_path);
        readBinary(applied_paxos_id, buffer);
        readBinary(applied_entity_id, buffer);
        readBinary(accepted_paxos_id, buffer);
        readBinary(accepted_entity_id, buffer);
        readStringBinary(accepted_entity_value, buffer);
    }

    store();
}

void DDLEntity::store()
{
    Poco::File(dir).createDirectories();
    WriteBufferFromFile buffer(data_path);
    writeBinary(applied_paxos_id, buffer);
    writeBinary(applied_entity_id, buffer);
    writeBinary(accepted_paxos_id, buffer);
    writeBinary(accepted_entity_id, buffer);
    writeStringBinary(accepted_entity_value, buffer);
    buffer.sync();
}

}