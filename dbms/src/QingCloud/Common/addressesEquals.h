#pragma once

#include <Interpreters/Cluster.h>

namespace DB
{

inline bool addressEquals(Cluster::Address expect, Cluster::Address actual)
{
    if (expect.host_name != actual.host_name)
        return false;
    if (expect.port != actual.port)
        return false;
    if (expect.user != actual.user)
        return false;
    if (expect.password != actual.password)
        return false;
    if (expect.secure != actual.secure)
        return false;

    return expect.compression == actual.compression;
}

}