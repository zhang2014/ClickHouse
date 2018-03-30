#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <mysqlxx/Query.h>
#include <mysqlxx/PoolWithFailover.h>

namespace DB
{

class MySQLBlockOutputStream : public IBlockOutputStream
{
public:
    MySQLBlockOutputStream(const mysqlxx::PoolWithFailover::Entry & entry, const std::string & query_str);

    void write(const Block & block) override;

private:
    mysqlxx::PoolWithFailover::Entry entry;
    mysqlxx::PreparedQuery prepared_query;
};

}
