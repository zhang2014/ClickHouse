#pragma once

#include <sstream>
#include <ostream>

#include <mysqlxx/UseQueryResult.h>
#include <mysqlxx/StoreQueryResult.h>


namespace mysqlxx
{

class PreparedQuery
{
public:
    PreparedQuery(Connection * conn_, const std::string & query_string = "");
    PreparedQuery(const PreparedQuery & other);
    PreparedQuery & operator= (const PreparedQuery & other);
    ~PreparedQuery();

    void setString(size_t index, std::string value, bool nullable);

private:
    Connection * conn;
    MYSQL_STMT * stmt;
    std::vector<MYSQL_BIND> binds;
    std::size_t affected_rows;


    void executeImpl();
};

}
