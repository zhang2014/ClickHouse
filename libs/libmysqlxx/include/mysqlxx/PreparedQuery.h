#pragma once

#include <sstream>
#include <ostream>

#include <mysqlxx/UseQueryResult.h>
#include <mysqlxx/StoreQueryResult.h>


namespace mysqlxx
{

class PreparedQuery : public std::ostream
{
public:
    PreparedQuery(Connection * conn_, const std::string & query_string = "");
    PreparedQuery & operator= (const PreparedQuery & other);
    ~PreparedQuery();

    void execute();

    std::string str() const
    {
        return query_buf.str();
    }

private:
    Connection * conn;
    std::stringbuf query_buf;

    void executeImpl();
};


/// Вывести текст запроса в ostream.
inline std::ostream & operator<< (std::ostream & ostr, const Query & query)
{
    return ostr << query.rdbuf();
}


}
