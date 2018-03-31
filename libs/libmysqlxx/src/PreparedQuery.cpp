#include <mysql/mysql.h>

#include <mysqlxx/Connection.h>
#include <mysqlxx/PreparedQuery.h>


namespace mysqlxx
{

PreparedQuery::PreparedQuery(Connection *conn_, const std::string &query_string) : conn(conn_)
{
    mysql_thread_init();
    stmt = mysql_stmt_init(conn->getDriver());
    if (mysql_stmt_prepare(stmt, query_string.c_str(), static_cast<unsigned int>(query_string.length())))
    {
        int err_code = mysql_errno(conn->getDriver());
        if (err_code == 2006 /* CR_SERVER_GONE_ERROR */ || err_code == 2013 /* CR_SERVER_LOST */)
        {
            err_code = 0;
            if (mysql_stmt_prepare(stmt, query_string.c_str(), static_cast<unsigned int>(query_string.length())))
                err_code = mysql_errno(conn->getDriver());
        }

        if (err_code != 0)
            throw BadQuery(errorMessage(conn->getDriver()), err_code);
    }

    if (const auto param_count = mysql_stmt_param_count(stmt))
        binds.reserve(param_count);
}

PreparedQuery::PreparedQuery(const PreparedQuery &other) : conn(other.conn), stmt(other.stmt)
{
    mysql_thread_init();
}

PreparedQuery::~PreparedQuery()
{
    mysql_stmt_close(stmt);
    mysql_thread_end();
}

void PreparedQuery::executeImpl()
{
    if (mysql_stmt_bind_param(stmt, binds.data()))
        throw BadQuery(errorMessage(conn->getDriver()), mysql_errno(conn->getDriver()));

    if (mysql_stmt_execute(stmt) != 0)
        throw BadQuery(errorMessage(conn->getDriver()), mysql_errno(conn->getDriver()));

    my_ulonglong affected_rows_ = mysql_affected_rows(conn->getDriver());
    if (affected_rows_ != ((my_ulonglong) -1))
        affected_rows = static_cast<std::size_t>(affected_rows_);
}

void PreparedQuery::setString(size_t index, std::string value, bool nullable)
{
    binds[index].buffer = (char *) value.data();
    binds[index].buffer_type = MYSQL_TYPE_STRING;
    binds[index].buffer_length = value.size();
    binds[index].is_null = (my_bool *) nullable;
//    binds[index].length= &length[0];
//    binds[index].error= &error[0];
}

}
