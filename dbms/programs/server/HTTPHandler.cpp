#include "HTTPHandler.h"

#include <chrono>
#include <iomanip>
#include <Poco/File.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerRequestImpl.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/NetException.h>
#include <ext/scope_guard.h>
#include <Core/ExternalTable.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/config.h>
#include <Common/SettingsChanges.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/BrotliReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Quota.h>
#include <Interpreters/CustomHTTP/CustomExecutor.h>
#include <Interpreters/CustomHTTP/HTTPInputStreams.h>
#include <Interpreters/CustomHTTP/ExtractorClientInfo.h>
#include <Interpreters/CustomHTTP/ExtractorContextChange.h>
#include <Common/typeid_cast.h>
#include <Poco/Net/HTTPStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int READONLY;
    extern const int UNKNOWN_COMPRESSION_METHOD;

    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_OPEN_FILE;

    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int UNEXPECTED_AST_STRUCTURE;

    extern const int SYNTAX_ERROR;

    extern const int INCORRECT_DATA;
    extern const int TYPE_MISMATCH;

    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_FUNCTION;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TYPE;
    extern const int UNKNOWN_STORAGE;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_SETTING;
    extern const int UNKNOWN_DIRECTION_OF_SORTING;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int UNKNOWN_FORMAT;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int UNKNOWN_TYPE_OF_QUERY;

    extern const int QUERY_IS_TOO_LARGE;

    extern const int NOT_IMPLEMENTED;
    extern const int SOCKET_TIMEOUT;

    extern const int UNKNOWN_USER;
    extern const int WRONG_PASSWORD;
    extern const int REQUIRED_PASSWORD;

    extern const int INVALID_SESSION_TIMEOUT;
    extern const int HTTP_LENGTH_REQUIRED;
}


static Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code)
{
    using namespace Poco::Net;

    if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
        return HTTPResponse::HTTP_UNAUTHORIZED;
    else if (exception_code == ErrorCodes::CANNOT_PARSE_TEXT ||
             exception_code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE ||
             exception_code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING ||
             exception_code == ErrorCodes::CANNOT_PARSE_DATE ||
             exception_code == ErrorCodes::CANNOT_PARSE_DATETIME ||
             exception_code == ErrorCodes::CANNOT_PARSE_NUMBER ||

             exception_code == ErrorCodes::UNKNOWN_ELEMENT_IN_AST ||
             exception_code == ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE ||
             exception_code == ErrorCodes::TOO_DEEP_AST ||
             exception_code == ErrorCodes::TOO_BIG_AST ||
             exception_code == ErrorCodes::UNEXPECTED_AST_STRUCTURE ||

             exception_code == ErrorCodes::SYNTAX_ERROR ||

             exception_code == ErrorCodes::INCORRECT_DATA ||
             exception_code == ErrorCodes::TYPE_MISMATCH)
        return HTTPResponse::HTTP_BAD_REQUEST;
    else if (exception_code == ErrorCodes::UNKNOWN_TABLE ||
             exception_code == ErrorCodes::UNKNOWN_FUNCTION ||
             exception_code == ErrorCodes::UNKNOWN_IDENTIFIER ||
             exception_code == ErrorCodes::UNKNOWN_TYPE ||
             exception_code == ErrorCodes::UNKNOWN_STORAGE ||
             exception_code == ErrorCodes::UNKNOWN_DATABASE ||
             exception_code == ErrorCodes::UNKNOWN_SETTING ||
             exception_code == ErrorCodes::UNKNOWN_DIRECTION_OF_SORTING ||
             exception_code == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION ||
             exception_code == ErrorCodes::UNKNOWN_FORMAT ||
             exception_code == ErrorCodes::UNKNOWN_DATABASE_ENGINE ||

             exception_code == ErrorCodes::UNKNOWN_TYPE_OF_QUERY)
        return HTTPResponse::HTTP_NOT_FOUND;
    else if (exception_code == ErrorCodes::QUERY_IS_TOO_LARGE)
        return HTTPResponse::HTTP_REQUESTENTITYTOOLARGE;
    else if (exception_code == ErrorCodes::NOT_IMPLEMENTED)
        return HTTPResponse::HTTP_NOT_IMPLEMENTED;
    else if (exception_code == ErrorCodes::SOCKET_TIMEOUT ||
             exception_code == ErrorCodes::CANNOT_OPEN_FILE)
        return HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
    else if (exception_code == ErrorCodes::HTTP_LENGTH_REQUIRED)
        return HTTPResponse::HTTP_LENGTH_REQUIRED;

    return HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
}


static std::chrono::steady_clock::duration parseSessionTimeout(
    const Poco::Util::AbstractConfiguration & config,
    const HTMLForm & params)
{
    unsigned session_timeout = config.getInt("default_session_timeout", 60);

    if (params.has("session_timeout"))
    {
        unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);
        std::string session_timeout_str = params.get("session_timeout");

        ReadBufferFromString buf(session_timeout_str);
        if (!tryReadIntText(session_timeout, buf) || !buf.eof())
            throw Exception("Invalid session timeout: '" + session_timeout_str + "'", ErrorCodes::INVALID_SESSION_TIMEOUT);

        if (session_timeout > max_session_timeout)
            throw Exception("Session timeout '" + session_timeout_str + "' is larger than max_session_timeout: " + toString(max_session_timeout)
                + ". Maximum session timeout could be modified in configuration file.",
                ErrorCodes::INVALID_SESSION_TIMEOUT);
    }

    return std::chrono::seconds(session_timeout);
}


HTTPHandler::HTTPHandler(IServer & server_)
    : server(server_), log(&Logger::get("HTTPHandler"))
{
    server_display_name = server.config().getString("display_name", getFQDNOrHostName());
}


HTTPHandler::SessionContextHolder::~SessionContextHolder()
{
    if (session_context)
        session_context->releaseSession(session_id, session_timeout);
}


HTTPHandler::SessionContextHolder::SessionContextHolder(IServer & accepted_server, HTMLForm & params)
{
    session_id = params.get("session_id", "");
    context = std::make_unique<Context>(accepted_server.context());

    if (!session_id.empty())
    {
        session_timeout = parseSessionTimeout(accepted_server.config(), params);
        session_context = context->acquireSession(session_id, session_timeout, params.check<String>("session_check", "1"));

        context = std::make_unique<Context>(*session_context);
        context->setSessionContext(*session_context);
    }
}

void HTTPHandler::SessionContextHolder::authentication(HTTPServerRequest & request, HTMLForm & params)
{
    auto user = request.get("X-ClickHouse-User", "");
    auto password = request.get("X-ClickHouse-Key", "");
    auto quota_key = request.get("X-ClickHouse-Quota", "");

    if (user.empty() && password.empty() && quota_key.empty())
    {
        /// User name and password can be passed using query parameters
        /// or using HTTP Basic auth (both methods are insecure).
        if (request.hasCredentials())
        {
            Poco::Net::HTTPBasicCredentials credentials(request);

            user = credentials.getUsername();
            password = credentials.getPassword();
        }
        else
        {
            user = params.get("user", "default");
            password = params.get("password", "");
        }

        quota_key = params.get("quota_key", "");
    }
    else
    {
        /// It is prohibited to mix different authorization schemes.
        if (request.hasCredentials()
            || params.has("user")
            || params.has("password")
            || params.has("quota_key"))
        {
            throw Exception("Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods simultaneously", ErrorCodes::REQUIRED_PASSWORD);
        }
    }

    std::string query_id = params.get("query_id", "");
    context->setUser(user, password, request.clientAddress(), quota_key);
    context->setCurrentQueryId(query_id);
}

void HTTPHandler::processQuery(Context & context, HTTPRequest & request, HTMLForm & params, HTTPResponse & response)
{
    const auto & name_with_custom_executor = context.getCustomExecutor(request/*, params*/);
    LOG_TRACE(log, "Using " << name_with_custom_executor.first << " to execute URI: " << request.getURI());

    ExtractorClientInfo{context.getClientInfo()}.extract(request);
    ExtractorContextChange{context, name_with_custom_executor.second}.extract(request, params);

    HTTPInputStreams input_streams{context, request, params};
    HTTPOutputStreams output_streams = HTTPOutputStreams(context, request, response, params, getKeepAliveTimeout());

    const auto & query_executors = name_with_custom_executor.second->getQueryExecutor(context, request, params, input_streams);
    for (const auto & query_executor : query_executors)
        query_executor(output_streams, response);

    output_streams.finalize(); /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to the client.
}

void HTTPHandler::trySendExceptionToClient(
    const std::string & message, int exception_code, Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, bool compression)
{
    try
    {
        /// If HTTP method is POST and Keep-Alive is turned on, we should read the whole request body
        /// to avoid reading part of the current request body in the next request.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && response.getKeepAlive()
                && !request.stream().eof() && exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED)
            request.stream().ignore(std::numeric_limits<std::streamsize>::max());

        if (exception_code == ErrorCodes::UNKNOWN_USER || exception_code == ErrorCodes::WRONG_PASSWORD ||
            exception_code == ErrorCodes::REQUIRED_PASSWORD || exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED)
        {
            response.requireAuthentication("ClickHouse server HTTP API");
            response.send() << message << std::endl;
        }
        else
        {
            response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
            HTTPOutputStreams output_streams(request, response, compression, getKeepAliveTimeout());

            writeString(message, *output_streams.out_maybe_compressed);
            writeChar('\n', *output_streams.out_maybe_compressed);

            output_streams.finalize();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot send exception to client");
    }
}

void HTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    setThreadName("HTTPHandler");
    ThreadStatus thread_status;

    /// In case of exception, send stack trace to client.
    bool with_stacktrace = false, internal_compression = false;

    try
    {
        response.set("Content-Type", "text/plain; charset=UTF-8");
        response.set("X-ClickHouse-Server-Display-Name", server_display_name);

        /// For keep-alive to work.
        if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        HTMLForm params(request);
        with_stacktrace = params.getParsed<bool>("stacktrace", false);
        internal_compression = params.getParsed<bool>("compress", false);

        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() && !request.hasContentLength())
            throw Exception("There is neither Transfer-Encoding header nor Content-Length header", ErrorCodes::HTTP_LENGTH_REQUIRED);

        {
            SessionContextHolder holder{server, params};
            CurrentThread::QueryScope query_scope(*holder.context);

            holder.authentication(request, params);
            processQuery(*holder.context, request, params, response);
            LOG_INFO(log, "Done processing query");
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        int exception_code = getCurrentExceptionCode();
        std::string exception_message = getCurrentExceptionMessage(with_stacktrace, true);
        trySendExceptionToClient(exception_message, exception_code, request, response, internal_compression);
    }
}

}
