#pragma once

#include "IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>

#include <Common/CurrentMetrics.h>
#include <Common/HTMLForm.h>

#include <Interpreters/CustomHTTP/HTTPStreamsWithOutput.h>


namespace CurrentMetrics
{
    extern const Metric HTTPConnection;
}

namespace Poco { class Logger; }

namespace DB
{

class WriteBufferFromHTTPServerResponse;


class HTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPHandler(IServer & server_);

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

    /// This method is called right before the query execution.
    virtual void customizeContext(DB::Context& /* context */) {}

private:
    struct Output
    {
        /* Raw data
         * ↓
         * CascadeWriteBuffer out_maybe_delayed_and_compressed (optional)
         * ↓ (forwards data if an overflow is occur or explicitly via pushDelayedResults)
         * CompressedWriteBuffer out_maybe_compressed (optional)
         * ↓
         * WriteBufferFromHTTPServerResponse out
         */

        std::shared_ptr<WriteBufferFromHTTPServerResponse> out;
        /// Points to 'out' or to CompressedWriteBuffer(*out), depending on settings.
        std::shared_ptr<WriteBuffer> out_maybe_compressed;
        /// Points to 'out' or to CompressedWriteBuffer(*out) or to CascadeWriteBuffer.
        std::shared_ptr<WriteBuffer> out_maybe_delayed_and_compressed;

        inline bool hasDelayed() const
        {
            return out_maybe_delayed_and_compressed != out_maybe_compressed;
        }
    };

    IServer & server;
    Poco::Logger * log;

    /// It is the name of the server that will be sent in an http-header X-ClickHouse-Server-Display-Name.
    String server_display_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

    /// Also initializes 'used_output'.
    void processQuery(
        Poco::Net::HTTPServerRequest & request,
        HTMLForm & params,
        Poco::Net::HTTPServerResponse & response,
        HTTPStreamsWithOutput & used_output);

    void trySendExceptionToClient(
        const std::string & s,
        int exception_code,
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response,
        HTTPStreamsWithOutput & used_output);

    void pushDelayedResults(Output & used_output);
};

}
