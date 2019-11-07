#pragma once

#include <Core/Types.h>
#include <Common/config.h>
#include <Common/HTMLForm.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomHTTP/CustomQueryExecutors.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <Poco/Net/HTTPServerRequest.h>


#if USE_RE2_ST
#    include <re2_st/re2.h>
#else
#    define re2_st re2
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

class CustomExecutorMatcher
{
public:
    virtual ~CustomExecutorMatcher() = default;

    virtual bool checkQueryExecutors(const std::vector<CustomQueryExecutorPtr> &check_executors) const = 0;

    virtual bool match(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params) const = 0;
};

using CustomExecutorMatcherPtr = std::shared_ptr<CustomExecutorMatcher>;


class AlwaysMatchedCustomExecutorMatcher : public CustomExecutorMatcher
{
public:
    bool checkQueryExecutors(const std::vector<CustomQueryExecutorPtr> & /*check_executors*/) const override { return true; }

    bool match(Context & /*context*/, Poco::Net::HTTPServerRequest & /*request*/, HTMLForm & /*params*/) const override { return true; }
};

class HTTPMethodCustomExecutorMatcher : public CustomExecutorMatcher
{
public:

    HTTPMethodCustomExecutorMatcher(const Poco::Util::AbstractConfiguration & configuration, const String & method_config_key)
    {
        match_method = Poco::toLower(configuration.getString(method_config_key));
    }

    bool checkQueryExecutors(const std::vector<CustomQueryExecutorPtr> & /*check_executors*/) const override { return true; }

    bool match(Context & /*context*/, Poco::Net::HTTPServerRequest & request, HTMLForm & /*params*/) const override
    {
        return Poco::toLower(request.getMethod()) == match_method;
    }

private:
    String match_method;
};

class HTTPURLCustomExecutorMatcher : public CustomExecutorMatcher
{
public:
    HTTPURLCustomExecutorMatcher(const Poco::Util::AbstractConfiguration & configuration, const String & url_config_key)
    {
        const auto & regex_str = configuration.getString(url_config_key);
        regex_matcher = std::make_unique<re2_st::RE2>(regex_str);

        if (!regex_matcher->ok())
            throw Exception("cannot compile re2: " + regex_str + ", error: " + regex_matcher->error() +
                ". Look at https://github.com/google/re2/wiki/Syntax for reference.", ErrorCodes::CANNOT_COMPILE_REGEXP);
    }

    bool checkQueryExecutors(const std::vector<CustomQueryExecutorPtr> & custom_query_executors) const override
    {
        for (const auto & named_capturing_group : regex_matcher->NamedCapturingGroups())
            if (!checkQueryExecutors(named_capturing_group.first, custom_query_executors))
                throw Exception("The param name '" + named_capturing_group.first + "' is uselessed.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return true;
    }

    bool match(Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & /*params*/) const override
    {
        const String request_uri = request.getURI();
        int num_captures = regex_matcher->NumberOfCapturingGroups() + 1;

        re2_st::StringPiece matches[num_captures];
        re2_st::StringPiece input(request_uri.data(), request_uri.size());
        if (regex_matcher->Match(input, 0, request_uri.size(), re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
        {
            const auto & full_match = matches[0];
            const char * url_end = request_uri.data() + request_uri.size();
            const char * not_matched_begin = request_uri.data() + full_match.size();

            if (not_matched_begin != url_end && *not_matched_begin == '/')
                ++not_matched_begin;

            if (not_matched_begin == url_end || *not_matched_begin == '?')
            {
                for (const auto & named_capturing_group : regex_matcher->NamedCapturingGroups())
                {
                    const auto & capturing_value = matches[named_capturing_group.second];
                    context.setQueryParameter(named_capturing_group.first, String(capturing_value.data(), capturing_value.size()));
                }

                return true;
            }
        }
        return false;
    }

private:
    std::unique_ptr<re2_st::RE2> regex_matcher;

    bool checkQueryExecutors(const String & param_name, const std::vector<CustomQueryExecutorPtr> & custom_query_executors) const
    {
        for (const auto & custom_query_executor : custom_query_executors)
            if (custom_query_executor->isQueryParam(param_name))
                return true;

        return false;
    }
};


}
