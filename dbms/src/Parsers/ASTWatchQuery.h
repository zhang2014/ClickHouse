/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#pragma once

#include <Parsers/ASTTraitWithNamedChildren.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>


namespace DB
{

class ASTWatchQueryTrait : public ASTQueryWithOutput
{
public:
    bool is_watch_events;

protected:
    enum class Children : UInt8
    {
        TABLE_EXPRESSION,
        LIMIT_LENGTH,
    };

    void reset(ASTWatchQueryTrait & res) const { cloneOutputOptions(res); }

    /** Get the text that identifies this element. */
    void identifier(char delim, ASTTraitOStream<ASTWatchQueryTrait> & out) const { out << "WatchQuery" << delim << Children::TABLE_EXPRESSION; }

    void formatSyntax(ASTTraitOStream<ASTWatchQueryTrait> & out, const FormatSettings & settings, FormatState &, FormatStateStacked frame) const
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

        out << (settings.hilite ? hilite_keyword : "") << "WATCH" << " " << (settings.hilite ? hilite_none : "")
            << Children::TABLE_EXPRESSION;

        if (is_watch_events)
            out << " " << (settings.hilite ? hilite_keyword : "") << "EVENTS" << (settings.hilite ? hilite_none : "");

        if (out.node->getChild(Children::LIMIT_LENGTH))
            out << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << indent_str << "LIMIT "
                << (settings.hilite ? hilite_none : "") << Children::LIMIT_LENGTH;
    }
};

using ASTWatchQuery = ASTTraitWithOutput<ASTWatchQueryTrait>;

}
