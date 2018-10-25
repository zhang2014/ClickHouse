//#include <QingCloud/Parsers/ParserPaxosQuery.h>
//#include <Parsers/CommonParsers.h>
//#include <Parsers/ExpressionElementParsers.h>
//#include <Core/Field.h>
//#include <Parsers/ASTIdentifier.h>
//#include <Common/typeid_cast.h>
//#include <Parsers/ASTLiteral.h>
//#include "ParserPaxosQuery.h"
//#include "ASTPaxosQuery.h"
//
//
//namespace DB
//{
//
//static bool parseNameValuePair(std::pair<String, Field> & name_value_pair, IParser::Pos & pos, Expected & expected)
//{
//    ParserIdentifier name_p;
//    ParserLiteral value_p;
//    ParserToken s_eq(TokenType::Equals);
//
//    ASTPtr name;
//    ASTPtr value;
//
//    if (!name_p.parse(pos, name, expected))
//        return false;
//
//    if (!s_eq.ignore(pos, expected))
//        return false;
//
//    if (!value_p.parse(pos, value, expected))
//        return false;
//
//    name_value_pair.first = typeid_cast<const ASTIdentifier *>(name.get())->name;
//    name_value_pair.second = typeid_cast<const ASTLiteral *>(value.get())->value;
//
//    return true;
//}
//
//bool ParserPaxos::parseImpl(IParser::Pos &pos, ASTPtr &node, Expected &expected)
//{
//    ParserKeyword s_paxos("PAXOS");
//    ParserIdentifier paxos_kind_p;
//
//    ASTPtr kind;
//
//    if (!s_paxos.ignore(pos))
//        return false;
//
//    if (!paxos_kind_p.parse(pos, kind, expected))
//        return false;
//
//    std::unordered_map<String, Field> names_values;
//    for (size_t index = 0; pos.isValid(); ++index)
//    {
//        std::pair<String, Field> name_value_pair;
//        parseNameValuePair(name_value_pair, pos, expected);
//        names_values.insert(name_value_pair);
//    }
//
//    auto paxos_query = std::make_shared<ASTPaxosQuery>();
//    paxos_query->kind = typeid_cast<const ASTIdentifier *>(kind.get())->name;
//    paxos_query->names_and_values = names_values;
//    node = paxos_query;
//
//    return false;
//}
//
//}
