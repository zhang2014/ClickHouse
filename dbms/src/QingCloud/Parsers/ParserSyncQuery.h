//#pragma once
//
//
//#include <Parsers/IParserBase.h>
//
//namespace DB
//{
///** Query of form
// * SYNC [TABLE] [database.]table REPLICAS
// */
//class ParserSyncQuery : public IParserBase
//{
//protected:
//    const char * getName() const { return "ALTER query"; }
//    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
//};
//}