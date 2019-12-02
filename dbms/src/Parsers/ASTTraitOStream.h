#pragma once

#include <ostream>
#include <sstream>

namespace DB
{

template <typename ASTTrait>
class ASTTraitWithNamedChildren;

template <typename ASTTrait>
struct ASTTraitOStream
{
    using FormatState = typename  ASTTraitWithNamedChildren<ASTTrait>::FormatState;
    using FormatSettings = typename  ASTTraitWithNamedChildren<ASTTrait>::FormatSettings;
    using FormatStateStacked = typename  ASTTraitWithNamedChildren<ASTTrait>::FormatStateStacked;

    enum OperatorsKind
    {
        GET_ID,
        FORMAT,
    };

    OperatorsKind operators_kind;

    std::ostream & out;
    const ASTTraitWithNamedChildren<ASTTrait> * node;

    char delim = '\0';
    FormatState * state = nullptr;
    const FormatSettings * settings = nullptr;
    FormatStateStacked * state_stacked = nullptr;

    ASTTraitOStream(std::ostringstream & out_, const ASTTraitWithNamedChildren<ASTTrait> * node_, char delim_)
        : operators_kind(OperatorsKind::GET_ID), out(out_), node(node_), delim(delim_)
    {
    }

    ASTTraitOStream(
        std::ostream & out_, const ASTTraitWithNamedChildren<ASTTrait> * node_,
        FormatState * state_, const FormatSettings * settings_, FormatStateStacked * state_stacked_)
        : operators_kind(OperatorsKind::FORMAT), out(out_), node(node_), state(state_), settings(settings_), state_stacked(state_stacked_)
    {
    }
};

template<typename ASTTrait>
inline ASTTraitOStream<ASTTrait> & operator<<(ASTTraitOStream<ASTTrait> & stream, const char x) { stream.out << x; return stream; }

template<typename ASTTrait>
inline ASTTraitOStream<ASTTrait> & operator<<(ASTTraitOStream<ASTTrait> & stream, const char * x) { stream.out << x; return stream; }

template<typename ASTTrait>
inline ASTTraitOStream<ASTTrait> & operator<<(ASTTraitOStream<ASTTrait> & stream, const std::string & x) { stream.out << x; return stream; }

template<typename ASTTrait>
inline ASTTraitOStream<ASTTrait> & operator<<(ASTTraitOStream<ASTTrait> & stream, typename ASTTraitWithNamedChildren<ASTTrait>::Children children)
{
    switch(stream.operators_kind)
    {
        case ASTTraitOStream<ASTTrait>::OperatorsKind::GET_ID:
            stream.out << stream.node->getChild(children)->getID(stream.delim);
            break;
        case ASTTraitOStream<ASTTrait>::OperatorsKind::FORMAT:
            stream.node->getChild(children)->formatImpl(*stream.settings, *stream.state, *stream.state_stacked);
            break;
    }

    return stream;
}

}
