#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTTraitOStream.h>

#include <ostream>
#include <sstream>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename ASTTrait>
class ASTTraitWithNamedChildren : public ASTTrait
{
public:
    using Children = typename ASTTrait::Children;
    using FormatState = typename ASTTrait::FormatState;
    using FormatSettings = typename ASTTrait::FormatSettings;
    using FormatStateStacked = typename ASTTrait::FormatStateStacked;

    const ASTPtr getChild(Children child_name) const
    {
        auto it = positions.find(child_name);

        if (it != positions.end())
            return ASTTrait::children[it->second];

        return {};
    }

    ASTPtr & getChildRef(Children child_name)
    {
        if (!positions.count(child_name))
            throw Exception("Get expression before set", ErrorCodes::LOGICAL_ERROR);

        return ASTTrait::children[positions[child_name]];
    }

    void setChild(Children child_name, ASTPtr && child)
    {
        if (child)
        {
            auto it = positions.find(child_name);
            if (it == positions.end())
            {
                positions[child_name] = ASTTrait::children.size();
                ASTTrait::children.emplace_back(child);
            }
            else
                ASTTrait::children[it->second] = child;
        }
        else if (positions.count(child_name))
        {
            size_t pos = positions[child_name];
            ASTTrait::children.erase(ASTTrait::children.begin() + pos);
            positions.erase(child_name);
            for (auto & pr : positions)
                if (pr.second > pos)
                    --pr.second;
        }
    }

protected:
    std::unordered_map<Children, size_t> positions;
};

template <typename ASTTrait>
class ASTTraitWithOutput : public ASTTraitWithNamedChildren<ASTTrait>
{
public:
    using FormatState = typename ASTTrait::FormatState;
    using FormatSettings = typename ASTTrait::FormatSettings;
    using FormatStateStacked = typename ASTTrait::FormatStateStacked;

    String getID(char delim) const override
    {
        std::ostringstream out;
        ASTTraitOStream<ASTTrait> stream(out, this, delim);

        ASTTrait::identifier(delim, stream);
        return out.str();
    }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTTraitWithOutput<ASTTrait>>(*this);

        res->children.clear();

        ASTTrait::reset(*res);
        for (const auto & position : ASTTraitWithNamedChildren<ASTTrait>::positions)
            res->setChild(position.first, ASTTrait::children.at(position.second)->clone());

        return res;
    }

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        ASTTraitOStream<ASTTrait> stream(settings.ostr, this, &state, &settings, &frame);
        ASTTrait::formatSyntax(stream, settings, state, frame);
    }
};


template <typename ASTTrait>
class ASTTraitWithNonOutput : public ASTTraitWithNamedChildren<ASTTrait>
{
public:
    using FormatState = typename ASTTrait::FormatState;
    using FormatSettings = typename ASTTrait::FormatSettings;
    using FormatStateStacked = typename ASTTrait::FormatStateStacked;

    String getID(char delim) const override
    {
        std::ostringstream out;
        ASTTraitOStream<ASTTrait> stream(out, this, delim);

        ASTTrait::identifier(delim, stream);
        return out.str();
    }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTTraitWithNonOutput<ASTTrait>>(*this);

        res->children.clear();

        ASTTrait::reset(*res);
        for (const auto & position : ASTTraitWithNamedChildren<ASTTrait>::positions)
            res->setChild(position.first, ASTTrait::children.at(position.second)->clone());

        return res;
    }

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        ASTTraitOStream<ASTTrait> stream(settings.ostr, this, &state, &settings, &frame);
        ASTTrait::formatSyntax(stream, settings, state, frame);
    }
};

}

