#pragma once

#include <Columns/IColumn.h>

namespace DB
{

class IPartOutputStream
{
public:
    virtual ~IPartOutputStream() = default;

    virtual void writePrefix() const = 0;

    virtual void writeSuffix() const = 0;

    virtual Columns getSampleColumns() const = 0;

    virtual void writeColumns(const Columns & write_columns) const = 0;
};

}
