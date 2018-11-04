#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class QingCloudErroneousBlockInputStream : public IProfilingBlockInputStream
{
public:
    String getName() const override;

    Block getHeader() const override;

private:
    Block readImpl() override;


};

}
