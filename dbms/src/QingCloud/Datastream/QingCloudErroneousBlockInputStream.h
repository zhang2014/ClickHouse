#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class QingCloudErroneousBlockInputStream : public IProfilingBlockInputStream
{
public:
    QingCloudErroneousBlockInputStream(const BlockInputStreamPtr & input);

    String getName() const override;

    Block getHeader() const override;

private:
    bool over{false};

    Block readImpl() override;


};

}
