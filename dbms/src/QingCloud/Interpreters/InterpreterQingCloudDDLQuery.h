#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/IInterpreter.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <QingCloud/Interpreters/Paxos/QingCloudDDLSynchronism.h>

namespace DB
{

class QingCloudDDLBlockInputStream : public IProfilingBlockInputStream
{
public:
    QingCloudDDLBlockInputStream(const String & ddl_query, Context & context);

    String getName() const override;

    Block getHeader() const override;

private:
    String ddl_query;
    Context & context;
    QingCloudDDLSynchronismPtr synchronism;

    bool is_enqueue{false};

    Block readImpl() override;
};


class InterpreterQingCloudDDLQuery : public IInterpreter
{
public:
    InterpreterQingCloudDDLQuery(Context & context, ASTPtr & query);

    BlockIO execute() override;

private:
    Context & context;
    ASTPtr & query;
};

}