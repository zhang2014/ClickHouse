#include <QingCloud/Datastream/QingCloudErroneousBlockInputStream.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/materializeBlock.h>
#include <Columns/ColumnConst.h>
#include "QingCloudErroneousBlockInputStream.h"
#include <Core/Block.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
}

String QingCloudErroneousBlockInputStream::getName() const
{
    return "QingCloudErroneousBlockInputStream";
}

Block QingCloudErroneousBlockInputStream::getHeader() const
{
    Block origin_header = children[0]->getHeader();

    if (origin_header.has("_res_code") || origin_header.has("_exception_message"))
        throw Exception("Origin header already exists _res_code or _expression_message columns.",
                        ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);

    Block new_header = origin_header.cloneEmpty();
    new_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_res_code"});
    new_header.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "_exception_message"});
    return new_header;
}

Block QingCloudErroneousBlockInputStream::readImpl()
{
    if (over)
        return {};

    try
    {
        Block origin_res = children[0]->read();

        if (origin_res)
        {
            DataTypePtr code_type = std::make_shared<DataTypeUInt64>();
            DataTypePtr exception_message_type = std::make_shared<DataTypeString>();
            origin_res.insert({ code_type->createColumnConstWithDefaultValue(origin_res.rows()), code_type, "_res_code" });
            origin_res.insert({ exception_message_type->createColumnConstWithDefaultValue(origin_res.rows()), exception_message_type, "_exception_message" });

            return materializeBlock(origin_res);
        }

        return origin_res;
    }
    catch (...)
    {
        over = true;
        tryLogCurrentException(&Logger::get("QingCloudErroneousBlockInputStream"));
        Block exception_block = getHeader().cloneEmpty();
        MutableColumns new_columns = exception_block.cloneEmptyColumns();

        size_t res_code_column_position = exception_block.getPositionByName("_res_code");
        size_t exception_message_position = exception_block.getPositionByName("_exception_message");

        for (size_t column = 0; column < exception_block.columns(); ++column)
        {
            if (column == res_code_column_position)
                new_columns[column]->insert(UInt64(1));
            else if (column == exception_message_position)
                new_columns[column]->insert(getCurrentExceptionMessage(false));
            else
                new_columns[column]->insertDefault();
        }

        exception_block.setColumns(std::move(new_columns));
        return exception_block;
    }
}

QingCloudErroneousBlockInputStream::QingCloudErroneousBlockInputStream(const BlockInputStreamPtr & input)
{
    children.emplace_back(input);
}

}
