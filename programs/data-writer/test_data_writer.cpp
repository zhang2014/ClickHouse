
#include "DataPartWriter.h"
#include <Columns/ColumnsNumber.h>
//#include <Colu>

int main(int /*argc_*/, char ** /*argv_*/)
{
    using namespace DB;

    try
    {
        ColumnsNameAndTypeName columns_name_and_type_name;
        columns_name_and_type_name.emplace_back(std::make_pair("date", "Date"));
        columns_name_and_type_name.emplace_back(std::make_pair("value", "UInt32"));
        DataPartWriter writer("/Users/coswde/Source/ClickHouse/build/", columns_name_and_type_name, 8192, "PARTITION BY toYYYYMM(date)", "ORDER BY date");
        auto output = writer.getPartOutputStream();
        output->writePrefix();

        for (size_t i = 0; i < 16; ++i)
        {
            ColumnUInt16::MutablePtr column_date = ColumnUInt16::create();
            ColumnUInt32::MutablePtr column_value = ColumnUInt32::create();

            column_date->reserve(65535);
            column_value->reserve(65535);
            for (size_t index = 0; index < 65535; ++index)
            {
                column_date->insertValue(0);
                column_value->insertValue(UInt32((i + 1) * index));
            }

            Columns write_columns(2);
            write_columns[0] = std::move(column_date);
            write_columns[1] = std::move(column_value);

            output->writeColumns(write_columns);
        }

        output->writeSuffix();
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << "\n";
        return 1;
//        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return 0;
}
