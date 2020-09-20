#pragma once

#include <vector>
#include <Storages/IStorage.h>
#include "IPartDataOutputStream.h"

namespace DB
{

using ColumnNameAndTypeName = std::pair<std::string, std::string>;
using ColumnsNameAndTypeName = std::vector<ColumnNameAndTypeName>;

class DataPartWriter
{
public:
    ~DataPartWriter();

    DataPartWriter(
        const std::string & data_dir_, const ColumnsNameAndTypeName & columns_name_and_type_name_,
        size_t granularity_size = 8192, const std::string & partition_by = "", const std::string & order_by = "");

    std::shared_ptr<IPartOutputStream> getPartOutputStream(const std::string & part_name = "");

private:
    std::string data_dir;
    StoragePtr temporary_storage;
};

}
