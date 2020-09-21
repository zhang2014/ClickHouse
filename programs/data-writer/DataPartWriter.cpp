#include "DataPartWriter.h"
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include "LocalEnvironment.h"
#include "MergeTreeDataPartOutputStream.h"
#include <Parsers/queryToString.h>

namespace DB
{

static LocalEnvironment local_environment;

DataPartWriter::DataPartWriter(
    const std::string & data_dir_, const ColumnsNameAndTypeName & columns_name_and_type_name_,
    size_t granularity_size, const std::string & partition_by, const std::string & order_by)
    : data_dir(data_dir_)
{
    NamesAndTypesList columns_type_and_name;

    for (const auto & column_name_and_type_name : columns_name_and_type_name_)
    {
        const auto & column_name = column_name_and_type_name.first;
        const auto & column_type_name = column_name_and_type_name.second;
        DataTypePtr data_type = DataTypeFactory::instance().get(column_type_name);
        columns_type_and_name.emplace_back(NameAndTypePair{column_name, data_type});
    }

    local_environment.initializeEnvironment(data_dir);
    local_environment.createTemporaryTable(columns_type_and_name, granularity_size, partition_by, order_by);
}

std::shared_ptr<IPartOutputStream> DataPartWriter::getPartOutputStream(const std::string & part_name)
{
    StorageMergeTree * temporary_storage_merge_tree = typeid_cast<StorageMergeTree *>(local_environment.getTemporaryTable().get());
    std::shared_ptr<MergeTreeDataWriter> data_writer_ptr = std::make_shared<MergeTreeDataWriter>(*temporary_storage_merge_tree);

    const StorageMetadataPtr & metadata_snapshot = local_environment.getTemporaryTable()->getInMemoryMetadataPtr();
    return std::make_shared<MergeTreeDataPartOutputStream>(
        data_writer_ptr, metadata_snapshot, *temporary_storage_merge_tree, data_dir, part_name);
}

DataPartWriter::~DataPartWriter()
{
    auto query_context_ptr = local_environment.getQueryContext();
    local_environment.getDefaultDatabase()->dropTable(*query_context_ptr, local_environment.getTemporaryTable()->getStorageID().table_name, true);
    local_environment.destroyEnvironment();
}

}
