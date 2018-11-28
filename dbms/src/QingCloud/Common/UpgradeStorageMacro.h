#pragma once

#define BEGIN_WITH(BEGIN_ENUM, END_ENUM) \
    ProgressEnum progress_enum = table_and_progress.second; \
    DatabaseAndTableName database_and_table_name = table_and_progress.first; \
    \
    const auto & table_name = database_and_table_name.second; \
    const auto & database_name = database_and_table_name.first; \
    for (ProgressEnum enum_it = BEGIN_ENUM; enum_it < BEGIN_ENUM;) \
        switch(enum_it) \
        { \
            default: throw Exception("LOGICAL ERROR: Progress Enum Cannot find in one stage.", ErrorCodes::LOGICAL_ERROR);

#define END_WITH(BEGIN_ENUM, END_ENUM) }

#define CASE_WITH_ALTER_LOCK(CURRENT_ENUM, NEXT_ENUM, UPGRADE_METHOD, ...) \
    case CURRENT_ENUM : \
        if (progress_enum == CURRENT_ENUM) \
        { \
            const auto storage = context.tryGetTable(database_name, table_name); \
            if (storage && !storage->is_dropped) \
            { \
                const auto storage_lock = storage->lockStructureForAlter(__PRETTY_FUNCTION__); \
                if (StorageQingCloud * upgrade_storage = dynamic_cast<StorageQingCloud *>(storage.get())) \
                    upgrade_storage->UPGRADE_METHOD (__VA_ARGS__); \
            } \
        } \
        safety_point_sync->broadcastSync(progressToString(CURRENT_ENUM) + "_" + database_name + "_" + table_name, 2); \
        upgrade_job_info.recordUpgradeStatus(database_name, table_name, NEXT_ENUM); \
        enum_it = NEXT_ENUM; \
        break;

#define CASE_WITH_STRUCT_LOCK(CURRENT_ENUM, NEXT_ENUM, UPGRADE_METHOD, ...) \
    case CURRENT_ENUM : \
        if (progress_enum == CURRENT_ENUM) \
        { \
            const auto storage = context.tryGetTable(database_name, table_name); \
            if (storage && !storage->is_dropped) \
            { \
                const auto storage_lock = storage->lockStructure(false, __PRETTY_FUNCTION__); \
                if (StorageQingCloud * upgrade_storage = dynamic_cast<StorageQingCloud *>(storage.get())) \
                    upgrade_storage->UPGRADE_METHOD (__VA_ARGS__); \
            } \
        } \
        safety_point_sync->broadcastSync(progressToString(CURRENT_ENUM) + "_" + database_name + "_" + table_name, 2); \
        upgrade_job_info.recordUpgradeStatus(database_name, table_name, NEXT_ENUM); \
        enum_it = NEXT_ENUM; \
        break;