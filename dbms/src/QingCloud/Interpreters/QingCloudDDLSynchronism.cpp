//#include <QingCloud/Interpreters/QingCloudDDLSynchronism.h>
//#include <DataStreams/RemoteBlockInputStream.h>
//#include <DataStreams/SquashingBlockInputStream.h>
//#include <Common/typeid_cast.h>
//#include <Poco/File.h>
//#include <Columns/ColumnsNumber.h>
//#include "QingCloudDDLSynchronism.h"
//
//
//namespace DB
//{
//
//QingCloudDDLSynchronism::QingCloudDDLSynchronism(QingCloudDDLSynchronism::AddressesWithConnections & addresses_with_connections)
//{
//    updateAddressesAndConnections(addresses_with_connections);
//}
//
//void QingCloudDDLSynchronism::updateAddressesAndConnections(QingCloudDDLSynchronism::AddressesWithConnections &addresses_with_connections)
//{
//    connections.resize(addresses_with_connections.size());
//    for (auto &addresses_with_connection : addresses_with_connections)
//        connections.emplace_back(addresses_with_connection.second);
//}
//
//static Block getBlockWithAllStreamData(const BlockInputStreamPtr &stream)
//{
//    auto res = std::make_shared<SquashingBlockInputStream>(stream, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max());
//    return res->read();
//}
//
//UInt64 QingCloudDDLSynchronism::applyQuorum(const Context &context)
//{
////    Block header;
////    Settings settings = context.getSettingsRef();
////
////    while (true)
////    {
////        UInt64 local_seq_id = 0;
////        size_t accepted_number = 0;
////        for (const auto & node_connections : connections)
////        {
////            auto connection = node_connections->get(&settings);
////            String query = "QingCloud Paxos APPLY " + toString(local_seq_id);
////            Block res = getBlockWithAllStreamData(std::make_shared<RemoteBlockInputStream>(*connection, query, header, context, &settings));
////            UInt64 accepted = typeid_cast<const ColumnUInt64 &>(*res.safeGetByPosition(0).column).getUInt(0);
////
////            if (accepted == local_seq_id)
////                accepted_number++;
////        }
////
////        if (accepted_number >= (connections.size() / 2 + 1))
////            return local_seq_id;
//    }
//}
//
//void QingCloudDDLSynchronism::enqueue(const String & query_string, const Context & context)
//{
//    /// TODO:申请全局id
//    UInt64 global_seq_id = applyQuorum(context);
//    /// TODO:将query_string保存在本地存储中
//    /// TODO:通知其他节点进行query_string的同步
//    notifySyncQueryLogForAllServer();
//    /// TODO:等待其他节点推送结果后组装结果返回即可
//}
//
//}