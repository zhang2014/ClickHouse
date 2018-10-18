//#pragma once
//
//#include <Poco/File.h>
//#include <Interpreters/Cluster.h>
//
//namespace DB
//{
//
//class QingCloudDDLSynchronism
//{
//private:
//    using AddressesWithConnections = std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>>;
//public:
//    QingCloudDDLSynchronism(AddressesWithConnections & addresses_with_connections);
//
//    void updateAddressesAndConnections(AddressesWithConnections &addresses_with_connections);
//
//    void enqueue(const String & query_string);
//private:
//    Poco::File increment;
//    std::vector<ConnectionPoolPtr> connections;
//
//    UInt64 applyQuorum(const Context &context);
//};
//
//using QingCloudDDLSynchronismPtr = std::shared_ptr<QingCloudDDLSynchronism>;
//
//}