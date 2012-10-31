/*
 * Copyright 2012 WiredTiger
 */
#include <boost/thread.hpp>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <wiredtiger.h>
#include "MapKeeper.h"
#include "WT.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace mapkeeper;
using boost::shared_ptr;

class WTServerHandler : virtual public MapKeeperIf {
public:
    WTServerHandler();
    int init(const std::string& homeDir, 
             uint32_t numRetries,
             uint32_t checkpointFrequencyMs);
    ResponseCode::type ping();
    ResponseCode::type addMap(const std::string& databaseName);
    ResponseCode::type dropMap(const std::string& databaseName);
    void listMaps(StringListResponse& _return);
    void scan(RecordListResponse& _return,
            const std::string& databaseName, const ScanOrder::type order, 
            const std::string& startKey, const bool startKeyIncluded,
            const std::string& endKey, const bool endKeyIncluded,
            const int32_t maxRecords, const int32_t maxBytes);
    void get(BinaryResponse& _return,
            const std::string& databaseName, const std::string& recordName);
    ResponseCode::type put(const std::string& databaseName,
            const std::string& recordName, const std::string& recordBody);
    ResponseCode::type insert(const std::string& databaseName,
            const std::string& recordName, const std::string& recordBody);
    ResponseCode::type insertMany(const std::string& databaseName,
            const std::vector<Record> & records);
    ResponseCode::type update(const std::string& databaseName,
            const std::string& recordName, const std::string& recordBody);
    ResponseCode::type remove(const std::string& databaseName,
            const std::string& recordName);

private:
    void checkpoint(uint32_t checkpointFrequencyMs);
    void initEnv(const std::string& homeDir);
    WT_CONNECTION *conn_;
    boost::ptr_map<std::string, WT> maps_;
    boost::shared_mutex mutex_; // protect maps_
    boost::scoped_ptr<boost::thread> checkpointer_;
    static std::string DBNAME_PREFIX;
};
