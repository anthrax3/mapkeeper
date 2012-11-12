/*
 * Copyright 2012 WiredTiger
 */
#include <boost/thread.hpp>
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
using namespace std;

class WTServerHandler : virtual public MapKeeperIf {
public:
    WTServerHandler();
    int init(const string& homeDir, 
             uint32_t checkpointFrequencyMs);
    ResponseCode::type ping();
    ResponseCode::type addMap(const string& databaseName);
    ResponseCode::type dropMap(const string& databaseName);
    void listMaps(StringListResponse& _return);
    void scan(RecordListResponse& _return,
            const string& databaseName, const ScanOrder::type order, 
            const string& startKey, const bool startKeyIncluded,
            const string& endKey, const bool endKeyIncluded,
            const int32_t maxRecords, const int32_t maxBytes);
    void get(BinaryResponse& _return,
            const string& databaseName, const string& recordName);
    ResponseCode::type put(const string& databaseName,
            const string& recordName, const string& recordBody);
    ResponseCode::type insert(const string& databaseName,
            const string& recordName, const string& recordBody);
    ResponseCode::type insertMany(const string& databaseName,
            const vector<Record> & records);
    ResponseCode::type update(const string& databaseName,
            const string& recordName, const string& recordBody);
    ResponseCode::type remove(const string& databaseName,
            const string& recordName);
    static void destroyWt(WT* wt);
    void initWt();

private:
    void checkpoint(uint32_t checkpointFrequencyMs);
    void initEnv(const string& homeDir);
    WT_CONNECTION *conn_;
    boost::thread_specific_ptr<WT>* wt_;
    boost::scoped_ptr<boost::thread> checkpointer_;
    /* Single thread updates with the mutex. */
    boost::shared_mutex mutex_;
};
