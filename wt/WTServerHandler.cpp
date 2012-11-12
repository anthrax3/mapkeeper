/*
 * Copyright 2012 WiredTiger
 */
#include <arpa/inet.h>
#include <sstream>
#include <cerrno>
#include <stdio.h>
#include <boost/thread/tss.hpp>
#include <boost/thread/thread.hpp>
#include <server/TServer.h>
#include <server/TNonblockingServer.h>
#include <server/TThreadedServer.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/concurrency/ThreadManager.h>
#include "WTServerHandler.h"
#include "MapKeeper.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using boost::shared_ptr;

void WTServerHandler::
initEnv(const string& homeDir)  
{
    string config;
    config.assign("create,transactional,cache_size=2GB,sync=false,session_max=120");
    /* TODO: Set a configurable cache size? */
    printf("Opening WT at: %s\n", homeDir.c_str());
    WT_CONNECTION *conn;
    int rc = wiredtiger_open(homeDir.c_str(), NULL, config.c_str(), &conn);
    if (rc != 0)
        fprintf(stderr, "wiredtiger_open: %s\n", wiredtiger_strerror(rc));
    conn_ = conn;
}

WTServerHandler::
WTServerHandler() :
        wt_ (new boost::thread_specific_ptr<WT>(destroyWt))
{
    printf("Constructing new server handler\n");
}

void WTServerHandler::destroyWt(WT* wt) {
    //wt->close();
    delete wt;
}

int nanoSleep(uint64_t sleepTimeNs)
{
    struct timespec tv;
    tv.tv_sec = (time_t)(sleepTimeNs / (1000 * 1000 * 1000));
    tv.tv_nsec = (time_t)(sleepTimeNs % (1000 * 1000 * 1000));

    while (true) {
        int rval = nanosleep (&tv, &tv);
        if (rval == 0) {
            return 0;
        } else if (errno == EINTR) {
            continue;
        } else  {
            return rval;
        }
    }
    return 0;
}

void WTServerHandler::
checkpoint(uint32_t checkpointFrequencyMs)
{
    int rc = 0;
    WT_SESSION *sess;
    rc = conn_->open_session(conn_, NULL, NULL, &sess);
    while (true) {
        rc = sess->checkpoint(sess, NULL);
        if (rc != 0) {
            fprintf(stderr, "WT_SESSION::checkpoint error %s\n",
                wiredtiger_strerror(rc));
        }
        nanoSleep(checkpointFrequencyMs * 1000 *1000);
    }
    sess->close(sess, NULL);
}

void WTServerHandler::initWt()
{
    if (wt_->get()) {
        return;
    }
    WT *wt = new WT(conn_, "lsm:");
    wt_->reset(wt);

}

int WTServerHandler::
init(const string& homeDir,
     uint32_t checkpointFrequencyMs)
{
    printf("initializing\n");
    initEnv(homeDir);

    /* TODO: why?
    checkpointer_.reset(new boost::thread(&WTServerHandler::checkpoint, this,
                                          checkpointFrequencyMs));
    */
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
ping() 
{
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
addMap(const string& mapName) 
{
    initWt();
    WT::ResponseCode rc = wt_->get()->create(mapName, 128);
    if (rc == WT::DbExists) {
        return ResponseCode::MapExists;
    }
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
dropMap(const string& mapName) 
{
    initWt();
    wt_->get()->drop(mapName);
    return ResponseCode::Success;
}

void WTServerHandler::
listMaps(StringListResponse& _return) 
{
    initWt();

    wt_->get()->listTables(_return);
}

void WTServerHandler::
scan(RecordListResponse& _return,
        const string& mapName, const ScanOrder::type order, 
        const string& startKey, const bool startKeyIncluded,
        const string& endKey, const bool endKeyIncluded,
        const int32_t maxRecords, const int32_t maxBytes)
{
    initWt();
    wt_->get()->scanStart(
            mapName, order, startKey, startKeyIncluded, endKey, endKeyIncluded);

    int32_t resultSize = 0;
    _return.responseCode = ResponseCode::Success;
    while ((maxRecords == 0 ||
           (int32_t)(_return.records.size()) < maxRecords) && 
           (maxBytes == 0 || resultSize < maxBytes)) {
        Record rec;
        WT::ResponseCode rc = wt_->get()->scanNext(rec);
        if (rc == WT::ScanEnded) {
            _return.responseCode = ResponseCode::ScanEnded;
            break;
        } else if (rc != WT::Success) {
            _return.responseCode = ResponseCode::Error;
            break;
        }
        _return.records.push_back(rec);
        resultSize += rec.key.length() + rec.value.length();
    } 
    wt_->get()->scanEnd();
}

void WTServerHandler::
get(BinaryResponse& _return,
        const string& mapName, const string& recordName) 
{
    initWt();
    WT::ResponseCode dbrc = wt_->get()->get(mapName, recordName, _return.value);
    if (dbrc == WT::Success) {
        _return.responseCode = ResponseCode::Success;
    } else if (dbrc == WT::KeyNotFound) {
        _return.responseCode = ResponseCode::RecordNotFound;
    } else {
        _return.responseCode = ResponseCode::Error;
    }
}

ResponseCode::type WTServerHandler::
put(const string& mapName, 
       const string& recordName, 
       const string& recordBody) 
{
    /* TODO: What is difference between put and insert? */
    return insert(mapName, recordName, recordBody);
}

ResponseCode::type WTServerHandler::
insert(const string& mapName, 
       const string& recordName, 
       const string& recordBody) 
{
    initWt();
    boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
    WT::ResponseCode dbrc = wt_->get()->insert(mapName, recordName, recordBody);
    if (dbrc == WT::KeyExists) {
        return ResponseCode::RecordExists;
    } else if (dbrc != WT::Success) {
        return ResponseCode::Error;
    }
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
insertMany(const string& databaseName,
        const vector<Record> & records)
{
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
update(const string& mapName, 
       const string& recordName, 
       const string& recordBody) 
{
    initWt();
    WT::ResponseCode dbrc = wt_->get()->update(mapName, recordName, recordBody);
    if (dbrc == WT::Success) {
        return ResponseCode::Success;
    } else if (dbrc == WT::KeyNotFound) {
        return ResponseCode::RecordNotFound;
    } else {
        return ResponseCode::Error;
    }
}

ResponseCode::type WTServerHandler::
remove(const string& mapName, const string& recordName) 
{
    initWt();
    WT::ResponseCode dbrc = wt_->get()->remove(mapName, recordName);
    if (dbrc == WT::Success) {
        return ResponseCode::Success;
    } else if (dbrc == WT::KeyNotFound) {
        return ResponseCode::RecordNotFound;
    } else {
        return ResponseCode::Error;
    }
}

int main(int argc, char **argv) {
    int port = 9090;
    string homeDir = "data";
    uint32_t checkpointFrequencyMs = 1000;
    shared_ptr<WTServerHandler> handler(new WTServerHandler());
    handler->init(homeDir, checkpointFrequencyMs);
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(
            new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server (
        processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
