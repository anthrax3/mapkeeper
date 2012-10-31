/*
 * Copyright 2012 WiredTiger
 */
#include <arpa/inet.h>
#include <sstream>
#include <cerrno>
#include <dirent.h>
#include <endian.h>
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

std::string WTServerHandler::DBNAME_PREFIX = "mapkeeper_";

void WTServerHandler::
initEnv(const std::string& homeDir)  
{
    std::string config;
    config.assign("create,transactional");
    /* TODO: Set a configurable cache size? */
    printf("Opening WT at: %s\n", homeDir.c_str());
    WT_CONNECTION *conn;
    int rc = wiredtiger_open(homeDir.c_str(), NULL, config.c_str(), &conn);
    if (rc != 0)
        fprintf(stderr, "wiredtiger_open: %s\n", wiredtiger_strerror(rc));
    conn_ = conn;
}

WTServerHandler::
WTServerHandler()
{
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

int WTServerHandler::
init(const std::string& homeDir,
     uint32_t numRetries,
     uint32_t checkpointFrequencyMs)
{
    printf("initializing\n");
    initEnv(homeDir);

    StringListResponse maps;
    listMaps(maps);
    boost::unique_lock<boost::shared_mutex> writeLock(mutex_);;
    for (std::vector<std::string>::iterator itr = maps.values.begin();
         itr != maps.values.end(); itr++) {
        std::string dbName = DBNAME_PREFIX + *itr;
        fprintf(stderr, "opening db: %s\n", dbName.c_str());
        WT* db = new WT();
        WT::ResponseCode rc = db->open(conn_, dbName, 128, 100);
        if (rc == WT::DbNotFound) {
            delete db;
            fprintf(stderr, "failed to open db: %s\n", dbName.c_str());
            return ResponseCode::MapNotFound;
        }
        maps_.insert(*itr, db);

    }
    checkpointer_.reset(new boost::thread(&WTServerHandler::checkpoint, this,
                                          checkpointFrequencyMs));
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
ping() 
{
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
addMap(const std::string& mapName) 
{
    boost::unique_lock<boost::shared_mutex> writeLock(mutex_);;
    std::string dbName = DBNAME_PREFIX + mapName;
    WT* db = new WT();
    WT::ResponseCode rc = db->create(conn_, dbName, 128, 100);
    if (rc == WT::DbExists) {
        delete db;
        return ResponseCode::MapExists;
    }
    std::string mapName_ = mapName;
    maps_.insert(mapName_, db);
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
dropMap(const std::string& mapName) 
{
    boost::unique_lock<boost::shared_mutex> writeLock(mutex_);;
    std::string dbName = DBNAME_PREFIX + mapName;
    boost::ptr_map<std::string, WT>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
    itr->second->drop();
    maps_.erase(itr);
    return ResponseCode::Success;
}

void WTServerHandler::
listMaps(StringListResponse& _return) 
{
    DIR *dp;
    struct dirent *dirp;
    const char* homeDir = conn_->get_home(conn_);
    if((dp = opendir(homeDir)) == NULL) {
        _return.responseCode = ResponseCode::Success;
        return;
    }

    while ((dirp = readdir(dp)) != NULL) {
        std::string fileName(dirp->d_name);
        if (fileName.find(DBNAME_PREFIX) == 0) {
            _return.values.push_back(fileName.substr(DBNAME_PREFIX.size()));
        }
    }
    closedir(dp);
    _return.responseCode = ResponseCode::Success;
}

void WTServerHandler::
scan(RecordListResponse& _return,
        const std::string& mapName, const ScanOrder::type order, 
        const std::string& startKey, const bool startKeyIncluded,
        const std::string& endKey, const bool endKeyIncluded,
        const int32_t maxRecords, const int32_t maxBytes)
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, WT>::iterator mapItr = maps_.find(mapName);
    if (mapItr == maps_.end()) {
        _return.responseCode = ResponseCode::MapNotFound;
        return;
    }
 
    WT *wt = mapItr->second;
    wt->scanStart(order, startKey, startKeyIncluded, endKey, endKeyIncluded);

    int32_t resultSize = 0;
    _return.responseCode = ResponseCode::Success;
    while ((maxRecords == 0 ||
           (int32_t)(_return.records.size()) < maxRecords) && 
           (maxBytes == 0 || resultSize < maxBytes)) {
        Record rec;
        WT::ResponseCode rc = wt->scanNext(rec);
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
    wt->scanEnd();
}

void WTServerHandler::
get(BinaryResponse& _return,
        const std::string& mapName, const std::string& recordName) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, WT>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        _return.responseCode = ResponseCode::MapNotFound;
        return;
    }
    WT::ResponseCode dbrc = itr->second->get(recordName, _return.value);
    if (dbrc == WT::Success) {
        _return.responseCode = ResponseCode::Success;
    } else if (dbrc == WT::KeyNotFound) {
        _return.responseCode = ResponseCode::RecordNotFound;
    } else {
        _return.responseCode = ResponseCode::Error;
    }
}

ResponseCode::type WTServerHandler::
put(const std::string& mapName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, WT>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
    WT::ResponseCode dbrc = itr->second->insert(recordName, recordBody);
    if (dbrc == WT::KeyExists) {
        return ResponseCode::RecordExists;
    } else if (dbrc != WT::Success) {
        return ResponseCode::Error;
    }
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
insert(const std::string& mapName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, WT>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
 
    WT::ResponseCode dbrc = itr->second->insert(recordName, recordBody);
    if (dbrc == WT::KeyExists) {
        return ResponseCode::RecordExists;
    } else if (dbrc != WT::Success) {
        return ResponseCode::Error;
    }
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
insertMany(const std::string& databaseName,
        const std::vector<Record> & records)
{
    return ResponseCode::Success;
}

ResponseCode::type WTServerHandler::
update(const std::string& mapName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, WT>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
 
    WT::ResponseCode dbrc = itr->second->update(recordName, recordBody);
    if (dbrc == WT::Success) {
        return ResponseCode::Success;
    } else if (dbrc == WT::KeyNotFound) {
        return ResponseCode::RecordNotFound;
    } else {
        return ResponseCode::Error;
    }
}

ResponseCode::type WTServerHandler::
remove(const std::string& mapName, const std::string& recordName) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, WT>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
    WT::ResponseCode dbrc = itr->second->remove(recordName);
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
    std::string homeDir = "data";
    uint32_t numRetries = 100;
    uint32_t checkpointFrequencyMs = 1000;
    uint32_t numThreads = 16;
    shared_ptr<WTServerHandler> handler(new WTServerHandler());
    handler->init(homeDir, numRetries, 
    checkpointFrequencyMs);
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(
            new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    shared_ptr<ThreadManager> threadManager =
        ThreadManager::newSimpleThreadManager(numThreads);
    shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    TServer* server = NULL;
    /*
     * TODO: Since we are implementing a threaded server, can we stop using
     * the maps mutex?
     */
    server = new TNonblockingServer(
            processor, protocolFactory, port, threadManager);
    server->serve();
    return 0;
}
