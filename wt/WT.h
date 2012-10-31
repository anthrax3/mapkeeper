/*
 * Copyright 2012 WiredTiger
 */
#ifndef WT_H
#define WT_H

#include <wiredtiger.h>
#include "MapKeeper.h"

class WT {
public:
    enum ResponseCode {
        Success = 0,
        Error,
        KeyExists,
        KeyNotFound,
        DbExists,
        DbNotFound,
        ScanEnded
    };

    WT();

    /**
     * Destructor. It'll close the database if it's open.
     */
    ~WT();

    /**
     * Create a database.
     *
     * @returns Success on success
     *          DbExists if the database already exists.
     */
    ResponseCode create(WT_CONNECTION *conn, 
                      const std::string& databaseName,
                      uint32_t pageSizeKb,
                      uint32_t numRetries);

    /**
     * Opens a database.
     *
     * @returns Success on success
     *          DbNotFound if the database doesn't exist.
     */
    ResponseCode open(WT_CONNECTION *conn, 
                      const std::string& databaseName,
                      uint32_t pageSizeKb,
                      uint32_t numRetries);

    ResponseCode close();
    ResponseCode drop();
    ResponseCode get(const std::string& key, std::string& value);
    ResponseCode insert(const std::string& key, const std::string& value);
    ResponseCode update(const std::string& key, const std::string& value);
    ResponseCode remove(const std::string& key);
    WT_SESSION* getSession();

    /* APIs for iteration. */
    ResponseCode scanStart(const mapkeeper::ScanOrder::type order,
            const std::string& startKey, const bool startKeyIncluded,
            const std::string& endKey, const bool endKeyIncluded);
    ResponseCode scanNext(mapkeeper::Record &rec);
    ResponseCode scanEnd();

private:
    WT_CONNECTION *conn_;
    WT_SESSION *sess_;
    WT_CURSOR *curs_;
    std::string uri_;
    std::string dbName_;
    bool inited_;
    uint32_t numRetries_;
    /* Variables tracking a scan (iteration). */
    bool scanning_;
    bool scanSetup_;
    mapkeeper::ScanOrder::type order_;
    std::string startKey_;
    std::string endKey_;
    bool startKeyIncluded_;
    bool endKeyIncluded_;
};

#endif // WT_H
