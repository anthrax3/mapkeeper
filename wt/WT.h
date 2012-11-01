/*
 * Copyright 2012 WiredTiger
 */
#ifndef WT_H
#define WT_H

#include <map>
#include <wiredtiger.h>
#include "MapKeeper.h"

#define ERROR_RET_PRINT(ret_, rc_, ...) do {    \
    fprintf(stderr, __VA_ARGS__);               \
    if (rc_ != 0)                               \
        fprintf(stderr, "%s\n", wiredtiger_strerror(rc_));  \
    else                                        \
        fprintf(stderr, "\n");                  \
} while(0)
#define ERROR_RET(ret_, rc_, ...) do {          \
    ERROR_RET_PRINT(ret_, rc_, __VA_ARGS__);    \
    return (ret_);                              \
} while (0)
#define ERROR_RET_VOID(rc_, ...) do {          \
    ERROR_RET_PRINT(0, rc_, __VA_ARGS__);      \
    return;                                    \
} while (0)
#define ERROR_GOTO(ret_, rc_, ...) do {         \
    ERROR_RET_PRINT(ret_, rc_, __VA_ARGS__);    \
    ret = ret_;                                 \
    goto error;                                 \
} while (0)

using namespace std;

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

    WT(WT_CONNECTION *conn, const string& tableType);

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
    ResponseCode create(const string& tableName, uint32_t pageSizeKb);

    /**
     * Opens a database.
     *
     * @returns Success on success
     *          DbNotFound if the database doesn't exist.
     */
    ResponseCode open(const string& tableName);

    ResponseCode listTables(mapkeeper::StringListResponse& _return);

    ResponseCode close();
    ResponseCode drop(const string& tableName);
    ResponseCode get(const string& tableName,
            const string& key, string& value);
    ResponseCode insert(const string& tableName,
            const string& key, const string& value);
    ResponseCode update(const string& tableName,
            const string& key, const string& value);
    ResponseCode remove(const string& tableName,
            const string& key);
    WT_SESSION* getSession();

    /* APIs for iteration. */
    ResponseCode scanStart(const string& tableName,
            const mapkeeper::ScanOrder::type order,
            const string& startKey, const bool startKeyIncluded,
            const string& endKey, const bool endKeyIncluded);
    ResponseCode scanNext(mapkeeper::Record &rec);
    ResponseCode scanEnd();

private:
    const string Name2Uri(const string& tableName);
    ResponseCode openCursor(const string& tableName);
    void closeCursor();

    WT_CONNECTION *conn_;
    WT_SESSION *sess_;
    map<string, WT_CURSOR *> cursors_;
    WT_CURSOR *curs_;
    const string tableType_;
    /* Variables tracking a scan (iteration). */
    bool scanning_;
    bool scanSetup_;
    mapkeeper::ScanOrder::type order_;
    string startKey_;
    string endKey_;
    bool startKeyIncluded_;
    bool endKeyIncluded_;
};

#endif // WT_H
