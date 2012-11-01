/*
 * Copyright 2012 WiredTiger
 */
#include <cerrno> // ENOENT
#include <arpa/inet.h> // ntohl
#include <iomanip>
#include <sstream>
#include <boost/thread/tss.hpp>
#include "WT.h"

using namespace mapkeeper;
using namespace std;

const string WT::Name2Uri(const string& tableName)
{
    string uri(tableType_);
    uri.append(tableName);
    return uri;
}

WT::ResponseCode WT::
openCursor(const string& tableName)
{
    if (curs_ != NULL)
        ERROR_RET(Error, 0,
            "Cannot execute operations in parallel.\n");

    map<string, WT_CURSOR *>::iterator itr =
        cursors_.find(tableName);
    if (itr != cursors_.end()) {
        curs_ = itr->second;
        return Success;
    }
    /* We don't have a cached cursor, open one and add it to the cache. */
    WT_CURSOR *curs;
    int rc = sess_->open_cursor(
        sess_, Name2Uri(tableName).c_str(), NULL, NULL, &curs);
    if (rc != 0)
        ERROR_RET(Error, rc, "Error opening cursor.\n");
    cursors_[tableName] = curs;
    curs_ = curs;
    return Success;
}

void WT::closeCursor()
{
    curs_->reset(curs_);
    curs_ = NULL;
}

WT::
WT(WT_CONNECTION *conn, const string& tableType) :
    conn_(conn),
    curs_(NULL),
    tableType_(tableType)
{
    /* TODO: Add WT event handler. */
    WT_SESSION *sess;
    assert(0 == conn_->open_session(conn_, NULL, NULL, &sess));
    sess_ = sess;
    printf("Creating new WT object.\n");
}

WT::
~WT()
{
    close();
}

WT::ResponseCode WT::
create(const string& tableName, uint32_t pageSizeKb)
{
    stringstream config;
    config.str("");
    /* TODO: Exclusive create? */
    config << "key_format=S,value_format=S";
    config << ",internal_page_max=" << pageSizeKb * 1024;
    config << ",leaf_page_max=" << pageSizeKb * 1024;
    config << ",lsm_chunk_size=20MB";
    int rc = sess_->create(
            sess_, Name2Uri(tableName).c_str(), config.str().c_str());
    if (rc == EEXIST)
        return DbExists;
    else if (rc != 0)
        // unexpected error
        ERROR_RET(Error, rc, "WT_SESSION::create() failed.");
    return Success;
}

WT::ResponseCode WT::
open(const string& tableName)
{
    /* TODO: Check for existence? */
    return Success;
}

#define WT_METADATA_URI "file:WiredTiger.wt"
WT::ResponseCode WT::
listTables(StringListResponse &_return)
{
    WT_CURSOR *cursor;
    ResponseCode ret = Success;
    int rc = 0;
    _return.values.clear();
    /* Open the metadata file. */
    if ((rc = sess_->open_cursor(
        sess_, WT_METADATA_URI, NULL, NULL, &cursor)) != 0) {
        // If there is no metadata treat it the same as an empty metadata.
        if (ret == ENOENT)
            return Success;

        _return.responseCode = mapkeeper::ResponseCode::Error;
        ERROR_RET(Error, rc, "WT::listMaps cursor open");
    }

    const char *key;
    while ((rc = cursor->next(cursor)) == 0) {
        /* Get the key. */
        if ((rc = cursor->get_key(cursor, &key)) != 0)
            ERROR_GOTO(Error, rc, "WT::listMaps metadata get.");

        _return.values.push_back(string(key));
    }
error:
    cursor->close(cursor);
    if (ret != Success)
        _return.responseCode = mapkeeper::ResponseCode::Error;
    return ret;
}

WT::ResponseCode WT::
close()
{
    printf("Closing WT object\n");
    /* No need to go through and close the cursors - session close does it. */
    if (sess_ == NULL)
        return Success; /* It's already closed. */
    int rc = sess_->close(sess_, NULL);
    if (rc != 0)
        ERROR_RET(Error, 0, "WT_SESSION::close() failed.");
    sess_ = NULL;
    curs_ = NULL;
    return Success;
}

WT::ResponseCode WT::
drop(const string& tableName)
{
    int rc = sess_->drop(sess_, Name2Uri(tableName).c_str(), NULL);
    if (rc != 0 && rc != ENOENT)
        ERROR_RET(Error, rc, "WT_SESSION::drop() failed.");
    return Success;
}

WT::ResponseCode WT::
get(const string& tableName, const string& key, string& value)
{
    ResponseCode ret = Success;
    int rc = 0;
    if ((ret = openCursor(tableName)) != Success)
        ERROR_RET(ret, 0, "WT::get failed to open cursor\n");

    curs_->set_key(curs_, key.c_str());
    rc = curs_->search(curs_);
    if (rc == 0) {
        const char *val;
        curs_->get_value(curs_, &val);
        value.assign(val);
        curs_->reset(curs_);
        ret = Success;
    } else if (rc == WT_NOTFOUND)
        ret = KeyNotFound;
    else
        ERROR_GOTO(Error, rc, "WT::insert operation failed\n");
error:
    closeCursor();
    return ret;
}

WT::ResponseCode WT::
insert(const string& tableName,
    const string& key, const string& value)
{
    ResponseCode ret = Success;
    int rc = 0;
    if ((ret = openCursor(tableName)) != Success)
        ERROR_RET(ret, 0, "WT::insert failed to open cursor\n");

    curs_->set_key(curs_, key.c_str());
    curs_->set_value(curs_, value.c_str());
    rc = curs_->insert(curs_);
    if (rc == WT_DUPLICATE_KEY)
        ret = KeyExists;
    else if (rc != 0)
        ERROR_GOTO(Error, rc, "WT::insert operation failed\n");
error:
    closeCursor();
    return ret;
}

/**
 * Cursor must be closed before the transaction is aborted/commited.
 */
WT::ResponseCode WT::
update(const string& tableName,
    const string& key, const string& value)
{
    ResponseCode ret = Success;
    int rc = 0;
    if ((ret = openCursor(tableName)) != Success)
        ERROR_RET(ret, 0, "WT::update failed to open cursor\n");

    curs_->set_key(curs_, key.c_str());
    curs_->set_value(curs_, value.c_str());
    rc = curs_->update(curs_);

    if (rc != 0)
        ERROR_GOTO(Error, rc, "WT::update operation failed\n");
error:
    closeCursor();
    return ret;
}

WT::ResponseCode WT::
remove(const string& tableName, const string& key)
{
    ResponseCode ret = Success;
    int rc = 0;
    if ((ret = openCursor(tableName)) != Success)
        ERROR_RET(ret, 0, "WT::get failed to open cursor\n");

    curs_->set_key(curs_, key.c_str());
    rc = curs_->remove(curs_);
    if (rc == WT_NOTFOUND)
        ret = KeyNotFound;
    else if (rc != 0)
        ERROR_GOTO(Error, rc, "WT::remove operation failed\n");
error:
    closeCursor();
    return ret;
}

WT::ResponseCode WT::scanStart(const string &tableName,
        const ScanOrder::type order,
        const string& startKey, const bool startKeyIncluded,
        const string& endKey, const bool endKeyIncluded)
{
    ResponseCode ret = Success;
    if ((ret = openCursor(tableName)) != Success)
        ERROR_RET(ret, 0, "WT::scanStart failed to open cursor\n");

    scanning_ = true;
    scanSetup_ = false;
    order_ = order;
    startKey_ = startKey;
    startKeyIncluded_ = startKeyIncluded;
    endKey_ = endKey;
    endKeyIncluded_ = endKeyIncluded;
    curs_->set_key(curs_, startKey.c_str());

    return Success;
}

WT::ResponseCode WT::scanNext(Record &rec)
{
    if (!scanning_)
        ERROR_RET(Error, 0,
            "WT::scanNext called when WT not setup for scan.\n");

    int rc = 0;
    if (!scanSetup_) {
        if (order_ == ScanOrder::Ascending && startKey_.empty())
            curs_->next(curs_);
        else if (order_ == ScanOrder::Descending && endKey_.empty())
            curs_->prev(curs_);
        int exact;
        curs_->search_near(curs_, &exact);
        if (exact < 0 && order_ == ScanOrder::Ascending)
            rc = curs_->next(curs_);
        else if (exact > 0 && order_ == ScanOrder::Descending)
            rc = curs_->prev(curs_);
        else if (exact == 0 &&
            order_ == ScanOrder::Ascending && !startKeyIncluded_)
            rc = curs_->next(curs_);
        else if (exact == 0 &&
            order_ == ScanOrder::Descending && !endKeyIncluded_)
            rc = curs_->prev(curs_);
        scanSetup_ = true;
    } else {
        if (order_ == ScanOrder::Ascending)
            rc = curs_->next(curs_);
        else
            rc = curs_->prev(curs_);
    }
    if (rc == WT_NOTFOUND &&
        ((order_ == ScanOrder::Ascending && endKey_.empty()) ||
        (order_ == ScanOrder::Descending && startKey_.empty())))
        return ScanEnded;
    else if (rc == WT_NOTFOUND)
        return (KeyNotFound);
    else if (rc != 0)
        ERROR_RET(Error, rc, "WT::scanNext error.");
    const char *key, *value;
    curs_->get_key(curs_, &key);
    curs_->get_value(curs_, &value);

    /* Check for terminating condition. */
    if (order_ == ScanOrder::Ascending) {
        int exact = string(key).compare(endKey_);
        if ((exact == 0 && !endKeyIncluded_) || exact > 0)
            return ScanEnded;
    } else { /* Descending */
        int exact = string(key).compare(startKey_);
        if ((exact == 0 && !startKeyIncluded_) || exact < 0)
            return ScanEnded;
    }
    /* Copy out the key/value pair. */
    rec.key.assign(key);
    rec.value.assign(value);
    return Success;
}

WT::ResponseCode WT::scanEnd()
{
    closeCursor();
    scanning_ = false;
    scanSetup_ = false;

    return Success;
}

WT_SESSION* WT::
getSession() 
{
    return sess_;
}
