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

WT::
WT() :
    conn_(NULL),
    sess_(NULL),
    dbName_(""), 
    inited_(false)
{
}

WT::
~WT()
{
    close();
}

WT::ResponseCode WT::
create(WT_CONNECTION *conn, 
     const std::string& databaseName,
     uint32_t pageSizeKb,
     uint32_t numRetries)
{
    if (inited_) {
        fprintf(stderr, "Tried to open db %s but %s is already open",
            databaseName.c_str(), dbName_.c_str());
        return Error;
    }
    conn_ = conn;
    numRetries_ = numRetries;
    /* TODO: Add WT event handler. */
    if (sess_ == NULL) {
        WT_SESSION *sess;
        assert(0 == conn_->open_session(conn_, NULL, NULL, &sess));
        sess_ = sess;
    }
    std::stringstream config;
    config.str("");
    /* TODO: Exclusive create? */
    config << "key_format=S,value_format=S";
    config << ",internal_page_max=" << pageSizeKb * 1024;
    config << ",leaf_page_max=" << pageSizeKb * 1024;
    std::stringstream uri;
    uri.str("");
    uri << "lsm:" << databaseName;
    int rc = sess_->create(sess_, uri.str().c_str(), config.str().c_str());
    if (rc == EEXIST) {
        return DbExists;
    } else if (rc != 0) {
        // unexpected error
        fprintf(stderr, "WT_SESSION::create() returned: %s",
                wiredtiger_strerror(rc));
        return Error;
    }
    WT_CURSOR *curs;
    assert(0 == sess_->open_cursor(
                sess_, uri.str().c_str(), NULL, NULL, &curs));
    curs_ = curs;

    dbName_ = databaseName;
    uri_ = uri.str();
    inited_ = true;
    return Success;
}

WT::ResponseCode WT::
open(WT_CONNECTION *conn, 
     const std::string& databaseName,
     uint32_t pageSizeKb,
     uint32_t numRetries)
{
    if (inited_) {
        fprintf(stderr, "Tried to open db %s but %s is already open",
                databaseName.c_str(), dbName_.c_str());
        return Error;
    }
    conn_ = conn;
    numRetries_ = numRetries;
    /* TODO: Add WT event handler. */
    if (sess_ == NULL) {
        WT_SESSION *sess;
        assert(0 == conn_->open_session(conn_, NULL, NULL, &sess));
        sess_ = sess;
    }
    /* TODO: Check for existence? */
    std::stringstream uri;
    uri.str("");
    uri << "lsm:" << databaseName;
    uri_ = uri.str();
    WT_CURSOR *curs;
    assert(0 == sess_->open_cursor(
                sess_, uri.str().c_str(), NULL, NULL, &curs));
    curs_ = curs;
    dbName_ = databaseName;
    inited_ = true;
    return Success;
}

WT::ResponseCode WT::
close()
{
    printf("Closing WT object\n");
    if (!inited_) {
        return Error;
    }
    int rc = curs_->close(curs_);
    if (rc != 0)
        /* Report errors but keep going. */
        fprintf(stderr, "WT_CURSOR::close error %s\n", wiredtiger_strerror(rc));
    rc = sess_->close(sess_, NULL);
    if (rc == WT_DEADLOCK) {
        fprintf(stderr, "Txn aborted to avoid deadlock: %s",
                wiredtiger_strerror(rc));
        return Error;
    } else if (rc != 0) {
        // unexpected error
        fprintf(stderr, "WT_SESSION::close() returned: %s",
                wiredtiger_strerror(rc));
        return Error;
    }
    curs_ = NULL;
    sess_ = NULL;
    inited_ = false;
    return Success;
}

WT::ResponseCode WT::
drop()
{
    curs_->close(curs_);
    int rc = sess_->drop(sess_, uri_.c_str(), NULL);
    if (rc == ENOENT) {
    } else if (rc == WT_DEADLOCK) {
        fprintf(stderr, "Txn aborted to avoid deadlock: %s",
                wiredtiger_strerror(rc));
        return Error;
    } else if (rc != 0) {
        fprintf(stderr, "WT_SESSION::drop() returned: %s",
                wiredtiger_strerror(rc));
        return Error;
    }
    ResponseCode returnCode = close();
    if (returnCode != 0) {
        return returnCode;
    }
    return Success;
}

WT::ResponseCode WT::
get(const std::string& key, std::string& value)
{
    if (!inited_) {
        fprintf(stderr, "get called on uninitialized database");
        return Error;
    }

    int rc = 0;
    for (uint32_t idx = 0; idx < numRetries_; idx++) {
        curs_->set_key(curs_, key.c_str());
        rc = curs_->search(curs_);
        if (rc == 0) {
            const char *val;
            curs_->get_value(curs_, &val);
            value.assign(val);
            curs_->reset(curs_);
            return Success;
        } else if (rc == WT_NOTFOUND) {
            return KeyNotFound;
        } else if (rc != WT_DEADLOCK) {
            fprintf(stderr, "WT_CURSOR::search() returned: %s",
                    wiredtiger_strerror(rc));
            return Error;
        } 
    }
    fprintf(stderr, "get failed %d times", numRetries_);
    return Error;
}

WT::ResponseCode WT::
insert(const std::string& key, const std::string& value)
{
    if (!inited_) {
        fprintf(stderr, "insert called on uninitialized database");
        return Error;
    }

    int rc = 0;
    for (uint32_t idx = 0; idx < numRetries_; idx++) {
        curs_->set_key(curs_, key.c_str());
        curs_->set_value(curs_, value.c_str());
        curs_->insert(curs_);
        if (rc == 0) {
            return Success;
        } else if (rc == WT_DUPLICATE_KEY) {
            return KeyExists;
        } else if (rc != WT_DEADLOCK) {
            fprintf(stderr, "WT_CURSOR::insert() returned: %s",
                    wiredtiger_strerror(rc));
            return Error;
        }
    }
    fprintf(stderr, "insert failed %d times", numRetries_);
    return Error;
}

/**
 * Cursor must be closed before the transaction is aborted/commited.
 */
WT::ResponseCode WT::
update(const std::string& key, const std::string& value)
{
    if (!inited_) {
        fprintf(stderr, "insert called on uninitialized database");
        return Error;
    }

    int rc = 0;
    for (uint32_t idx = 0; idx < numRetries_; idx++) {
        curs_->set_key(curs_, key.c_str());
        curs_->set_value(curs_, value.c_str());
        curs_->update(curs_);

        if (rc == 0) {
            return Success;
        } else if (rc != WT_DEADLOCK) {
                fprintf(stderr, "WT_CURSOR::update() returned: %s",
                        wiredtiger_strerror(rc));
                return Error;
        }
    }
    fprintf(stderr, "update failed %d times", numRetries_);
    return Error;
}

WT::ResponseCode WT::
remove(const std::string& key)
{
    if (!inited_) {
        return Error;
    }

    int rc = 0;
    for (uint32_t idx = 0; idx < numRetries_; idx++) {
        curs_->set_key(curs_, key.c_str());
        curs_->remove(curs_);
        if (rc == 0) {
            return Success;
        } else if (rc == WT_NOTFOUND) {
            return KeyNotFound;
        } else if (rc != WT_DEADLOCK) {
            fprintf(stderr, "WT_CURSOR::remove() returned: %s",
                    wiredtiger_strerror(rc));
            return Error;
        }
    }
    fprintf(stderr, "update failed %d times", numRetries_);
    return Error;
}

WT::ResponseCode WT::scanStart(const ScanOrder::type order,
        const std::string& startKey, const bool startKeyIncluded,
        const std::string& endKey, const bool endKeyIncluded)
{
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
    if (!scanning_) {
        fprintf(stderr, "scanNext called when WT not setup for scan.\n");
        return Error;
    }
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
    else if (rc != 0) {
        fprintf(stderr, "scanNext failed with %s\n",
            wiredtiger_strerror(rc));
        return Error;
    }
    const char *key, *value;
    curs_->get_key(curs_, &key);
    curs_->get_value(curs_, &value);

    /* Check for terminating condition. */
    if (order_ == ScanOrder::Ascending) {
        int exact = std::string(key).compare(endKey_);
        if ((exact == 0 && !endKeyIncluded_) || exact > 0)
            return ScanEnded;
    } else { /* Descending */
        int exact = std::string(key).compare(startKey_);
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
    curs_->reset(curs_);
    scanning_ = false;
    scanSetup_ = false;

    return Success;
}

WT_SESSION* WT::
getSession() 
{
    return sess_;
}
