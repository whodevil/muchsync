
#include <sstream>

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

#include <notmuch.h>
#include <openssl/rand.h>
#include <sqlite3.h>
#include <talloc.h>

#include "muchsync.h"

using namespace std;

#define DBVERS "muchsync 0"

void
dbthrow (sqlite3 *db, const char *query)
{
  const char *dbpath = sqlite3_db_filename (db, "main");
  if (!dbpath)
    dbpath = "sqlite3 database";
  ostringstream errbuf;
  if (query)
    errbuf << dbpath << ":\n  Query: " << query
	   << "\n  Error: " << sqlite3_errmsg (db);
  else
    errbuf << dbpath << ": " << sqlite3_errmsg (db);
  throw sqlerr_t (errbuf.str ());
}

void
sqlstmt_t::set_status (int status)
{
  status_ = status;
  if (status != SQLITE_OK && status != SQLITE_ROW && status != SQLITE_DONE)
    dbthrow (sqlite3_db_handle (stmt_), nullptr);
}

sqlstmt_t::sqlstmt_t (sqlite3 *db, const char *fmt, ...)
{
  char *query;
  const char *tail;
  va_list ap;

  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query)
    throw sqlerr_t ("sqlite3_vmprintf: out of memory");
  cleanup _c (bind (sqlite3_free, query));

  if (sqlite3_prepare_v2 (db, query, -1, &stmt_, &tail))
    dbthrow (db, query);
  if (tail && *tail) {
    fprintf (stderr, "fmtstep: illegal compound query\n  Query: %s\n", query);
    abort ();
  }
}

sqlstmt_t::sqlstmt_t (const sqldb_t &db, const char *fmt, ...)
{
  char *query;
  const char *tail;
  va_list ap;

  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query)
    throw sqlerr_t ("sqlite3_vmprintf: out of memory");
  cleanup _c (bind (sqlite3_free, query));

  if (sqlite3_prepare_v2 (db.get(), query, -1, &stmt_, &tail))
    dbthrow (db.get(), query);
  if (tail && *tail) {
    fprintf (stderr, "fmtstep: illegal compound query\n  Query: %s\n", query);
    abort ();
  }
}


int
fmtexec (sqlite3 *db, const char *fmt, ...)
{
  char *query;
  va_list ap;
  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query)
    return SQLITE_NOMEM;
  int err = sqlite3_exec (db, query, NULL, NULL, NULL);
  if (err)
    dbthrow (db, query);
  sqlite3_free (query);
  return err;
}

sqlstmt_t
fmtstmt (sqlite3 *db, const char *fmt, ...)
{
  char *query;
  const char *tail;
  sqlite3_stmt *stmtp;
  int err;
  va_list ap;

  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query) {
    fprintf (stderr, "sqlite3_vmprintf(%s): out of memory\n", fmt);
    return sqlstmt_t (nullptr);
  }

  if (sqlite3_prepare_v2 (db, query, -1, &stmtp, &tail)) {
    dbthrow (db, query);
    return sqlstmt_t (nullptr);
  }
  if (tail && *tail) {
    fprintf (stderr, "fmtstep: illegal compound query\n  Query: %s\n", query);
    abort ();
  }
  return sqlstmt_t (stmtp);
}

int
fmtstep (sqlite3 *db, sqlite3_stmt **stmtpp, const char *fmt, ...)
{
  char *query;
  const char *tail;
  sqlite3_stmt *stmtp = NULL;
  int err;
  va_list ap;

  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query) {
    if (stmtpp)
      *stmtpp = NULL;
    fprintf (stderr, "sqlite3_vmprintf: out of memory\n");
    return SQLITE_NOMEM;
  }

  err = sqlite3_prepare_v2 (db, query, -1, &stmtp, &tail);
  if (!err && tail && *tail) {
    fprintf (stderr, "fmtstep: illegal compound query\n  Query: %s\n", query);
    abort ();
  }
  if (!err)
    err = sqlite3_step (stmtp);

  if (err != SQLITE_ROW) {
    sqlite3_finalize (stmtp);
    stmtp = NULL;
  }
  if (stmtpp)
    *stmtpp = stmtp;

  if (err != SQLITE_OK && err != SQLITE_ROW && err != SQLITE_DONE)
    dbthrow (db, query);
  sqlite3_free (query);
  return err;
}

void
save_old_table (sqlite3 *sqldb, const string &table, const char *create)
{
  fmtexec (sqldb, "%s", create);
  fmtexec (sqldb, "DROP TABLE IF EXISTS \"old_%s\";"
		  "ALTER TABLE \"%s\" RENAME TO \"old_%s\";",
	   table.c_str(), table.c_str(), table.c_str());
  fmtexec (sqldb, "%s", create);
}


char *
getconfig_text (void *ctx, sqlite3 *db, const char *key)
{
  sqlite3_stmt *pStmt;
  if (fmtstep (db, &pStmt,
	       "SELECT value FROM configuration WHERE key = %Q;", key)
      != SQLITE_ROW)
    return NULL;
  const char *value = (const char *) sqlite3_column_text (pStmt, 0);
  char *ret = value ? talloc_strdup (ctx, value) : NULL;
  sqlite3_finalize (pStmt);
  return ret;
}

int
getconfig_int64 (sqlite3 *db, const char *key, i64 *valp)
{
  sqlite3_stmt *pStmt;
  int err = fmtstep (db, &pStmt,
		     "SELECT value FROM configuration WHERE key = %Q;", key);
  if (err == SQLITE_ROW) {
    *valp = sqlite3_column_int64 (pStmt, 0);
    sqlite3_finalize (pStmt);
    return SQLITE_OK;
  }
  assert (err != SQLITE_OK); /* should be ROW or DONE if no error */
  return err;
}

/* type should be Q for strings, lld for i64s, f for double */
#define setconfig_type(db, key, type, val)				\
  fmtexec(db,							\
	  "INSERT OR REPLACE INTO configuration (key, value)"	\
          "VALUES (%Q, %" #type ");", key, val)
#define setconfig_int64(db, key, val) setconfig_type(db, key, lld, val)
#define setconfig_text(db, key, val) setconfig_type(db, key, Q, val)

static const char files_def[] = "\
CREATE TABLE files (message_id TEXT,\
 path TEXT UNIQUE NOT NULL,\
 size INTEGER,\
 sha1 BLOB,\
 mtime REAL,\
 ctime REAL);";

sqlite3 *
dbcreate (const char *path)
{
  static const char extra_defs[] = "\
CREATE TABLE sync_vector (replica INTEGER PRIMARY KEY, version INTEGER);\n\
CREATE TABLE configuration (key TEXT PRIMARY KEY NOT NULL, value TEXT);";

  i64 self;
  if (RAND_pseudo_bytes ((unsigned char *) &self, sizeof (self)) == -1) {
    fprintf (stderr, "RAND_pseudo_bytes failed\n");
    return NULL;
  }
  self &= ~((i64) 1 << 63);

  sqlite3 *pDb = NULL;
  int err = sqlite3_open_v2 (path, &pDb,
			     SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL);
  if (err) {
    fprintf (stderr, "%s: %s\n", path, sqlite3_errstr (err));
    return NULL;
  }

  if (fmtexec (pDb, "BEGIN;")
      // || fmtexec (pDb, files_def)
      || fmtexec (pDb, extra_defs)
      || setconfig_text (pDb, "DBVERS", DBVERS)
      || setconfig_int64 (pDb, "self", self)
      || fmtexec (pDb, "INSERT INTO sync_vector (replica, version)"
		  " VALUES (%lld, 0);", self)
      || fmtexec (pDb, "COMMIT;")) {
    sqlite3_close_v2 (pDb);
    return NULL;
  }
  return pDb;
}

sqlite3 *
dbopen (const char *path)
{
  sqlite3 *pDb = NULL;
  if (access (path, 0) && errno == ENOENT)
    pDb = dbcreate (path);
  else
    sqlite3_open_v2 (path, &pDb, SQLITE_OPEN_READWRITE, NULL);
  if (!pDb)
    return NULL;

  return pDb;
}


