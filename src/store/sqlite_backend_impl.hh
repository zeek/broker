#ifndef BROKER_STORE_SQLITE_BACKEND_IMPL_HH
#define BROKER_STORE_SQLITE_BACKEND_IMPL_HH

#include "broker/store/sqlite_backend.hh"
#include "sqlite3.h"
#include <functional>

namespace broker {
namespace store {

#define STMT_INSERT "replace into store(k, v, expiry) values(?, ?, ?);"
#define STMT_UPDATE "update store set v = ? where k = ?;"
#define STMT_UPDATE_EXPIRY "update store set v = ?, expiry = ? where k = ?;"
#define STMT_ERASE "delete from store where k = ?;"
#define STMT_EXPIRE "delete from store where k = ? and expiry = ?;"
#define STMT_CLEAR "delete from store;"
#define STMT_LOOKUP "select v from store where k = ?;"
#define STMT_LOOKUP_EXPIRY "select v, expiry from store where k = ?;"
#define STMT_EXISTS "select 1 from store where k = ?;"
#define STMT_KEYS "select k from store;"
#define STMT_SIZE "select count(*) from store;"
#define STMT_SNAP "select k, v, expiry from store;"
#define STMT_EXPIRIES "select k, expiry from store where expiry is not null;"

struct stmt_guard {

	stmt_guard() = default;

	stmt_guard(sqlite3_stmt* arg_stmt, std::function<int(sqlite3_stmt*)> arg_f)
		: stmt(arg_stmt), func(std::move(arg_f))
		{}

	stmt_guard(stmt_guard&) = delete;

	stmt_guard(stmt_guard&& other)
		: stmt(other.stmt), func(std::move(other.func))
		{ other.func = {}; }

	stmt_guard& operator=(stmt_guard&) = delete;

	stmt_guard& operator=(stmt_guard&& other)
		{
		stmt = other.stmt;
		func = std::move(other.func);
		other.func = {};
		return *this;
		}

	~stmt_guard()
		{ if ( func ) func(stmt); }

	sqlite3_stmt* stmt = nullptr;
	std::function<int(sqlite3_stmt*)> func;
};

struct sqlite_stmt {

	sqlite_stmt(const char* arg_sql, std::deque<sqlite_stmt*>* list)
		: sql(arg_sql)
		{ list->push_back(this); }

	bool prepare(sqlite3* db)
		{
		if ( sqlite3_prepare_v2(db, sql.data(), -1, &stmt,
		                        nullptr) != SQLITE_OK )
			return false;

		g = {stmt, sqlite3_finalize};
		return true;
		}

	stmt_guard guard(std::function<int(sqlite3_stmt*)> func) const
		{ return {stmt, func}; }

	operator sqlite3_stmt*() const
		{ return stmt; }

	sqlite3_stmt* stmt = nullptr;
	std::string sql;
	stmt_guard g;
};

class sqlite_backend::impl {
public:

	impl() = default;
	impl(impl&&) = default;
	impl& operator=(impl&&) = default;

	~impl()
		{ sqlite3_close(db); }

	bool prepare_statements()
		{
		for ( auto& s : statements )
			if ( ! s->prepare(db) )
				return false;

		return true;
		}

	sequence_num sn;
	int last_rc = 0;
	std::string our_last_error;
	sqlite3* db = nullptr;
	std::deque<sqlite_stmt*> statements;
	sqlite_stmt insert = {STMT_INSERT, &statements};
	sqlite_stmt erase = {STMT_ERASE, &statements};
	sqlite_stmt expire = {STMT_EXPIRE, &statements};
	sqlite_stmt clear = {STMT_CLEAR, &statements};
	sqlite_stmt update = {STMT_UPDATE, &statements};
	sqlite_stmt update_expiry = {STMT_UPDATE_EXPIRY, &statements};
	sqlite_stmt lookup = {STMT_LOOKUP, &statements};
	sqlite_stmt lookup_expiry = {STMT_LOOKUP_EXPIRY, &statements};
	sqlite_stmt exists = {STMT_EXISTS, &statements};
	sqlite_stmt keys = {STMT_KEYS, &statements};
	sqlite_stmt size = {STMT_SIZE, &statements};
	sqlite_stmt snap = {STMT_SNAP, &statements};
	sqlite_stmt expiries = {STMT_EXPIRIES, &statements};
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_SQLITE_BACKEND_IMPL_HH
