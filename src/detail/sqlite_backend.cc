#include "broker/logger.hh"

#include <cstdio> // std::snprintf
#include <utility>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include <caf/detail/scope_guard.hpp>

#include "broker/config.hh"
#include "broker/version.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/optional.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/appliers.hh"
#include "broker/detail/blob.hh"
#include "broker/detail/filesystem.hh"
#include "broker/detail/sqlite_backend.hh"

#include "sqlite3.h"

namespace broker {
namespace detail {
namespace {

auto make_statement_guard = [](sqlite3_stmt* stmt) {
  return caf::detail::make_scope_guard([=] { sqlite3_reset(stmt); });
};

} // namespace <anonymous>

struct sqlite_backend::impl {
  impl(backend_options opts) : options{std::move(opts)} {
    auto i = options.find("path");
    if (i == options.end())
      return;
    auto path = caf::get_if<std::string>(&i->second);
    if (!path)
      return;
    if (!open(*path))
      db = nullptr;
  }

  ~impl() {
    if (!db)
      return;
    // Deallocate prepared statements.
    for (auto stmt : finalize)
      sqlite3_finalize(stmt);
    // Close database.
    sqlite3_close(db);
  }

  bool open(const std::string& path) {
    auto dir = detail::dirname(path);

    if ( ! dir.empty() ) {
      if ( ! detail::mkdirs(dir) ) {
        BROKER_ERROR("failed to create database dir:" << path);
        return false;
      }
    }

    // Initialize database.  SQLite has a bit of custom memory management
    // that seems to cause some LSANs to lose track and report a leak.
    BROKER_LSAN_DISABLE();
    auto result = sqlite3_open(path.c_str(), &db);
    BROKER_LSAN_ENABLE();
    if (result != SQLITE_OK) {
      sqlite3_close(db);
      BROKER_ERROR("failed to open database:" << path);
      return false;
    }
    // Create table for store meta data.
    result = sqlite3_exec(db,
                          "create table if not exists "
                          "meta(key text primary key, value text);",
                          nullptr, nullptr, nullptr);
    if (result != SQLITE_OK) {
      BROKER_ERROR("failed to create meta data table");
      return false;
    }
    // Create table for actual data.
    result = sqlite3_exec(db,
                          "create table if not exists store"
                          "(key blob primary key, value blob, expiry integer);",
                          nullptr, nullptr, nullptr);
    if (result != SQLITE_OK) {
      BROKER_ERROR("failed to create store table");
      return false;
    }
    // Store Broker version in meta table.
    char tmp[128];
    std::snprintf(tmp, sizeof(tmp),
                  "replace into meta(key, value) "
                  "values('broker_version', '%u.%u.%u');",
                  version::major, version::minor, version::patch);
    result = sqlite3_exec(db, tmp, nullptr, nullptr, nullptr);
    if (result != SQLITE_OK) {
      BROKER_ERROR("failed to insert Broker version");
      return false;
    }
    // Prepare statements.
    std::vector<std::pair<sqlite3_stmt**, const char*>> statements{
      {&replace, "replace into store(key, value, expiry) values(?, ?, ?);"},
      {&update, "update store set value = ?, expiry = ? where key = ?;"},
      {&erase, "delete from store where key = ?;"},
      {&expire, "delete from store where key = ? and expiry <= ?;"},

      {&lookup, "select value from store where key = ?;"},
      {&exists, "select 1 from store where key = ?;"},
      {&size, "select count(*) from store;"},
      {&snapshot, "select key, value from store;"},
      {&expiries, "select key, expiry from store where expiry is not null;"},
      {&clear, "delete from store;"},
      {&keys, "select key from store;"},
    };
    auto prepare = [&](sqlite3_stmt** stmt, const char* sql) {
      finalize.push_back(*stmt);
      return sqlite3_prepare_v2(db, sql, -1, stmt, nullptr) == SQLITE_OK;
    };
    for (auto& stmt: statements)
      if (!prepare(stmt.first, stmt.second)) {
        BROKER_ERROR("failed to prepare statement:" << stmt.second);
        return false;
      }
    return true;
  }

  bool modify(const data& key, const data& value,
              optional<timestamp> expiry) {
    auto key_blob = to_blob(key);
    auto value_blob = to_blob(value);
    auto guard = make_statement_guard(update);

    // Bind value.
    auto result = sqlite3_bind_blob64(update, 1, value_blob.data(),
                                      value_blob.size(), SQLITE_STATIC);

    if (result != SQLITE_OK)
      return false;

    if (expiry) {
      // Bind expiry.
      auto t = expiry->time_since_epoch().count();
      result = sqlite3_bind_int64(update, 2, t);
    } else {
      result = sqlite3_bind_null(update, 2);
    }

    if (result != SQLITE_OK)
      return false;

    // Bind key.
    result = sqlite3_bind_blob64(update, 3, key_blob.data(),
                                 key_blob.size(), SQLITE_STATIC);

    if (result != SQLITE_OK)
      return false;

    // Execute statement.
    return sqlite3_step(update) == SQLITE_DONE;
  }

  backend_options options;
  sqlite3* db = nullptr;
  sqlite3_stmt* replace = nullptr;
  sqlite3_stmt* update = nullptr;
  sqlite3_stmt* erase = nullptr;
  sqlite3_stmt* expire = nullptr;
  sqlite3_stmt* lookup = nullptr;
  sqlite3_stmt* exists = nullptr;
  sqlite3_stmt* size = nullptr;
  sqlite3_stmt* snapshot = nullptr;
  sqlite3_stmt* expiries = nullptr;
  sqlite3_stmt* clear = nullptr;
  sqlite3_stmt* keys = nullptr;
  std::vector<sqlite3_stmt*> finalize;
};


sqlite_backend::sqlite_backend(backend_options opts)
  : impl_{std::make_unique<impl>(std::move(opts))} {
}

sqlite_backend::~sqlite_backend() {
}

expected<void> sqlite_backend::put(const data& key, data value,
                                   optional<timestamp> expiry) {
  if (!impl_->db)
    return ec::backend_failure;
  auto guard = make_statement_guard(impl_->replace);
  // Bind key.
  auto key_blob = to_blob(key);
  auto result = sqlite3_bind_blob64(impl_->replace, 1, key_blob.data(),
                                    key_blob.size(), SQLITE_STATIC);
  if (result != SQLITE_OK)
    return ec::backend_failure;
  // Bind value.
  auto value_blob = to_blob(value);
  result = sqlite3_bind_blob64(impl_->replace, 2, value_blob.data(),
                               value_blob.size(), SQLITE_STATIC);
  if (result != SQLITE_OK)
    return ec::backend_failure;
  if (expiry)
    result = sqlite3_bind_int64(impl_->replace, 3,
                                expiry->time_since_epoch().count());
  else
    result = sqlite3_bind_null(impl_->replace, 3);
  if (result != SQLITE_OK)
    return ec::backend_failure;
  // Execute statement.
  if (sqlite3_step(impl_->replace) != SQLITE_DONE)
    return ec::backend_failure;
  return {};
}

expected<void> sqlite_backend::add(const data& key, const data& value,
                                   data::type init_type,
                                   optional<timestamp> expiry) {
  auto v = get(key);
  data vv;
  if (!v) {
    if (v.error() != ec::no_such_key)
      return v.error();
    vv = data::from_type(init_type);
  } else {
    vv = std::move(*v);
  }
  auto result = caf::visit(adder{value}, vv);
  if (!result)
    return result;
  return put(key, std::move(vv), expiry);
}

expected<void> sqlite_backend::subtract(const data& key, const data& value,
                                        optional<timestamp> expiry) {
  auto v = get(key);
  if (!v)
    return v.error();
  auto result = caf::visit(remover{value}, *v);
  if (!result)
    return result;
  if (!impl_->modify(key, *v, expiry))
    return ec::backend_failure;
  return {};
}

expected<void> sqlite_backend::erase(const data& key) {
  if (!impl_->db)
    return ec::backend_failure;
  auto guard = make_statement_guard(impl_->erase);
	auto key_blob = to_blob(key);
  auto result = sqlite3_bind_blob64(impl_->erase, 1, key_blob.data(),
                                    key_blob.size(), SQLITE_STATIC);
  if (result != SQLITE_OK)
    return ec::backend_failure;
	result = sqlite3_step(impl_->erase);
  if (result != SQLITE_DONE)
    return ec::backend_failure;
  //if (sqlite3_changes(impl_->db) == 0)
  //  return ec::no_such_key;
  return {};
}

expected<void> sqlite_backend::clear() {
  if (!impl_->db)
    return ec::backend_failure;
  auto guard = make_statement_guard(impl_->clear);
  auto result = sqlite3_step(impl_->clear);
  if (result != SQLITE_DONE)
    return ec::backend_failure;
  return {};
}

expected<bool> sqlite_backend::expire(const data& key, timestamp ts) {
  if (!impl_->db)
    return ec::backend_failure;
  auto guard = make_statement_guard(impl_->expire);
  // Bind key.
	auto key_blob = to_blob(key);
  auto result = sqlite3_bind_blob64(impl_->expire, 1, key_blob.data(),
                                    key_blob.size(), SQLITE_STATIC);
  if (result != SQLITE_OK)
    return ec::backend_failure;
  // Bind expiry.
  result = sqlite3_bind_int64(impl_->expire, 2, ts.time_since_epoch().count());
  if (result != SQLITE_OK)
    return ec::backend_failure;
  // Execute query.
	result = sqlite3_step(impl_->expire);
  if (result != SQLITE_DONE)
    return ec::backend_failure;
  return sqlite3_changes(impl_->db) == 1;
}

expected<data> sqlite_backend::get(const data& key) const {
  if (!impl_->db)
    return ec::backend_failure;
  auto guard = make_statement_guard(impl_->lookup);
	auto key_blob = to_blob(key);
  auto result = sqlite3_bind_blob64(impl_->lookup, 1, key_blob.data(),
                                    key_blob.size(), SQLITE_STATIC);
  if (result != SQLITE_OK)
    return ec::backend_failure;
	result = sqlite3_step(impl_->lookup);
	if (result == SQLITE_DONE)
	  return ec::no_such_key;
	if (result != SQLITE_ROW)
    return ec::backend_failure;
	return from_blob<data>(sqlite3_column_blob(impl_->lookup, 0),
                         sqlite3_column_bytes(impl_->lookup, 0));
}

expected<data> sqlite_backend::keys() const {
  if (!impl_->db)
    return ec::backend_failure;
  auto guard = make_statement_guard(impl_->keys);
  set keys;
  auto result = SQLITE_DONE;
  while ((result = sqlite3_step(impl_->keys)) == SQLITE_ROW) {
    auto key = from_blob<data>(sqlite3_column_blob(impl_->keys, 0),
                               sqlite3_column_bytes(impl_->keys, 0));
    keys.insert(std::move(key));
  }
  if (result == SQLITE_DONE)
    return {std::move(keys)};
  return ec::backend_failure;
}

expected<bool> sqlite_backend::exists(const data& key) const {
  if (!impl_->db)
    return ec::backend_failure;
  auto guard = make_statement_guard(impl_->exists);
	auto key_blob = to_blob(key);
  auto result = sqlite3_bind_blob64(impl_->exists, 1, key_blob.data(),
                                    key_blob.size(), SQLITE_STATIC);
  if (result != SQLITE_OK)
    return ec::backend_failure;
	result = sqlite3_step(impl_->exists);
	if (result == SQLITE_DONE)
	  return false;
	if (result != SQLITE_ROW)
    return ec::backend_failure;
  auto n = sqlite3_column_int(impl_->exists, 0);
  BROKER_ASSERT(n == 1);
	return true;

}

expected<uint64_t> sqlite_backend::size() const {
  if (!impl_->db)
    return ec::backend_failure;
  auto guard = make_statement_guard(impl_->size);
	auto result = sqlite3_step(impl_->size);
	if (result != SQLITE_ROW)
	  return ec::backend_failure;
	return sqlite3_column_int(impl_->size, 0);
}

expected<snapshot> sqlite_backend::snapshot() const {
  if (!impl_->db)
    return ec::backend_failure;
  auto guard = make_statement_guard(impl_->snapshot);
  broker::snapshot ss;
  auto result = SQLITE_DONE;
  while ((result = sqlite3_step(impl_->snapshot)) == SQLITE_ROW) {
    auto key = from_blob<data>(sqlite3_column_blob(impl_->snapshot, 0),
                               sqlite3_column_bytes(impl_->snapshot, 0));
    auto value = from_blob<data>(sqlite3_column_blob(impl_->snapshot, 1),
                                 sqlite3_column_bytes(impl_->snapshot, 1));
    ss.emplace(std::move(key), std::move(value));
  }
  if (result == SQLITE_DONE)
    return {std::move(ss)};
  return ec::backend_failure;
}

expected<expirables> sqlite_backend::expiries() const {
  if (!impl_->db)
    return ec::backend_failure;

  auto guard = make_statement_guard(impl_->expiries);
  expirables rval;
  auto result = SQLITE_DONE;

  while ((result = sqlite3_step(impl_->expiries)) == SQLITE_ROW) {
    auto key = from_blob<data>(sqlite3_column_blob(impl_->expiries, 0),
                               sqlite3_column_bytes(impl_->expiries, 0));
    auto expiry_count = sqlite3_column_int64(impl_->expiries, 1);
    auto duration = timespan(expiry_count);
    auto expiry = timestamp(duration);
    auto e = expirable(std::move(key), std::move(expiry));
    rval.emplace_back(std::move(e));
  }

  if (result == SQLITE_DONE)
    return {std::move(rval)};

  return ec::backend_failure;
}

} // namespace detail
} // namespace broker
