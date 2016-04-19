#include <cstdio>

#include "broker/broker.h"
#include "broker/store/sqlite_backend.hh"

#include "../persistables.hh"
#include "../util/misc.hh"

namespace broker {
namespace store {

template <class T>
static std::string to_blob(const T& obj) {
  util::persist::save_archive saver;
  save(saver, obj);
  return saver.get();
}

template <class T>
static T from_blob(const void* blob, size_t num_bytes) {
  T rval;
  util::persist::load_archive loader(blob, num_bytes);
  load(loader, &rval);
  return rval;
}

sqlite_backend::~sqlite_backend() {
  if (db_)
    sqlite3_close(db_);
}

bool sqlite_backend::open(std::string db_path,
                          std::deque<std::string> pragmas) {
  last_rc_ = 0;
  if (sqlite3_open(db_path.c_str(), &db_) != SQLITE_OK) {
    sqlite3_close(db_);
    db_ = nullptr;
    return false;
  }
  if (sqlite3_exec(db_,
                   "create table if not exists "
                   "meta(k text primary key, v text);",
                   nullptr, nullptr, nullptr)
      != SQLITE_OK)
    return false;
  if (sqlite3_exec(db_,
                   "create table if not exists "
                   "store(k blob primary key, v blob, expiry blob);",
                   nullptr, nullptr, nullptr)
      != SQLITE_OK)
    return false;
  char tmp[128];
  snprintf(tmp, sizeof(tmp),
           "replace into meta(k, v) values('broker_version', '%s');",
           BROKER_VERSION);
  if (sqlite3_exec(db_, tmp, nullptr, nullptr, nullptr) != SQLITE_OK)
    return false;
  if (!insert_.prepare(db_))
    return false;
  if (!erase_.prepare(db_))
    return false;
  if (!expire_.prepare(db_))
    return false;
  if (!clear_.prepare(db_))
    return false;
  if (!update_.prepare(db_))
    return false;
  if (!update_expiry_.prepare(db_))
    return false;
  if (!lookup_.prepare(db_))
    return false;
  if (!lookup_expiry_.prepare(db_))
    return false;
  if (!exists_.prepare(db_))
    return false;
  if (!keys_.prepare(db_))
    return false;
  if (!size_.prepare(db_))
    return false;
  if (!snap_.prepare(db_))
    return false;
  if (!expiries_.prepare(db_))
    return false;
  for (auto& p : pragmas) {
    if (!pragma(std::move(p)))
      return false;
  }
  return true;
}

bool sqlite_backend::sqlite_backend::pragma(std::string p) {
  return sqlite3_exec(db_, p.c_str(), nullptr, nullptr, nullptr)
         == SQLITE_OK;
}

int sqlite_backend::last_error_code() const {
  if (last_rc_ < 0)
    return last_rc_;
  return sqlite3_errcode(db_);
}

void sqlite_backend::do_increase_sequence() {
  ++sn_;
}

std::string sqlite_backend::do_last_error() const {
  if (last_rc_ < 0)
    return our_last_error_;
  return sqlite3_errmsg(db_);
}

bool sqlite_backend::do_init(snapshot sss) {
  if (!clear())
    return false;
  for (const auto& e : sss.entries) {
    if (!insert(insert_, e.first, e.second.item, e.second.expiry)) {
      last_rc_ = 0;
      return false;
    }
  }
  sn_ = std::move(sss.sn);
  return true;
}

const sequence_num& sqlite_backend::do_sequence() const {
  return sn_;
}

bool sqlite_backend::do_insert(
  data k, data v, optional<expiration_time> e) {
  if (!insert(insert_, k, v, e)) {
    last_rc_ = 0;
    return false;
  }
  return true;
}

modification_result
sqlite_backend::do_increment(const data& k, int64_t by, double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op) {
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  if (!op->first) {
    if (insert(insert_, k, data{by}))
      return {modification_result::status::success, {}};
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  auto& v = *op->first;
  if (!util::increment_data(v, by, &our_last_error_)) {
    last_rc_ = -1;
    return {modification_result::status::invalid, {}};
  }
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  return update_entry(update_, update_expiry_, k, v,
                      std::move(new_expiry), last_rc_);
}

modification_result
sqlite_backend::do_add_to_set(const data& k, data element, double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op) {
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  if (!op->first) {
    if (insert(insert_, k, set{std::move(element)}))
      return {modification_result::status::success, {}};
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  auto& v = *op->first;
  if (!util::add_data_to_set(v, std::move(element), &our_last_error_)) {
    last_rc_ = -1;
    return {modification_result::status::invalid, {}};
  }
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  return update_entry(update_, update_expiry_, k, v,
                      std::move(new_expiry), last_rc_);
}

modification_result sqlite_backend::do_remove_from_set(const data& k,
                                                       const data& element,
                                                       double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op) {
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  if (!op->first) {
    if (insert(insert_, k, set{}))
      return {modification_result::status::success, {}};
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  auto& v = *op->first;
  if (!util::remove_data_from_set(v, element, &our_last_error_)) {
    last_rc_ = -1;
    return {modification_result::status::invalid, {}};
  }
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  return update_entry(update_, update_expiry_, k, v, std::move(new_expiry),
                      last_rc_);
}

bool sqlite_backend::do_erase(const data& k) {
  const auto& stmt = erase_;
  auto g = stmt.guard(sqlite3_reset);
  auto kblob = to_blob(k);
  if (sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(), SQLITE_STATIC)
      != SQLITE_OK) {
    last_rc_ = 0;
    return false;
  }
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    last_rc_ = 0;
    return false;
  }
  return true;
}

bool sqlite_backend::do_expire(
  const data& k, const expiration_time& expiration) {
  const auto& stmt = expire_;
  auto g = stmt.guard(sqlite3_reset);
  auto kblob = to_blob(k);
  auto eblob = to_blob(expiration);
  if (sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(), SQLITE_STATIC)
        != SQLITE_OK
      || sqlite3_bind_blob64(stmt, 2, eblob.data(), eblob.size(), SQLITE_STATIC)
           != SQLITE_OK) {
    last_rc_ = 0;
    return false;
  }
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    last_rc_ = 0;
    return false;
  }
  return true;
}

bool sqlite_backend::do_clear() {
  const auto& stmt = clear_;
  auto g = stmt.guard(sqlite3_reset);
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    last_rc_ = 0;
    return false;
  }
  return true;
}

modification_result sqlite_backend::do_push_left(const data& k, vector items,
                                                 double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op) {
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  if (!op->first) {
    if (insert(insert_, k, std::move(items)))
      return {modification_result::status::success, {}};
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  auto& v = *op->first;
  if (!util::push_left(v, std::move(items), &our_last_error_)) {
    last_rc_ = -1;
    return {modification_result::status::invalid, {}};
  }
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  return update_entry(update_, update_expiry_, k, v,
                      std::move(new_expiry), last_rc_);
}

modification_result
sqlite_backend::do_push_right(const data& k, vector items, double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op) {
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  if (!op->first) {
    if (insert(insert_, k, std::move(items)))
      return {modification_result::status::success, {}};
    last_rc_ = 0;
    return {modification_result::status::failure, {}};
  }
  auto& v = *op->first;
  if (!util::push_right(v, std::move(items), &our_last_error_)) {
    last_rc_ = -1;
    return {modification_result::status::invalid, {}};
  }
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  return update_entry(update_, update_expiry_, k, v,
                      std::move(new_expiry), last_rc_);
}

std::pair<modification_result, optional<data>>
sqlite_backend::do_pop_left(const data& k, double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op) {
    last_rc_ = 0;
    return {{modification_result::status::failure, {}}, {}};
  }
  if (!op->first)
    // Fine, key didn't exist.
    return {{modification_result::status::success, {}}, {}};
  auto& v = *op->first;
  auto rval = util::pop_left(v, &our_last_error_);
  if (!rval) {
    last_rc_ = -1;
    return {{modification_result::status::invalid, {}}, {}};
  }
  if (!*rval)
    // Fine, popped an empty list.
    return {{modification_result::status::success, {}}, {}};
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  return {update_entry(update_, update_expiry_, k, v, std::move(new_expiry),
                       last_rc_),
         std::move(*rval)};
}

std::pair<modification_result, optional<data>>
sqlite_backend::do_pop_right(const data& k, double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op) {
    last_rc_ = 0;
    return {{modification_result::status::failure, {}}, {}};
  }
  if (!op->first)
    // Fine, key didn't exist.
    return {{modification_result::status::success, {}}, {}};
  auto& v = *op->first;
  auto rval = util::pop_right(v, &our_last_error_);
  if (!rval) {
    last_rc_ = -1;
    return {{modification_result::status::invalid, {}}, {}};
  }
  if (!*rval)
    // Fine, popped an empty list.
    return {{modification_result::status::success, {}}, {}};
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  return {update_entry(update_, update_expiry_, k, v,
                       std::move(new_expiry), last_rc_),
          std::move(*rval)};
}

optional<optional<data>> sqlite_backend::do_lookup(const data& k) const {
  const auto& stmt = lookup_;
  auto g = stmt.guard(sqlite3_reset);
  auto kblob = to_blob(k);
  if (sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(), SQLITE_STATIC)
      != SQLITE_OK) {
    const_cast<int&>(last_rc_) = 0; // FIXME: improve API
    return {};
  }
  auto rc = sqlite3_step(stmt);
  if (rc == SQLITE_DONE)
    return optional<data>{};
  if (rc != SQLITE_ROW) {
    const_cast<int&>(last_rc_) = 0; // FIXME: improve API
    return {};
  }
  return {from_blob<data>(sqlite3_column_blob(stmt, 0),
                          sqlite3_column_bytes(stmt, 0))};
}

optional<std::pair<optional<data>, optional<expiration_time>>>
sqlite_backend::do_lookup_expiry(const data& k) const {
  const auto& stmt = lookup_expiry_;
  auto g = stmt.guard(sqlite3_reset);
  auto kblob = to_blob(k);
  if (sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(), SQLITE_STATIC)
      != SQLITE_OK) {
    const_cast<int&>(last_rc_) = 0; // FIXME: improve API
    return {};
  }
  auto rc = sqlite3_step(stmt);
  if (rc == SQLITE_DONE)
    return {std::make_pair(optional<data>{},
                           optional<expiration_time>{})};
  if (rc != SQLITE_ROW) {
    const_cast<int&>(last_rc_) = 0; // FIXME: improve API
    return {};
  }
  auto val = from_blob<data>(sqlite3_column_blob(stmt, 0),
                             sqlite3_column_bytes(stmt, 0));
  optional<expiration_time> e;
  if (sqlite3_column_type(stmt, 1) != SQLITE_NULL)
    e = from_blob<expiration_time>(sqlite3_column_blob(stmt, 1),
                                   sqlite3_column_bytes(stmt, 1));
  return {std::make_pair(std::move(val), std::move(e))};
}

optional<bool> sqlite_backend::do_exists(const data& k) const {
  const auto& stmt = exists_;
  auto g = stmt.guard(sqlite3_reset);
  auto kblob = to_blob(k);
  if (sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(), SQLITE_STATIC)
      != SQLITE_OK) {
    const_cast<int&>(last_rc_) = 0; // FIXME: improve API
    return {};
  }
  auto rc = sqlite3_step(stmt);
  if (rc == SQLITE_DONE)
    return false;
  if (rc == SQLITE_ROW)
    return true;
  const_cast<int&>(last_rc_) = 0; // FIXME: improve API
  return {};
}

optional<std::vector<data>> sqlite_backend::do_keys() const {
  const auto& stmt = keys_;
  auto g = stmt.guard(sqlite3_reset);
  std::vector<data> rval;
  int rc;
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    rval.emplace_back(from_blob<data>(sqlite3_column_blob(stmt, 0),
                                      sqlite3_column_bytes(stmt, 0)));
  if (rc == SQLITE_DONE)
    return rval;
  const_cast<int&>(last_rc_) = 0; // FIXME: improve API
  return {};
}

optional<uint64_t> sqlite_backend::do_size() const {
  const auto& stmt = size_;
  auto g = stmt.guard(sqlite3_reset);
  auto rc = sqlite3_step(stmt);
  if (rc == SQLITE_DONE) {
    const_cast<int&>(last_rc_) = -1; // FIXME: improve API
    const_cast<std::string&>(our_last_error_) = "size query returned no result";
    return {};
  }
  if (rc != SQLITE_ROW) {
    const_cast<int&>(last_rc_) = 0; // FIXME: improve API
    return {};
  }
  return sqlite3_column_int(stmt, 0);
}

optional<snapshot> sqlite_backend::do_snap() const {
  const auto& stmt = snap_;
  auto g = stmt.guard(sqlite3_reset);
  snapshot rval;
  rval.sn = sn_;
  int rc;
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    auto k = from_blob<data>(sqlite3_column_blob(stmt, 0),
                             sqlite3_column_bytes(stmt, 0));
    auto v = from_blob<data>(sqlite3_column_blob(stmt, 1),
                             sqlite3_column_bytes(stmt, 1));
    optional<expiration_time> e;
    if (sqlite3_column_type(stmt, 2) != SQLITE_NULL)
      e = from_blob<expiration_time>(sqlite3_column_blob(stmt, 2),
                                     sqlite3_column_bytes(stmt, 2));
    auto entry = std::make_pair(
      std::move(k), value{std::move(v), std::move(e)});
    rval.entries.emplace_back(std::move(entry));
  }
  if (rc == SQLITE_DONE)
    return rval;
  const_cast<int&>(last_rc_) = 0; // FIXME: improve API
  return {};
}

optional<std::deque<expirable>> sqlite_backend::do_expiries() const {
  const auto& stmt = expiries_;
  auto g = stmt.guard(sqlite3_reset);
  std::deque<expirable> rval;
  int rc;
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    auto k = from_blob<data>(sqlite3_column_blob(stmt, 0),
                             sqlite3_column_bytes(stmt, 0));
    auto e = from_blob<expiration_time>(sqlite3_column_blob(stmt, 1),
                                        sqlite3_column_bytes(stmt, 1));
    rval.emplace_back(expirable{std::move(k), std::move(e)});
  }
  if (rc == SQLITE_DONE)
    return rval;
  const_cast<int&>(last_rc_) = 0; // FIXME: improve API
  return {};
}


sqlite_backend::stmt_guard::stmt_guard(sqlite3_stmt* arg_stmt,
                                       std::function<int(sqlite3_stmt*)> fun)
  : stmt{arg_stmt},
    func{std::move(fun)} {
}

sqlite_backend::stmt_guard::~stmt_guard() {
  if (func)
    func(stmt);
}


sqlite_backend::statement::statement(const char* arg_sql) : sql{arg_sql} {
}

sqlite_backend::statement::operator sqlite3_stmt*() const {
  return stmt;
}

bool sqlite_backend::statement::prepare(sqlite3* db) {
  if (sqlite3_prepare_v2(db, sql.data(), -1, &stmt, nullptr) != SQLITE_OK)
    return false;
  g.stmt = stmt;
  g.func = sqlite3_finalize;
  return true;
}

sqlite_backend::stmt_guard 
sqlite_backend::statement::guard(std::function<int(sqlite3_stmt*)> func) const {
  return {stmt, func};
}

bool sqlite_backend::insert(const statement& stmt, const data& k,
                            const data& v, const optional<expiration_time>& e) {
  auto g = stmt.guard(sqlite3_reset);
  auto kblob = to_blob(k);
  auto vblob = to_blob(v);
  // Need the expiry blob data to stay in scope until query is done.
  auto eblob = e ? to_blob(*e) : std::string{};
  if (sqlite3_bind_blob64(stmt, 1, kblob.data(), kblob.size(), SQLITE_STATIC)
        != SQLITE_OK
      || sqlite3_bind_blob64(stmt, 2, vblob.data(), vblob.size(), SQLITE_STATIC)
           != SQLITE_OK)
    return false;
  if (e) {
    if (sqlite3_bind_blob64(stmt, 3, eblob.data(), eblob.size(), SQLITE_STATIC)
        != SQLITE_OK)
      return false;
  } else {
    if (sqlite3_bind_null(stmt, 3) != SQLITE_OK)
      return false;
  }
  if (sqlite3_step(stmt) != SQLITE_DONE)
    return false;
  return true;
}

bool sqlite_backend::update(const statement& stmt, const data& k,
                            const data& v) {
  auto g = stmt.guard(sqlite3_reset);
  auto kblob = to_blob(k);
  auto vblob = to_blob(v);
  if (sqlite3_bind_blob64(stmt, 1, vblob.data(), vblob.size(), SQLITE_STATIC)
        != SQLITE_OK
      || sqlite3_bind_blob64(stmt, 2, kblob.data(), kblob.size(), SQLITE_STATIC)
           != SQLITE_OK)
    return false;
  if (sqlite3_step(stmt) != SQLITE_DONE)
    return false;
  return true;
}

bool sqlite_backend::update_expiry(const statement& stmt,
                                   const data& k, const data& v,
                                   const expiration_time& e) {
  auto g = stmt.guard(sqlite3_reset);
  auto kblob = to_blob(k);
  auto vblob = to_blob(v);
  auto eblob = to_blob(e);
  if (sqlite3_bind_blob64(stmt, 1, vblob.data(), vblob.size(), SQLITE_STATIC)
        != SQLITE_OK
      || sqlite3_bind_blob64(stmt, 2, eblob.data(), eblob.size(), SQLITE_STATIC)
           != SQLITE_OK
      || sqlite3_bind_blob64(stmt, 3, kblob.data(), kblob.size(), SQLITE_STATIC)
           != SQLITE_OK)
    return false;
  if (sqlite3_step(stmt) != SQLITE_DONE)
    return false;
  return true;
}

modification_result 
sqlite_backend::update_entry(const statement& update_stmt,
                             const statement& update_expiry_stmt,
                             const data& k, const data& v,
                             optional<expiration_time> e,
                             int& last_rc) {
  using store::modification_result;
  if (e) {
    if (update_expiry(update_expiry_stmt, k, v, *e))
      return {modification_result::status::success, e};
  } else {
    if (update(update_stmt, k, v))
      return {modification_result::status::success, {}};
  }
  last_rc = 0;
  return {modification_result::status::failure, {}};
}

} // namespace broker
} // namespace store

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_store_sqlite_backend* broker_store_sqlite_backend_create() {
  auto rval = new (nothrow) broker::store::sqlite_backend();
  return reinterpret_cast<broker_store_sqlite_backend*>(rval);
}

void broker_store_sqlite_backend_delete(broker_store_sqlite_backend* b) {
  delete reinterpret_cast<broker::store::sqlite_backend*>(b);
}

int broker_store_sqlite_backend_open(broker_store_sqlite_backend* b,
                                     const char* path) {
  auto bb = reinterpret_cast<broker::store::sqlite_backend*>(b);
  return bb->open(path);
}

int broker_store_sqlite_backend_pragma(broker_store_sqlite_backend* b,
                                       const char* pragma) {
  auto bb = reinterpret_cast<broker::store::sqlite_backend*>(b);
  return bb->pragma(pragma);
}

int broker_store_sqlite_backend_last_error_code(
  const broker_store_sqlite_backend* b) {
  auto bb = reinterpret_cast<const broker::store::sqlite_backend*>(b);
  return bb->last_error_code();
}

const char*
broker_store_sqlite_backend_last_error(const broker_store_sqlite_backend* b) {
  auto bb = reinterpret_cast<const broker::store::sqlite_backend*>(b);
  return bb->last_error().data();
}
