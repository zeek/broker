#include <rocksdb/env.h>

#include "broker/broker.hh"
#include "broker/store/rocksdb_backend.hh"

#include "../persistables.hh"
#include "../util/misc.hh"

namespace broker {
namespace store {

namespace detail {

template <class T>
static void to_serial(const T& obj, std::string& rval) {
  util::persist::save_archive saver(std::move(rval));
  save(saver, obj);
  rval = saver.get();
}

template <class T>
static std::string to_serial(const T& obj) {
  std::string rval;
  to_serial(obj, rval);
  return rval;
}

template <class T>
static std::string to_serial(const T& obj, char keyspace) {
  std::string rval{keyspace};
  to_serial(obj, rval);
  return rval;
}

template <class T>
static T from_serial(const char* blob, size_t num_bytes) {
  T rval;
  util::persist::load_archive loader(blob, num_bytes);
  load(loader, &rval);
  return rval;
}

template <class T, class C>
static T from_serial(const C& bytes) {
  return from_serial<T>(bytes.data(), bytes.size());
}

static rocksdb::Status
insert(rocksdb::DB* db, const data& k, const data& v,
       bool delete_expiry_if_nil,
       const maybe<expiration_time>& e = {}) {
  auto kserial = to_serial(k, 'a');
  auto vserial = to_serial(v);
  rocksdb::WriteBatch batch;
  batch.Put(kserial, vserial);
  kserial[0] = 'e';
  if (e) {
    auto evserial = to_serial(*e);
    batch.Put(kserial, evserial);
  } else if (delete_expiry_if_nil)
    batch.Delete(kserial);
  return db->Write({}, &batch);
}

} // namespace detail

rocksdb_backend::rocksdb_backend(uint64_t exact_size_threshold)
  : exact_size_threshold_{exact_size_threshold} {
}

rocksdb::Status rocksdb_backend::open(std::string db_path,
                                      rocksdb::Options options) {
  rocksdb::DB* db;
  auto rval = rocksdb::DB::Open(options, db_path, &db);
  db_.reset(db);
  options.create_if_missing = true;
  options_ = options;
  if (require_ok(rval)) {
    // Use key-space prefix 'm' to store metadata, 'a' for application
    // data, and 'e' for expiration values.
    rval = db_->Put({}, "mbroker_version", BROKER_VERSION);
    require_ok(rval);
    return rval;
  }
  return rval;
}

void rocksdb_backend::do_increase_sequence() {
  ++sn_;
}

std::string rocksdb_backend::do_last_error() const {
  return last_error_;
}

bool rocksdb_backend::do_init(snapshot sss) {
  if (!do_clear())
    return false;
  rocksdb::WriteBatch batch;
  for (const auto& kv : sss.entries) {
    auto kserial = detail::to_serial(kv.first, 'a');
    auto vserial = detail::to_serial(kv.second.item);
    batch.Put(kserial, vserial);
    if (kv.second.expiry) {
      kserial[0] = 'e';
      auto evserial = detail::to_serial(*kv.second.expiry);
      batch.Put(kserial, evserial);
    }
  }
  sn_ = std::move(sss.sn);
  return require_ok(db_->Write({}, &batch));
}

const sequence_num& rocksdb_backend::do_sequence() const {
  return sn_;
}

bool rocksdb_backend::do_insert(
  data k, data v, maybe<expiration_time> e) {
  if (!require_db())
    return false;
  return require_ok(detail::insert(db_.get(), k, v, true, e));
}

modification_result rocksdb_backend::do_increment(const data& k, int64_t by,
                                                  double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op)
    return {modification_result::status::failure, {}};
  if (!util::increment_data(op->first, by, &last_error_))
    return {modification_result::status::invalid, {}};
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  if (require_ok(
        detail::insert(db_.get(), k, *op->first, false, new_expiry)))
    return {modification_result::status::success, std::move(new_expiry)};
  return {modification_result::status::failure, {}};
}

modification_result rocksdb_backend::do_add_to_set(const data& k, data element,
                                                   double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op)
    return {modification_result::status::failure, {}};
  if (!util::add_data_to_set(op->first, std::move(element), &last_error_))
    return {modification_result::status::invalid, {}};
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  if (require_ok(
        detail::insert(db_.get(), k, *op->first, false, new_expiry)))
    return {modification_result::status::success, std::move(new_expiry)};
  return {modification_result::status::failure, {}};
}

modification_result rocksdb_backend::do_remove_from_set(const data& k,
                                                        const data& element,
                                                        double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op)
    return {modification_result::status::failure, {}};
  if (!util::remove_data_from_set(op->first, element, &last_error_))
    return {modification_result::status::invalid, {}};
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  if (require_ok(
        detail::insert(db_.get(), k, *op->first, false, new_expiry)))
    return {modification_result::status::success, std::move(new_expiry)};
  return {modification_result::status::failure, {}};
}

bool rocksdb_backend::do_erase(const data& k) {
  if (!require_db())
    return false;
  auto kserial = detail::to_serial(k, 'a');
  if (!require_ok(db_->Delete({}, kserial)))
    return false;
  kserial[0] = 'e';
  return require_ok(db_->Delete({}, kserial));
}

bool rocksdb_backend::do_erase(std::string kserial) {
  if (!require_db())
    return false;
  kserial[0] = 'a';
  if (!require_ok(db_->Delete({}, kserial)))
    return false;
  kserial[0] = 'e';
  return require_ok(db_->Delete({}, kserial));
}

bool rocksdb_backend::do_expire(const data& k,
                                const expiration_time& expiration) {
  if (!require_db())
    return false;
  auto kserial = detail::to_serial(k, 'e');
  std::string vserial;
  bool value_found;
  if (!db_->KeyMayExist({}, kserial, &vserial, &value_found))
    return true;
  if (value_found) {
    auto stored_expiration = detail::from_serial<expiration_time>(vserial);
    if (stored_expiration == expiration)
      return do_erase(std::move(kserial));
    else
      return true;
  }
  auto stat = db_->Get(rocksdb::ReadOptions{}, kserial, &vserial);
  if (stat.IsNotFound())
    return true;
  if (!require_ok(stat))
    return false;
  auto stored_expiration = detail::from_serial<expiration_time>(vserial);
  if (stored_expiration == expiration)
    return do_erase(std::move(kserial));
  else
    return true;
}

bool rocksdb_backend::do_clear() {
  if (!require_db())
    return false;
  std::string db_path = db_->GetName();
  db_.reset();
  auto stat = rocksdb::DestroyDB(db_path, rocksdb::Options{});
  if (!require_ok(stat))
    return false;
  return require_ok(open(std::move(db_path), options_));
}

modification_result rocksdb_backend::do_push_left(const data& k, vector items,
                                                  double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op)
    return {modification_result::status::failure, {}};
  if (!util::push_left(op->first, std::move(items), &last_error_))
    return {modification_result::status::invalid, {}};
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  if (require_ok(
        detail::insert(db_.get(), k, *op->first, false, new_expiry)))
    return {modification_result::status::success, std::move(new_expiry)};
  return {modification_result::status::failure, {}};
}

modification_result rocksdb_backend::do_push_right(const data& k, vector items,
                                                   double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op)
    return {modification_result::status::failure, {}};
  if (!util::push_right(op->first, std::move(items), &last_error_))
    return {modification_result::status::invalid, {}};
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  if (require_ok(
        detail::insert(db_.get(), k, *op->first, false, new_expiry)))
    return {modification_result::status::success, std::move(new_expiry)};
  return {modification_result::status::failure, {}};
}

std::pair<modification_result, maybe<data>>
rocksdb_backend::do_pop_left(const data& k, double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op)
    return {{modification_result::status::failure, {}}, {}};
  if (!op->first)
    // Fine, key didn't exist.
    return {{modification_result::status::success, {}}, {}};
  auto& v = *op->first;
  auto rval = util::pop_left(v, &last_error_);
  if (!rval)
    return {{modification_result::status::invalid, {}}, {}};
  if (!*rval)
    // Fine, popped an empty list.
    return {{modification_result::status::success, {}}, {}};
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  if (require_ok(detail::insert(db_.get(), k, v, false,
                                      new_expiry)))
    return {{modification_result::status::success, std::move(new_expiry)},
            std::move(*rval)};
  return {{modification_result::status::failure, {}}, {}};
}

std::pair<modification_result, maybe<data>>
rocksdb_backend::do_pop_right(const data& k, double mod_time) {
  auto op = do_lookup_expiry(k);
  if (!op)
    return {{modification_result::status::failure, {}}, {}};
  if (!op->first)
    // Fine, key didn't exist.
    return {{modification_result::status::success, {}}, {}};
  auto& v = *op->first;
  auto rval = util::pop_right(v, &last_error_);
  if (!rval)
    return {{modification_result::status::invalid, {}}, {}};
  if (!*rval)
    // Fine, popped an empty list.
    return {{modification_result::status::success, {}}, {}};
  auto new_expiry = util::update_last_modification(op->second, mod_time);
  if (require_ok(detail::insert(db_.get(), k, v, false,
                                      new_expiry)))
    return {{modification_result::status::success, std::move(new_expiry)},
            std::move(*rval)};
  return {{modification_result::status::failure, {}}, {}};
}

maybe<maybe<data>>
rocksdb_backend::do_lookup(const data& k) const {
  if (!require_db())
    return {};
  auto kserial = detail::to_serial(k, 'a');
  std::string vserial;
  bool value_found;
  if (!db_->KeyMayExist({}, kserial, &vserial, &value_found))
    return maybe<data>{};
  if (value_found)
    return {detail::from_serial<data>(vserial)};
  auto stat = db_->Get(rocksdb::ReadOptions{}, kserial, &vserial);
  if (stat.IsNotFound())
    return maybe<data>{};
  if (!require_ok(stat))
    return {};
  return {detail::from_serial<data>(vserial)};
}

maybe<std::pair<maybe<data>, maybe<expiration_time>>>
rocksdb_backend::do_lookup_expiry(const data& k) const {
  if (!require_db())
    return {};
  auto kserial = detail::to_serial(k, 'a');
  std::string vserial;
  bool value_found;
  if (!db_->KeyMayExist({}, kserial, &vserial, &value_found))
    return {std::make_pair(maybe<data>{},
                           maybe<expiration_time>{})};
  data value;
  if (value_found)
    value = detail::from_serial<data>(vserial);
  else {
    auto stat = db_->Get(rocksdb::ReadOptions{}, kserial, &vserial);
    if (stat.IsNotFound())
      return {std::make_pair(maybe<data>{},
                             maybe<expiration_time>{})};
    if (!require_ok(stat))
      return {};
    value = detail::from_serial<data>(vserial);
  }
  kserial[0] = 'e';
  value_found = false;
  if (!db_->KeyMayExist({}, kserial, &vserial, &value_found))
    return {
      std::make_pair(std::move(value), maybe<expiration_time>{})};
  expiration_time expiry;
  if (value_found)
    expiry = detail::from_serial<expiration_time>(vserial);
  else {
    auto stat = db_->Get(rocksdb::ReadOptions{}, kserial, &vserial);
    if (stat.IsNotFound())
      return {
        std::make_pair(std::move(value), maybe<expiration_time>{})};
    if (!require_ok(stat))
      return {};
    expiry = detail::from_serial<expiration_time>(vserial);
  }
  return {std::make_pair(std::move(value), std::move(expiry))};
}

maybe<bool> rocksdb_backend::do_exists(const data& k) const {
  if (!require_db())
    return {};
  auto kserial = detail::to_serial(k, 'a');
  std::string vserial;
  if (!db_->KeyMayExist(rocksdb::ReadOptions{}, kserial, &vserial))
    return false;
  auto stat = db_->Get(rocksdb::ReadOptions{}, kserial, &vserial);
  if (stat.IsNotFound())
    return false;
  if (!require_ok(stat))
    return {};
  return true;
}

maybe<std::vector<data>> rocksdb_backend::do_keys() const {
  if (!require_db())
    return {};
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(options));
  std::vector<data> rval;
  for (it->Seek("a"); it->Valid() && it->key()[0] == 'a'; it->Next()) {
    auto s = it->key();
    s.remove_prefix(1);
    rval.emplace_back(detail::from_serial<data>(s));
  }
  if (!require_ok(it->status()))
    return {};
  return rval;
}

maybe<uint64_t> rocksdb_backend::do_size() const {
  if (!require_db())
    return {};
  uint64_t rval;
  if (db_->GetIntProperty("rocksdb.estimate-num-keys", &rval)
      && rval > exact_size_threshold_)
    return rval;
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it{db_->NewIterator(options)};
  rval = 0;
  for (it->Seek("a"); it->Valid() && it->key()[0] == 'a'; it->Next())
    ++rval;
  if (require_ok(it->status()))
    return rval;
  return {};
}

maybe<snapshot> rocksdb_backend::do_snap() const {
  if (!require_db())
    return {};
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(options));
  snapshot rval;
  rval.sn = sn_;
  std::unordered_map<data, expiration_time> expiries;
  for (it->Seek("e"); it->Valid() && it->key()[0] == 'e'; it->Next()) {
    auto ks = it->key();
    auto vs = it->value();
    ks.remove_prefix(1);
    auto key = detail::from_serial<data>(ks);
    expiries[std::move(key)] = detail::from_serial<expiration_time>(vs);
  }
  if (!require_ok(it->status()))
    return {};
  for (it->Seek("a"); it->Valid() && it->key()[0] == 'a'; it->Next()) {
    auto ks = it->key();
    auto vs = it->value();
    ks.remove_prefix(1);
    auto entry = std::make_pair(detail::from_serial<data>(ks),
                                value{detail::from_serial<data>(vs)});
    auto eit = expiries.find(entry.first);
    if (eit != expiries.end())
      entry.second.expiry = std::move(eit->second);
    rval.entries.emplace_back(std::move(entry));
  }
  if (!require_ok(it->status()))
    return {};
  return rval;
}

maybe<std::deque<expirable>> rocksdb_backend::do_expiries() const {
  if (!require_db())
    return {};
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> it{db_->NewIterator(options)};
  std::deque<expirable> rval;
  for (it->Seek("e"); it->Valid() && it->key()[0] == 'e'; it->Next()) {
    auto ks = it->key();
    auto vs = it->value();
    ks.remove_prefix(1);
    auto key = detail::from_serial<data>(ks);
    auto expiry = detail::from_serial<expiration_time>(vs);
    rval.emplace_back(expirable{std::move(key), std::move(expiry)});
  }
  if (!require_ok(it->status()))
    return {};
  return rval;
}

bool rocksdb_backend::require_db() const {
  if (db_)
    return true;
  // FIXME: this const_cast is just a workaround for the sub-optimal
  // inheritence API. Most of the virtual functions should *not* be const
  // because the mutate the DB. Neither should this function be const, because
  // it changes the backend's state.
  const_cast<std::string&>(last_error_) = "db not open";
  return false;
}

bool rocksdb_backend::require_ok(const rocksdb::Status& s) const {
  if (s.ok())
    return true;
  // FIXME: see note above.
  const_cast<std::string&>(last_error_) = s.ToString();
  return false;
}

} // namespace store
} // namespace broker

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_store_rocksdb_backend* broker_store_rocksdb_backend_create() {
  auto rval = new (nothrow) broker::store::rocksdb_backend();
  return reinterpret_cast<broker_store_rocksdb_backend*>(rval);
}

void broker_store_rocksdb_backend_delete(broker_store_rocksdb_backend* b) {
  delete reinterpret_cast<broker::store::rocksdb_backend*>(b);
}

int broker_store_rocksdb_backend_open(broker_store_rocksdb_backend* b,
                                      const char* path, int create_if_missing) {
  auto bb = reinterpret_cast<broker::store::rocksdb_backend*>(b);
  rocksdb::Options options = {};
  options.create_if_missing = create_if_missing;
  return bb->open(path, options).ok();
}

const char*
broker_store_rocksdb_backend_last_error(const broker_store_rocksdb_backend* b) {
  auto bb = reinterpret_cast<const broker::store::rocksdb_backend*>(b);
  return bb->last_error().data();
}
