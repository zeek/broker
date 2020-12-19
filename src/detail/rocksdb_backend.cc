#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <string_view>

#include "broker/detail/appliers.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/base64.hh"
#include "broker/detail/blob.hh"
#include "broker/detail/filesystem.hh"
#include "broker/detail/rocksdb_backend.hh"
#include "broker/error.hh"
#include "broker/logger.hh"
#include "broker/version.hh"

namespace broker::detail {

// The data store layout follows the following convention: we use a key-space
// prefix to emulate different tables:
//
//   - 'm' for meta data
//   - 'd' for application data
//   - 'e' for expiration values
//
namespace {

enum class prefix : char {
  meta = 'm',
  data = 'd',
  expiry = 'e',
};

template <prefix P, class T>
std::string to_base64_key(const T& x) {
  std::string result;
  result.push_back(static_cast<char>(P));
  auto buf = to_blob(x);
  base64::encode(buf, result);
  return result;
}

template <prefix P>
data from_base64_key(rocksdb::Slice slice) {
  BROKER_ASSERT(!slice.empty());
  BROKER_ASSERT(slice[0] == static_cast<char>(P));
  blob_buffer buf;
  slice.remove_prefix(1);
  base64::decode(std::string_view{slice.data(), slice.size()}, buf);
  return from_blob<data>(buf);
}

template <class T>
std::string to_base64_value(const T& x) {
  std::string result;
  auto buf = to_blob(x);
  base64::encode(buf, result);
  return result;
}

template <class T>
T from_base64_value(rocksdb::Slice slice) {
  blob_buffer buf;
  base64::decode(std::string_view{slice.data(), slice.size()}, buf);
  return from_blob<T>(buf);
}

} // namespace <anonymous>

struct rocksdb_backend::impl {
  template <class Key, class Value>
  bool put(const Key& key, const Value& value) {
    if (!db)
      return false;
    auto status = db->Put({}, key, value);
    if (!status.ok()) {
      BROKER_ERROR("failed put key-value-pair:" << status.ToString());
      return false;
    }
    return true;
  }

  template <class Key, class Value>
  bool put(Key& key, const Value& value, optional<timestamp> expiry) {
    if (!db)
      return false;
    rocksdb::WriteBatch batch;
    batch.Put(key, value);
    // Write expiry.
    if (expiry) {
      BROKER_ASSERT(key.size() > 1);
      key[0] = static_cast<char>(prefix::expiry); // reuse key blob
      auto blob = to_base64_value(*expiry);
      batch.Put(key, blob);
    }
    auto status = db->Write({}, &batch);
    if (!status.ok()) {
      BROKER_ERROR("failed to put key-value pair:" << status.ToString());
      return false;
    }
    return true;
  }

  template <class Key>
  expected<std::string> get(const Key& key) {
    if (!db)
      return ec::backend_failure;
    std::string value;
    bool exists;
    if (!db->KeyMayExist({}, key, &value, &exists))
      return ec::no_such_key;
    if (exists)
      return value;
    auto status = db->Get(rocksdb::ReadOptions{}, key, &value);
    if (status.IsNotFound())
      return ec::no_such_key;
    if (!status.ok()) {
      BROKER_ERROR("failed to lookup value:" << status.ToString());
      return ec::backend_failure;
    }
    return value;
  }

  // This is a rather expensive operation for large values, because the RocksDB
  // API surprisingly doesn't allow for efficient checking of key existence; a
  // value is always returned along the way.
  template <class Key>
  expected<bool> exists(const Key& key) {
    if (!db)
      return ec::backend_failure;
    bool exists;
    std::string value; // unused, but can't pass nullptr
    if (!db->KeyMayExist({}, key, &value, &exists))
      return false;
    if (exists)
      return true;
    auto status = db->Get(rocksdb::ReadOptions{}, key, nullptr);
    if (status.IsNotFound())
      return false;
    if (!status.ok()) {
      BROKER_ERROR("failed to lookup value:" << status.ToString());
      return ec::backend_failure;
    }
    return true;
  }

  template <class Key>
  expected<void> erase(const Key& key) {
    if (!db)
      return ec::backend_failure;
    auto status = db->Delete({}, key);
    if (!status.ok()) {
      BROKER_ERROR("failed to delete key:" << status.ToString());
      return ec::backend_failure;
    }
  }

  rocksdb::DB* db = nullptr;
  count exact_size_threshold = 10000;
  std::string path;
};

rocksdb_backend::rocksdb_backend(backend_options opts)
  : impl_{std::make_unique<impl>()} {
  // Parse required options.
  auto i = opts.find("path");
  if (i == opts.end())
    return;
  auto path = caf::get_if<std::string>(&i->second);
  if (!path)
    return;
  impl_->path = *path;
  // Parse optional options.
  i = opts.find("exact-size-threshold");
  if (i != opts.end()) {
    if (auto exact_size_threshold = caf::get_if<count>(&i->second))
      impl_->exact_size_threshold = *exact_size_threshold;
    else
      BROKER_ERROR("exact-size-threshold must be of type count");
  }

  open_db();
}

bool rocksdb_backend::open_db() {
  auto dir = detail::dirname(impl_->path);
  if (dir.empty()) {
    BROKER_ERROR("dirname returned an empty path for:" << impl_->path);
    return false;
  } else if (!detail::exists(dir) && !detail::mkdirs(dir)) {
    BROKER_ERROR("failed to create database dir:" << dir.string());
    return false;
  }
  rocksdb::Options rocks_opts;
  rocks_opts.create_if_missing = true;
  auto status = rocksdb::DB::Open(rocks_opts, impl_->path.c_str(), &impl_->db);
  if (!status.ok()) {
    BROKER_ERROR("failed to open DB:" << status.ToString());
    impl_->db = nullptr;
    return false;
  }
  // Check/write the broker version.
  status = impl_->db->Put({}, "mbroker_version", version::string());
  if (!status.ok()) {
    BROKER_ERROR("failed to open DB:" << status.ToString());
    delete impl_->db;
    impl_->db = nullptr;
    return false;
  }

  return true;
}

rocksdb_backend::~rocksdb_backend() {
  if (impl_->db)
    delete impl_->db;
}

expected<void> rocksdb_backend::put(const data& key, data value,
                                    optional<timestamp> expiry) {
  if (!impl_->db)
    return ec::backend_failure;
  auto base64_key = to_base64_key<prefix::data>(key);
  auto base64_value = to_base64_value(value);
  if (!impl_->put(base64_key, base64_value, expiry))
    return ec::backend_failure;
  return {};
}

expected<void> rocksdb_backend::add(const data& key, const data& value,
                                    data::type init_type,
                                    optional<timestamp> expiry) {
  auto base64_key = to_base64_key<prefix::data>(key);
  auto base64_value = impl_->get(base64_key);
  broker::data v;
  if (!base64_value) {
    if (base64_value.error() != ec::no_such_key)
      return base64_value.error();
    v = data::from_type(init_type);
  } else {
    v = from_base64_value<data>(*base64_value);
  }
  auto result = caf::visit(adder{value}, v);
  if (!result)
    return result;
  if (!impl_->put(base64_key, to_base64_value(v), expiry))
    return ec::backend_failure;
  return {};
}

expected<void> rocksdb_backend::subtract(const data& key, const data& value,
                                         optional<timestamp> expiry) {
  auto base64_key = to_base64_key<prefix::data>(key);
  auto base64_value = impl_->get(base64_key);
  if (!base64_value)
    return base64_value.error();
  auto v = from_base64_value<data>(*base64_value);
  auto result = caf::visit(remover{value}, v);
  if (!result)
    return result;
  *base64_value = to_base64_value(v);
  if (!impl_->put(base64_key, *base64_value, expiry))
    return ec::backend_failure;
  return {};
}

expected<void> rocksdb_backend::erase(const data& key) {
  if (!impl_->db)
    return ec::backend_failure;
  rocksdb::WriteBatch batch;
  auto base64_key = to_base64_key<prefix::data>(key);
  batch.Delete(base64_key);
  base64_key[0] = static_cast<char>(prefix::expiry);
  batch.Delete(base64_key);
  auto status = impl_->db->Write({}, &batch);
  if (!status.ok()) {
    BROKER_ERROR("failed to delete key:" << status.ToString());
    return ec::backend_failure;
  }
  return {};
}

expected<void> rocksdb_backend::clear() {
  if (!impl_->db)
    return ec::backend_failure;
  std::string path = impl_->path;
  delete impl_->db;
  impl_->db = nullptr;
  auto status = rocksdb::DestroyDB(path.c_str(), rocksdb::Options());
  if (!status.ok()) {
    BROKER_ERROR("failed to destroy DB:" << status.ToString());
    return ec::backend_failure;
  }
  if (!open_db()) {
    BROKER_ERROR("failed to reopen DB");
    return ec::backend_failure;
  }
  return {};
}

expected<bool> rocksdb_backend::expire(const data& key, timestamp ts) {
  auto base64_key = to_base64_key<prefix::expiry>(key);
  auto expiry_blob = impl_->get(base64_key);
  if (!expiry_blob) {
    if (expiry_blob == ec::no_such_key)
      return false;
    return expiry_blob.error();
  }
  auto expiry = from_base64_value<timestamp>(*expiry_blob);
  if (ts < expiry)
    return false;
  rocksdb::WriteBatch batch;
  batch.Delete(base64_key);
  base64_key[0] = static_cast<char>(prefix::data);
  batch.Delete(base64_key);
  auto status = impl_->db->Write({}, &batch);
  if (!status.ok()) {
    BROKER_ERROR("failed to delete key:" << status.ToString());
    return ec::backend_failure;
  }
  return true;
}

expected<data> rocksdb_backend::get(const data& key) const {
  if (auto base64_value = impl_->get(to_base64_key<prefix::data>(key)))
    return from_base64_value<data>(*base64_value);
  else
    return base64_value.error();
}

expected<data> rocksdb_backend::keys() const {
  if (!impl_->db)
    return ec::backend_failure;
  set result;
  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  auto i = std::unique_ptr<rocksdb::Iterator>{impl_->db->NewIterator(opts)};
  static const auto pfx = static_cast<char>(prefix::data);
  i->Seek(rocksdb::Slice{&pfx, 1}); // initializes iterator
  while (i->Valid() && i->key()[0] == pfx) {
    auto key = from_base64_key<prefix::data>(i->key());
    result.insert(std::move(key));
    i->Next();
  }
  if (!i->status().ok()) {
    BROKER_ERROR("failed to get keys:" << i->status().ToString());
    return ec::backend_failure;
  }
  return {std::move(result)};
}

expected<bool> rocksdb_backend::exists(const data& key) const {
  return impl_->exists(to_base64_key<prefix::data>(key));
}

expected<uint64_t> rocksdb_backend::size() const {
  if (!impl_->db)
    return ec::backend_failure;
  uint64_t result;
  if (!impl_->db->GetIntProperty("rocksdb.estimate-num-keys", &result))
    return ec::backend_failure;
  if (result > impl_->exact_size_threshold)
    return result;
  result = 0;
  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  auto i = std::unique_ptr<rocksdb::Iterator>{impl_->db->NewIterator(opts)};
  static const auto data_prefix = static_cast<char>(prefix::data);
  i->Seek(rocksdb::Slice{&data_prefix, 1}); // initializes iterator
  while (i->Valid() && i->key()[0] == data_prefix) {
    ++result;
    i->Next();
  }
  if (!i->status().ok()) {
    BROKER_ERROR("failed to compute size:" << i->status().ToString());
    return ec::backend_failure;
  }
  return result;
}

expected<snapshot> rocksdb_backend::snapshot() const {
  if (!impl_->db)
    return ec::backend_failure;
  broker::snapshot result;
  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  auto i = std::unique_ptr<rocksdb::Iterator>{impl_->db->NewIterator(opts)};
  static const auto pfx = static_cast<char>(prefix::data);
  i->Seek(rocksdb::Slice{&pfx, 1}); // initializes iterator
  while (i->Valid() && i->key()[0] == pfx) {
    auto key = from_base64_key<prefix::data>(i->key());
    auto value = from_base64_value<data>(i->value());
    result.emplace(std::move(key), std::move(value));
    i->Next();
  }
  if (!i->status().ok()) {
    BROKER_ERROR("failed to compute size:" << i->status().ToString());
    return ec::backend_failure;
  }
  return {std::move(result)};
}

expected<expirables> rocksdb_backend::expiries() const {
  if (!impl_->db)
    return ec::backend_failure;
  expirables result;
  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  auto i = std::unique_ptr<rocksdb::Iterator>{impl_->db->NewIterator(opts)};
  static const auto pfx = static_cast<char>(prefix::expiry);
  i->Seek(rocksdb::Slice{&pfx, 1}); // initializes iterator
  while (i->Valid() && i->key()[0] == pfx) {
    auto key = from_base64_key<prefix::expiry>(i->key());
    auto expiry = from_base64_value<timestamp>(i->value());
    auto e = expirable(std::move(key), std::move(expiry));
    result.emplace_back(std::move(e));
    i->Next();
  }
  if (!i->status().ok()) {
    BROKER_ERROR("failed to compute size:" << i->status().ToString());
    return ec::backend_failure;
  }
  return {std::move(result)};
}

} // namespace broker::detail
