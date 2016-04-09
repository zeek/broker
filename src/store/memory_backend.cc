#include "broker/store/memory_backend.hh"

#include "../util/misc.hh"

namespace broker {
namespace store {

void memory_backend::do_increase_sequence() {
  ++sn_;
}

std::string memory_backend::do_last_error() const {
  return last_error_;
}

bool memory_backend::do_init(snapshot sss) {
  sn_ = std::move(sss.sn);
  datastore_.clear();
  for (auto& e : sss.entries)
    datastore_.emplace(std::move(e));
  last_error_.clear();
  return true;
}

const sequence_num& memory_backend::do_sequence() const {
  return sn_;
}

bool memory_backend::do_insert(
  data k, data v, maybe<expiration_time> t) {
  datastore_[std::move(k)] = value{std::move(v), std::move(t)};
  return true;
}

modification_result memory_backend::do_increment(const data& k, int64_t by,
                                                 double mod_time) {
  auto it = datastore_.find(k);
  if (it == datastore_.end()) {
    datastore_[k] = value{by, {}};
    return {modification_result::status::success, {}};
  }
  if (util::increment_data(it->second.item, by, &last_error_))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

modification_result memory_backend::do_add_to_set(const data& k, data element,
                                                  double mod_time) {
  auto it = datastore_.find(k);
  if (it == datastore_.end()) {
    datastore_[k] = value{set{std::move(element)}, {}};
    return {modification_result::status::success, {}};
  }
  if (util::add_data_to_set(it->second.item, std::move(element),
                            &last_error_))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

modification_result memory_backend::do_remove_from_set(const data& k,
                                                       const data& element,
                                                       double mod_time) {
  auto it = datastore_.find(k);
  if (it == datastore_.end()) {
    datastore_[k] = value{set{}, {}};
    return {modification_result::status::success, {}};
  }
  if (util::remove_data_from_set(it->second.item, element, &last_error_))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

bool memory_backend::do_erase(const data& k) {
  datastore_.erase(k);
  return true;
}

bool memory_backend::do_expire(
  const data& k, const expiration_time& expiration) {
  auto it = datastore_.find(k);
  if (it == datastore_.end())
    return true;
  if (it->second.expiry == expiration)
    datastore_.erase(it);
  return true;
}

bool memory_backend::do_clear() {
  datastore_.clear();
  return true;
}

modification_result memory_backend::do_push_left(const data& k, vector items,
                                                 double mod_time) {
  auto it = datastore_.find(k);
  if (it == datastore_.end()) {
    datastore_[k] = value{std::move(items), {}};
    return {modification_result::status::success, {}};
  }
  if (util::push_left(it->second.item, std::move(items), &last_error_))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

modification_result memory_backend::do_push_right(const data& k, vector items,
                                                  double mod_time) {
  auto it = datastore_.find(k);
  if (it == datastore_.end()) {
    datastore_[k] = value{std::move(items), {}};
    return {modification_result::status::success, {}};
  }
  if (util::push_right(it->second.item, std::move(items), &last_error_))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

std::pair<modification_result, maybe<data>>
memory_backend::do_pop_left(const data& k, double mod_time) {
  auto it = datastore_.find(k);
  if (it == datastore_.end())
    return {{modification_result::status::success, {}}, {}};
  auto ood = util::pop_left(it->second.item, &last_error_, true);
  if (!ood)
    return {{modification_result::status::invalid, {}}, {}};
  return {{modification_result::status::success,
           util::update_last_modification(it->second.expiry, mod_time)},
          std::move(*ood)};
}

std::pair<modification_result, maybe<data>>
memory_backend::do_pop_right(const data& k, double mod_time) {
  auto it = datastore_.find(k);
  if (it == datastore_.end())
    return {{modification_result::status::success, {}}, {}};
  auto ood = util::pop_right(it->second.item, &last_error_, true);
  if (!ood)
    return {{modification_result::status::invalid, {}}, {}};
  return {{modification_result::status::success,
           util::update_last_modification(it->second.expiry, mod_time)},
          std::move(*ood)};
}

maybe<maybe<data>> memory_backend::do_lookup(const data& k) const {
  try {
    return {datastore_.at(k).item};
  } catch (const std::out_of_range&) {
    return {maybe<data>{}};
  }
}

maybe<bool> memory_backend::do_exists(const data& k) const {
  if (datastore_.find(k) == datastore_.end())
    return false;
  else
    return true;
}

maybe<std::vector<data>> memory_backend::do_keys() const {
  std::vector<data> rval;
  for (const auto& kv : datastore_)
    rval.emplace_back(kv.first);
  return rval;
}

maybe<uint64_t> memory_backend::do_size() const {
  return datastore_.size();
}

maybe<snapshot> memory_backend::do_snap() const {
  snapshot rval;
  rval.sn = sn_;
  for (const auto& e : datastore_)
    rval.entries.emplace_back(e);
  return rval;
}

maybe<std::deque<expirable>> memory_backend::do_expiries() const {
  std::deque<expirable> rval;
  for (const auto& entry : datastore_)
    if (entry.second.expiry)
      rval.push_back({entry.first, *entry.second.expiry});
  return rval;
}

} // namespace store
} // namespace broker
