#include "memory_backend_impl.hh"
#include "../util/misc.hh"

namespace broker {
namespace store {

memory_backend::memory_backend() : pimpl(new impl) {
}

memory_backend::~memory_backend() = default;

memory_backend::memory_backend(memory_backend& other)
  : pimpl(new impl(*other.pimpl)) {
}

memory_backend::memory_backend(memory_backend&& other)
  : pimpl(std::move(other.pimpl)) {
}

memory_backend& memory_backend::
operator=(memory_backend other) {
  using std::swap;
  swap(pimpl, other.pimpl);
  return *this;
}

void memory_backend::do_increase_sequence() {
  ++pimpl->sn;
}

std::string memory_backend::do_last_error() const {
  return pimpl->last_error;
}

bool memory_backend::do_init(snapshot sss) {
  *pimpl = {};
  pimpl->sn = std::move(sss.sn);
  for (auto& e : sss.entries)
    pimpl->datastore.emplace(std::move(e));
  return true;
}

const sequence_num& memory_backend::do_sequence() const {
  return pimpl->sn;
}

bool memory_backend::do_insert(
  data k, data v, maybe<expiration_time> t) {
  pimpl->datastore[std::move(k)] = value{std::move(v), std::move(t)};
  return true;
}

modification_result memory_backend::do_increment(const data& k, int64_t by,
                                                 double mod_time) {
  auto it = pimpl->datastore.find(k);
  if (it == pimpl->datastore.end()) {
    pimpl->datastore[k] = value{by, {}};
    return {modification_result::status::success, {}};
  }
  if (util::increment_data(it->second.item, by, &pimpl->last_error))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

modification_result memory_backend::do_add_to_set(const data& k, data element,
                                                  double mod_time) {
  auto it = pimpl->datastore.find(k);
  if (it == pimpl->datastore.end()) {
    pimpl->datastore[k] = value{set{std::move(element)}, {}};
    return {modification_result::status::success, {}};
  }
  if (util::add_data_to_set(it->second.item, std::move(element),
                            &pimpl->last_error))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

modification_result memory_backend::do_remove_from_set(const data& k,
                                                       const data& element,
                                                       double mod_time) {
  auto it = pimpl->datastore.find(k);
  if (it == pimpl->datastore.end()) {
    pimpl->datastore[k] = value{set{}, {}};
    return {modification_result::status::success, {}};
  }
  if (util::remove_data_from_set(it->second.item, element, &pimpl->last_error))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

bool memory_backend::do_erase(const data& k) {
  pimpl->datastore.erase(k);
  return true;
}

bool memory_backend::do_expire(
  const data& k, const expiration_time& expiration) {
  auto it = pimpl->datastore.find(k);
  if (it == pimpl->datastore.end())
    return true;
  if (it->second.expiry == expiration)
    pimpl->datastore.erase(it);
  return true;
}

bool memory_backend::do_clear() {
  pimpl->datastore.clear();
  return true;
}

modification_result memory_backend::do_push_left(const data& k, vector items,
                                                 double mod_time) {
  auto it = pimpl->datastore.find(k);
  if (it == pimpl->datastore.end()) {
    pimpl->datastore[k] = value{std::move(items), {}};
    return {modification_result::status::success, {}};
  }
  if (util::push_left(it->second.item, std::move(items), &pimpl->last_error))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

modification_result memory_backend::do_push_right(const data& k, vector items,
                                                  double mod_time) {
  auto it = pimpl->datastore.find(k);
  if (it == pimpl->datastore.end()) {
    pimpl->datastore[k] = value{std::move(items), {}};
    return {modification_result::status::success, {}};
  }
  if (util::push_right(it->second.item, std::move(items), &pimpl->last_error))
    return {modification_result::status::success,
            util::update_last_modification(it->second.expiry, mod_time)};
  return {modification_result::status::invalid, {}};
}

std::pair<modification_result, maybe<data>>
memory_backend::do_pop_left(const data& k, double mod_time) {
  auto it = pimpl->datastore.find(k);
  if (it == pimpl->datastore.end())
    return {{modification_result::status::success, {}}, {}};
  auto ood = util::pop_left(it->second.item, &pimpl->last_error, true);
  if (!ood)
    return {{modification_result::status::invalid, {}}, {}};
  return {{modification_result::status::success,
           util::update_last_modification(it->second.expiry, mod_time)},
          std::move(*ood)};
}

std::pair<modification_result, maybe<data>>
memory_backend::do_pop_right(const data& k, double mod_time) {
  auto it = pimpl->datastore.find(k);
  if (it == pimpl->datastore.end())
    return {{modification_result::status::success, {}}, {}};
  auto ood = util::pop_right(it->second.item, &pimpl->last_error, true);
  if (!ood)
    return {{modification_result::status::invalid, {}}, {}};
  return {{modification_result::status::success,
           util::update_last_modification(it->second.expiry, mod_time)},
          std::move(*ood)};
}

maybe<maybe<data>> memory_backend::do_lookup(const data& k) const {
  try {
    return {pimpl->datastore.at(k).item};
  } catch (const std::out_of_range&) {
    return {maybe<data>{}};
  }
}

maybe<bool> memory_backend::do_exists(const data& k) const {
  if (pimpl->datastore.find(k) == pimpl->datastore.end())
    return false;
  else
    return true;
}

maybe<std::vector<data>> memory_backend::do_keys() const {
  std::vector<data> rval;
  for (const auto& kv : pimpl->datastore)
    rval.emplace_back(kv.first);
  return rval;
}

maybe<uint64_t> memory_backend::do_size() const {
  return pimpl->datastore.size();
}

maybe<snapshot> memory_backend::do_snap() const {
  snapshot rval;
  rval.sn = pimpl->sn;
  for (const auto& e : pimpl->datastore)
    rval.entries.emplace_back(e);
  return rval;
}

maybe<std::deque<expirable>> memory_backend::do_expiries() const {
  std::deque<expirable> rval;
  for (const auto& entry : pimpl->datastore)
    if (entry.second.expiry)
      rval.push_back({entry.first, *entry.second.expiry});
  return rval;
}

} // namespace store
} // namespace broker

