#include "broker/store/backend.hh"

namespace broker {
namespace store {
  
backend::~backend() = default;

std::string backend::last_error() const {
  return do_last_error();
}

bool backend::init(snapshot sss) {
  return do_init(std::move(sss));
}

const sequence_num& backend::sequence() const {
  return do_sequence();
}

bool backend::insert(data k, data v, maybe<expiration_time> t) {
  if (!do_insert(std::move(k), std::move(v), std::move(t)))
    return false;

  do_increase_sequence();
  return true;
}

modification_result backend::increment(const data& k, int64_t by, 
                                       double mod_time) {
  auto rc = do_increment(k, by, mod_time);
  if (rc.stat == modification_result::status::success)
    do_increase_sequence();
  return rc;
}

modification_result backend::add_to_set(const data& k, data element, 
                                        double mod_time) {
  auto rc = do_add_to_set(k, std::move(element), mod_time);
  if (rc.stat == modification_result::status::success)
    do_increase_sequence();
  return rc;
}

modification_result backend::remove_from_set(const data& k, const data&
                                             element, double mod_time) {
  auto rc = do_remove_from_set(k, element, mod_time);
  if (rc.stat == modification_result::status::success)
    do_increase_sequence();
  return rc;
}

bool backend::erase(const data& k) {
  if (!do_erase(k))
    return false;
  do_increase_sequence();
  return true;
}

bool backend::expire(const data& k, const expiration_time& expiration) {
  if (!do_expire(k, expiration))
    return false;
  do_increase_sequence();
  return true;
}

bool backend::clear() {
  if (!do_clear())
    return false;
  do_increase_sequence();
  return true;
}

modification_result backend::push_left(const data& k, vector items,
                                       double mod_time) {
  auto rc = do_push_left(k, std::move(items), mod_time);
  if (rc.stat == modification_result::status::success)
    do_increase_sequence();
  return rc;
}

modification_result backend::push_right(const data& k, vector items,
                                        double mod_time) {
  auto rc = do_push_right(k, std::move(items), mod_time);
  if (rc.stat == modification_result::status::success)
    do_increase_sequence();
  return rc;
}

std::pair<modification_result, maybe<data>>
backend::pop_left(const data& k, double mod_time) {
  auto rc = do_pop_left(k, mod_time);
  if (rc.first.stat == modification_result::status::success && rc.second)
    do_increase_sequence();
  return rc;
}

std::pair<modification_result, maybe<data>>
backend::pop_right(const data& k, double mod_time) {
  auto rc = do_pop_right(k, mod_time);
  if (rc.first.stat == modification_result::status::success && rc.second)
    do_increase_sequence();
  return rc;
}

maybe<maybe<data>> backend::lookup(const data& k) const {
  return do_lookup(k);
}

maybe<bool> backend::exists(const data& k) const {
  return do_exists(k);
}

maybe<std::vector<data>> backend::keys() const {
  return do_keys();
}

maybe<uint64_t> backend::size() const {
  return do_size();
}

maybe<snapshot> backend::snap() const {
  return do_snap();
}

maybe<std::deque<expirable>> backend::expiries() const {
  return do_expiries();
}

} // namespace store
} // namespace broker
