#ifndef BROKER_STORE_MEMORY_BACKEND_HH
#define BROKER_STORE_MEMORY_BACKEND_HH

#include <deque>
#include <memory>
#include <string>
#include <unordered_map>

#include "broker/store/backend.hh"

namespace broker {
namespace store {

/// An in-memory implementation of a storage backend.
class memory_backend : public backend {
private:
  void do_increase_sequence() override;

  std::string do_last_error() const override;

  bool do_init(snapshot sss) override;

  const sequence_num& do_sequence() const override;

  bool do_insert(data k, data v, optional<expiration_time> t) override;

  modification_result do_increment(const data& k, int64_t by,
                                   double mod_time) override;

  modification_result do_add_to_set(const data& k, data element,
                                    double mod_time) override;

  modification_result do_remove_from_set(const data& k, const data& element,
                                         double mod_time) override;

  bool do_erase(const data& k) override;

  bool do_expire(const data& k, const expiration_time& expiration) override;

  bool do_clear() override;

  modification_result do_push_left(const data& k, vector items,
                                   double mod_time) override;

  modification_result do_push_right(const data& k, vector items,
                                    double mod_time) override;

  std::pair<modification_result, optional<data>>
  do_pop_left(const data& k, double mod_time) override;

  std::pair<modification_result, optional<data>>
  do_pop_right(const data& k, double mod_time) override;

  optional<optional<data>> do_lookup(const data& k) const override;

  optional<bool> do_exists(const data& k) const override;

  optional<std::vector<data>> do_keys() const override;

  optional<uint64_t> do_size() const override;

  optional<snapshot> do_snap() const override;

  optional<std::deque<expirable>> do_expiries() const override;

  sequence_num sn_;
  std::unordered_map<data, value> datastore_;
  std::string last_error_;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_MEMORY_BACKEND_HH
