#ifndef BROKER_DETAIL_MEMORY_BACKEND_HH
#define BROKER_DETAIL_MEMORY_BACKEND_HH

#include "broker/detail/abstract_backend.hh"

namespace broker {
namespace detail {

/// An in-memory key-value storage backend.
class memory_backend : public abstract_backend {
public:
  expected<void> put(const data& key, data value,
                     optional<time::point> expiry = {}) override;

  expected<void> erase(const data& key) override;

  expected<void> add(const data& key, const data& value,
                     time::point t) override;

  expected<void> remove(const data& key, const data& value,
                        time::point t) override;

  expected<void> expire(const data& key, time::point expiry) override;

  expected<data> get(const data& key) const override;

  expected<data> get(const data& key, const data& value) const override;

  expected<bool> exists(const data& key) const override;

  expected<uint64_t> size() const override;

  expected<broker::snapshot> snapshot() const override;

private:
  std::unordered_map<data, data> store_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_MEMORY_BACKEND_HH
