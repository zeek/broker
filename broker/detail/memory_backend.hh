#ifndef BROKER_DETAIL_MEMORY_BACKEND_HH
#define BROKER_DETAIL_MEMORY_BACKEND_HH

#include <unordered_map>

#include "broker/detail/abstract_backend.hh"

namespace broker {
namespace detail {

/// An in-memory key-value storage backend.
class memory_backend : public abstract_backend {
public:
  expected<void> put(const data& key, data value,
                     optional<time::point> expiry) override;

  expected<void> add(const data& key, const data& value,
                     optional<time::point> expiry) override;

  expected<void> remove(const data& key, const data& value,
                        optional<time::point> expiry) override;

  expected<bool> erase(const data& key) override;

  expected<bool> expire(const data& key) override;

  expected<data> get(const data& key) const override;

  expected<data> get(const data& key, const data& value) const override;

  expected<bool> exists(const data& key) const override;

  expected<uint64_t> size() const override;

  expected<broker::snapshot> snapshot() const override;

private:
  std::unordered_map<data, std::pair<data, optional<time::point>>> store_;
  std::unordered_map<data, time::point> expirations_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_MEMORY_BACKEND_HH
