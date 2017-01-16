#ifndef BROKER_DETAIL_MEMORY_BACKEND_HH
#define BROKER_DETAIL_MEMORY_BACKEND_HH

#include <unordered_map>

#include "broker/backend_options.hh"

#include "broker/detail/abstract_backend.hh"

namespace broker {
namespace detail {

/// An in-memory key-value storage backend.
class memory_backend : public abstract_backend {
public:
  /// Constructs a memory backend.
  /// @param opts The options controlling the backend behavior.
  memory_backend(backend_options opts = backend_options{});

  result<void> put(const data& key, data value,
                   optional<timestamp> expiry) override;

  result<void> add(const data& key, const data& value,
                   optional<timestamp> expiry) override;

  result<void> remove(const data& key, const data& value,
                      optional<timestamp> expiry) override;

  result<void> erase(const data& key) override;

  result<bool> expire(const data& key) override;

  result<data> get(const data& key) const override;

  result<data> get(const data& key, const data& value) const override;

  result<bool> exists(const data& key) const override;

  result<uint64_t> size() const override;

  result<broker::snapshot> snapshot() const override;

private:
  backend_options options_;
  std::unordered_map<data, std::pair<data, optional<timestamp>>> store_;
  std::unordered_map<data, timestamp> expirations_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_MEMORY_BACKEND_HH
