#ifndef BROKER_DETAIL_SQLITE_BACKEND_HH
#define BROKER_DETAIL_SQLITE_BACKEND_HH

#include <string>

#include "broker/detail/abstract_backend.hh"

namespace broker {
namespace detail {

/// An in-memory key-value storage backend.
class sqlite_backend : public abstract_backend {
public:
  sqlite_backend();

  ~sqlite_backend();

  // --- SQLite interface ---------------------------------------------------

  expected<void> open(const std::string& path);

  // --- backend interface --------------------------------------------------

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
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SQLITE_BACKEND_HH
