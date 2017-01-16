#ifndef BROKER_DETAIL_SQLITE_BACKEND_HH
#define BROKER_DETAIL_SQLITE_BACKEND_HH

#include "broker/backend_options.hh"

#include "broker/detail/abstract_backend.hh"

namespace broker {
namespace detail {

/// A SQLite storage backend.
class sqlite_backend : public abstract_backend {
public:
  /// Constructs a SQLite backend.
  /// @param opts The options to create/open a database.
  /// Required parameters:
  ///   - `path`: a `std::string` representing the location of the database on
  ///             the filesystem.
  sqlite_backend(backend_options opts = backend_options{});

  ~sqlite_backend();

  result<void> put(const data& key, data value,
                   optional<timestamp> expiry) override;

  result<void> add(const data& key, const data& value,
                   optional<timestamp> expiry) override;

  result<void> remove(const data& key, const data& value,
                      optional<timestamp> expiry) override;

  result<void> erase(const data& key) override;

  result<bool> expire(const data& key) override;

  result<data> get(const data& key) const override;

  result<bool> exists(const data& key) const override;

  result<uint64_t> size() const override;

  result<broker::snapshot> snapshot() const override;

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_SQLITE_BACKEND_HH
