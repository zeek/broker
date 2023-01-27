#pragma once
#include <string>
#include <vector>

#include "broker/backend_options.hh"
#include "broker/detail/abstract_backend.hh"
#include "broker/expected.hh"

namespace broker::detail {

/// A SQLite storage backend.
class sqlite_backend : public abstract_backend {
public:
  /// Constructs a SQLite backend.
  /// @param opts The options to create/open a database.
  /// Required parameters:
  ///   - `path`: a `std::string` representing the location of the database on
  ///             the filesystem.
  /// Optional parameters:
  ///   - `synchronous`: a `broker::enum_value` representing the value
  ///                    to be used for PRAGMA synchronous.
  ///   - `journal_mode`: a `broker::enum_value` representing the value
  ///                    to be used for PRAGMA journal_mode.
  ///   - `failure_mode`: a `broker::enum_value` allowing
  ///                     "Broker::SQLITE_FAILURE_MODE_DELETE" to indicate
  ///                     deletion of the database file when initialization
  ///                     fails is acceptable.
  ///   - `integrity_check`: a `broker::boolean` toggling PRAGMA integrity_check
  ///                        execution during initialization.
  sqlite_backend(backend_options opts = backend_options{});

  ~sqlite_backend() override;

  bool init_failed() const;

  expected<void> put(const data& key, data value,
                     std::optional<timestamp> expiry) override;

  expected<void> add(const data& key, const data& value, data::type init_type,
                     std::optional<timestamp> expiry) override;

  expected<void> subtract(const data& key, const data& value,
                          std::optional<timestamp> expiry) override;

  expected<void> erase(const data& key) override;

  expected<void> clear() override;

  expected<bool> expire(const data& key, timestamp current_time) override;

  expected<data> get(const data& key) const override;

  expected<bool> exists(const data& key) const override;

  expected<uint64_t> size() const override;

  expected<data> keys() const override;

  expected<broker::snapshot> snapshot() const override;

  expected<expirables> expiries() const override;

  /// Run PRAGAMA command with an optional value.
  /// @param name The name of the PRAGMA to run.
  /// @param value An optional value for the PRAGMA.
  /// @param messages Pointer to vector for collecting the output.
  /// @returns True on success, false on error.
  bool exec_pragma(std::string_view name,
                   std::string_view value = std::string{},
                   std::vector<std::string>* messages = nullptr);

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace broker::detail
