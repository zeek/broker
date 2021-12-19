#pragma once

#include <string_view>

#include <caf/telemetry/metric_type.hpp>

#include "broker/data.hh"
#include "broker/detail/assert.hh"

namespace broker::internal {

/// Provides a view into a `broker::data` that represents a Broker metric.
class metric_view {
public:
  // -- constants --------------------------------------------------------------

  enum class field : size_t {
    prefix,
    name,
    type,
    unit,
    helptext,
    is_sum,
    labels,
    value,
    end_of_row,
  };

  static constexpr size_t index(field x) noexcept {
    return static_cast<size_t>(x);
  }

  static constexpr size_t row_size = static_cast<size_t>(field::end_of_row);

  // -- constructors, destructors, and assignment operators --------------------

  explicit metric_view(const vector* row);

  explicit metric_view(const vector& row);

  explicit metric_view(const data& row_data);

  metric_view(const metric_view&) noexcept = default;

  metric_view& operator=(const metric_view&) noexcept = default;

  // -- properties -------------------------------------------------------------

  bool valid() const noexcept {
    return row_ != nullptr;
  }

  explicit operator bool() const noexcept {
    return valid();
  }

  /// @pre `valid()`
  const std::string& prefix() const {
    return {get<std::string>(field::prefix)};
  }

  /// @pre `valid()`
  const std::string& name() const {
    return {get<std::string>(field::name)};
  }

  /// @pre `valid()`
  caf::telemetry::metric_type type() const noexcept {
    return type_;
  }

  /// @pre `valid()`
  const std::string& type_str() const {
    return {get<std::string>(field::type)};
  }

  /// @pre `valid()`
  const std::string& unit() const {
    return {get<std::string>(field::unit)};
  }

  /// @pre `valid()`
  const std::string& helptext() const {
    return {get<std::string>(field::helptext)};
  }

  /// @pre `valid()`
  bool is_sum() const {
    return get<bool>(field::is_sum);
  }

  /// @pre `valid()`
  const table& labels() const {
    return get<table>(field::labels);
  }

  /// @pre `valid()`
  const data& value() const {
    return (*row_)[index(field::value)];
  }

private:
  static bool has_properly_typed_labels(const vector& row) noexcept;

  static bool get_type(const vector& row,
                       caf::telemetry::metric_type& var) noexcept;

  template <class T>
  const T& get(field x) const {
    BROKER_ASSERT(row_ != nullptr);
    return broker::get<T>((*row_)[index(x)]);
  }

  const vector* row_;
  caf::telemetry::metric_type type_;
};

} // namespace broker::internal
