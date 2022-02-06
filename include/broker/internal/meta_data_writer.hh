#pragma once

#include <cstdio>

#include <type_traits>
#include <unordered_map>

#include <caf/error.hpp>
#include <caf/fwd.hpp>
#include <caf/none.hpp>

#include "broker/data.hh"
#include "broker/entity_id.hh"
#include "broker/error.hh"
#include "broker/fwd.hh"

namespace broker::internal {

/// Writes meta information (type and size) of Broker ::data to a serializer.
class meta_data_writer {
public:
  static constexpr bool is_loading = false;

  explicit meta_data_writer(caf::binary_serializer& sink);

  error operator()(const data& x);

  error operator()(const internal_command& x);

private:
  caf::binary_serializer& sink_;
};

} // namespace broker::internal
