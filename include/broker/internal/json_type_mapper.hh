#pragma once

#include "caf/type_id.hpp"

namespace broker::internal {

/// Translates between human-readable type names and type IDs.
class json_type_mapper : public caf::type_id_mapper {
public:
  caf::string_view operator()(caf::type_id_t type) const override;

  caf::type_id_t operator()(caf::string_view name) const override;
};

} // namespace broker::internal
