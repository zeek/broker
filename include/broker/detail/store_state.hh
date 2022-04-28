#pragma once

#include <memory>

namespace broker::detail {

class store_state {
public:
  virtual ~store_state();
};

using shared_store_state_ptr = std::shared_ptr<store_state>;

using weak_store_state_ptr = std::weak_ptr<store_state>;

} // namespace broker::detail
