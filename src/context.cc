#include "broker/context.hh"

namespace broker {
namespace {

} // namespace <anonymous>

context::context(configuration config)
  : config_{std::move(config)},
    system_{config_} {
  // nop
}

} // namespace broker
