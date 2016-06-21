#include "broker/context.hh"

namespace broker {
namespace {

} // namespace <anonymous>

context::context(configuration config)
  : system_{std::move(config)} {
}

} // namespace broker
