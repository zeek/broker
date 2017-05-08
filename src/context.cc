#include "broker/context.hh"

#include "broker/endpoint.hh"

#include "broker/detail/core_actor.hh"

namespace broker {

context::context(configuration config)
  : config_{std::move(config)},
    system_{config_} {
  core_ = system_.spawn(detail::core_actor, detail::filter_type{});
}

endpoint context::spawn() {
  return {*this};
}

} // namespace broker
