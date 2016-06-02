#include "broker/context.hh"
#include "broker/endpoint.hh"

namespace broker {

context::context(configuration config) : system_{std::move(config)} {
}

} // namespace broker
