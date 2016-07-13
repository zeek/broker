#include <caf/all.hpp>

#include "broker/store/frontend.hh"

#include "broker/detail/die.hh"

namespace broker {
namespace store {

namespace {

constexpr auto zero = count{0};

} // namespace <anonymous>

frontend::frontend(caf::actor actor) : frontend_{std::move(actor)} {
  // nop
}

std::string frontend::name() const {
  std::string result;
  caf::scoped_actor self{frontend_->home_system()};
  self->request(frontend_, timeout::frontend, atom::get::value).receive(
    [&](std::string& name) {
      result = name;
    },
    [&](caf::error& e) {
      detail::die("failed to retrieve frontend name:", to_string(e));
    }
  );
  return result;
}

void frontend::put(data key, data value) const {
  caf::anon_send(frontend_, atom::put::value, std::move(key), std::move(value),
                 zero);
}

void frontend::erase(data key) const {
  caf::anon_send(frontend_, atom::erase::value, std::move(key), count{0});
}

void frontend::increment(data key, data value) const {
  add(std::move(key), std::move(value));
}

void frontend::decrement(data key, data value) const {
  remove(std::move(key), std::move(value));
}

void frontend::add(data key, data value) const {
  caf::anon_send(frontend_, atom::add::value, std::move(key),
                 std::move(value), zero);
}

void frontend::remove(data key, data value) const {
  caf::anon_send(frontend_, atom::remove::value, std::move(key),
                 std::move(value), zero);
}

} // namespace store
} // namespace broker
