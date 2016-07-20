#include <caf/all.hpp>

#include "broker/store.hh"

#include "broker/detail/die.hh"

namespace broker {

namespace {

constexpr auto zero = count{0};

} // namespace <anonymous>

store::store(caf::actor actor) : frontend_{std::move(actor)} {
  // nop
}

std::string store::name() const {
  std::string result;
  caf::scoped_actor self{frontend_->home_system()};
  self->request(frontend_, timeout::frontend, atom::get::value).receive(
    [&](std::string& name) {
      result = name;
    },
    [&](caf::error& e) {
      detail::die("failed to retrieve store name:", to_string(e));
    }
  );
  return result;
}

void store::put(data key, data value) const {
  caf::anon_send(frontend_, atom::put::value, std::move(key), std::move(value),
                 zero);
}

void store::erase(data key) const {
  caf::anon_send(frontend_, atom::erase::value, std::move(key), count{0});
}

void store::increment(data key, data value) const {
  add(std::move(key), std::move(value));
}

void store::decrement(data key, data value) const {
  remove(std::move(key), std::move(value));
}

void store::add(data key, data value) const {
  caf::anon_send(frontend_, atom::add::value, std::move(key),
                 std::move(value), zero);
}

void store::remove(data key, data value) const {
  caf::anon_send(frontend_, atom::remove::value, std::move(key),
                 std::move(value), zero);
}

} // namespace broker
