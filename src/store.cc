#include <caf/all.hpp>

#include "broker/store.hh"

#include "broker/detail/die.hh"

namespace broker {

store::store(caf::actor actor) : frontend_{std::move(actor)} {
  // nop
}

std::string store::name() const {
  std::string result;
  caf::scoped_actor self{frontend_->home_system()};
  self->request(frontend_, timeout::frontend, atom::get::value,
                atom::name::value).receive(
    [&](std::string& name) {
      result = name;
    },
    [&](caf::error& e) {
      detail::die("failed to retrieve store name:", to_string(e));
    }
  );
  return result;
}

void store::put(data key, data value, optional<timestamp> expiry) const {
  caf::anon_send(frontend_, atom::put::value, std::move(key), std::move(value),
                 expiry);
}

void store::add(data key, data value, optional<timestamp> expiry) const {
  caf::anon_send(frontend_, atom::add::value, std::move(key),
                 std::move(value), expiry);
}

void store::remove(data key, data value, optional<timestamp> expiry) const {
  caf::anon_send(frontend_, atom::remove::value, std::move(key),
                 std::move(value), expiry);
}

void store::erase(data key) const {
  caf::anon_send(frontend_, atom::erase::value, std::move(key));
}

void store::increment(data key, data value,
                      optional<timestamp> expiry) const {
  add(std::move(key), std::move(value), expiry);
}

void store::decrement(data key, data value,
                      optional<timestamp> expiry) const {
  remove(std::move(key), std::move(value), expiry);
}

} // namespace broker
