#include "broker/logger.hh"

#include <caf/all.hpp>

#include "broker/store.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/die.hh"
#include "broker/detail/flare_actor.hh"

namespace broker {

store::proxy::proxy(store& s) : frontend_{s.frontend_} {
  proxy_ = frontend_.home_system().spawn<detail::flare_actor>();
}

request_id store::proxy::get(data key) {
  if (!frontend_)
    return 0;
  send_as(proxy_, frontend_, atom::get::value, std::move(key), ++id_);
  return id_;
}

mailbox store::proxy::mailbox() {
  return detail::make_mailbox(caf::actor_cast<detail::flare_actor*>(proxy_));
}

store::response store::proxy::receive() {
  auto resp = response{error{}, 0};
  caf::actor_cast<caf::blocking_actor*>(proxy_)->receive(
    [&](data& x, request_id id) {
      resp = {std::move(x), id};
    },
    [&](caf::error& e, request_id id) {
      BROKER_ERROR("proxy failed to receive response from store");
      resp = {std::move(e), id};
    }
  );
  return resp;
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

expected<data> store::get(data key) const {
  return request<data>(atom::get::value, std::move(key));
}

expected<data> store::get(data key, data aspect) const {
  return request<data>(atom::get::value, std::move(key), std::move(aspect));
}

void store::put(data key, data value, optional<timespan> expiry) const {
  if (frontend_)
    anon_send(frontend_, atom::put::value, std::move(key), std::move(value),
              expiry);
}

void store::erase(data key) const {
  if (frontend_)
    anon_send(frontend_, atom::erase::value, std::move(key));
}

void store::add(data key, data value, optional<timespan> expiry) const {
  if (frontend_)
    anon_send(frontend_, atom::add::value, std::move(key), std::move(value),
              expiry);
}

void store::subtract(data key, data value, optional<timespan> expiry) const {
  if (frontend_)
    anon_send(frontend_, atom::subtract::value, std::move(key),
              std::move(value), expiry);
}

store::store(caf::actor actor) : frontend_{std::move(actor)} {
  // nop
}

} // namespace broker
