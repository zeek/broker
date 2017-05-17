#include "broker/logger.hh"

#include <caf/all.hpp>

#include "broker/store.hh"

#include "broker/internal_command.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/die.hh"
#include "broker/detail/flare_actor.hh"

using namespace broker::detail;

namespace broker {

store::proxy::proxy(store& s) : frontend_{s.frontend_} {
  proxy_ = frontend_.home_system().spawn<flare_actor>();
}

request_id store::proxy::get(data key) {
  if (!frontend_)
    return 0;
  send_as(proxy_, frontend_, atom::get::value, std::move(key), ++id_);
  return id_;
}

request_id store::proxy::keys() {
  if (!frontend_)
    return 0;
  send_as(proxy_, frontend_, atom::keys::value, ++id_);
  return id_;
}

mailbox store::proxy::mailbox() {
  return make_mailbox(caf::actor_cast<flare_actor*>(proxy_));
}

store::response store::proxy::receive() {
  auto resp = response{error{}, 0};
  caf::actor_cast<caf::blocking_actor*>(proxy_)->receive(
    [&](data& x, request_id id) {
      resp = {std::move(x), id};
    },
    [&](caf::error& e, request_id id) {
      BROKER_ERROR("proxy failed to receive response from store" << id);
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
      die("failed to retrieve store name:", to_string(e));
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

expected<data> store::keys() const {
  // TODO: implement me
  return caf::sec::bad_function_call;
  //return request<data>(atom::keys::value);
}

void store::put(data key, data value, optional<timespan> expiry) const {
  anon_send(frontend_, atom::local::value,
            make_internal_command<put_command>(
              std::move(key), std::move(value), expiry));
}

void store::erase(data key) const {
  anon_send(frontend_, atom::local::value,
            make_internal_command<erase_command>(std::move(key)));
}

void store::add(data key, data value, optional<timespan> expiry) const {
  anon_send(frontend_, atom::local::value,
            make_internal_command<add_command>(std::move(key), std::move(value),
                                               expiry));
}

void store::subtract(data key, data value, optional<timespan> expiry) const {
  anon_send(frontend_, atom::local::value,
            make_internal_command<subtract_command>(std::move(key),
                                                    std::move(value), expiry));
}

void store::clear() const {
  if (frontend_)
    anon_send(frontend_, atom::clear::value);
}

store::store(caf::actor actor) : frontend_{std::move(actor)} {
  // nop
}

} // namespace broker
