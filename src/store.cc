#include "broker/logger.hh"

#include <caf/all.hpp>

#include "broker/store.hh"

#include "broker/detail/assert.hh"
#include "broker/detail/die.hh"
#include "broker/detail/flare_actor.hh"

namespace broker {

store::response::response() : data_{caf::error{}} {
}

store::response::operator bool() const {
  return !!data_;
}

data& store::response::operator*() {
  return *data_;
}

data* store::response::operator->() {
  return &*data_;
}

status store::response::status() {
  return make_status(data_.error());
}

request_id store::response::id() const {
  return id_;
}


store::proxy::proxy(store& s) : frontend_{s.frontend_} {
  proxy_ = frontend_.home_system().spawn<detail::flare_actor>();
}

request_id store::proxy::get(data key) {
  auto p = caf::actor_cast<caf::blocking_actor*>(proxy_);
  p->send(frontend_, atom::get::value, std::move(key), p, ++id_);
  return id_;
}

mailbox store::proxy::mailbox() {
  return detail::make_mailbox(caf::actor_cast<detail::flare_actor*>(proxy_));
}

store::response store::proxy::receive() {
  response resp;
  caf::actor_cast<caf::blocking_actor*>(proxy_)->receive(
    [&](data& x, caf::error& e, request_id id) {
      resp.id_ = id;
      if (e)
        resp.data_ = std::move(e);
      else
        resp.data_ = std::move(x);
    },
    [&](caf::error& e) {
      BROKER_ERROR("proxy failed to receive response from store");
      resp.data_ = std::move(e);
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

store::store(caf::actor actor) : frontend_{std::move(actor)} {
  // nop
}

} // namespace broker
