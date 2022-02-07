#include <utility>
#include <string>

#include "broker/internal/logger.hh"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/error.hpp>
#include <caf/make_message.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include "broker/expected.hh"
#include "broker/internal/flare_actor.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"
#include "broker/internal_command.hh"
#include "broker/store.hh"

namespace atom = broker::internal::atom;

using broker::internal::facade;
using broker::internal::native;

namespace broker {

namespace {

template <class... Ts>
void send_as(worker& src, worker& dst, Ts&&... xs) {
  caf::send_as(native(src), native(dst), std::forward<Ts>(xs)...);
}

} // namespace

store::proxy::proxy(store& s) : frontend_{s.frontend_} {
  auto hdl = native(frontend_).home_system().spawn<internal::flare_actor>();
  proxy_ = facade(hdl);
}

request_id store::proxy::exists(data key) {
  if (!frontend_)
    return 0;
  send_as(proxy_, frontend_, atom::exists_v, std::move(key), ++id_);
  return id_;
}

request_id store::proxy::get(data key) {
  if (!frontend_)
    return 0;
  send_as(proxy_, frontend_, atom::get_v, std::move(key), ++id_);
  return id_;
}

request_id store::proxy::put_unique(data key, data val,
                                    std::optional<timespan> expiry) {
  if (!frontend_)
    return 0;
  send_as(
    proxy_, frontend_, atom::local_v,
    make_internal_command<put_unique_command>(
      std::move(key), std::move(val), expiry, proxy_, ++id_, frontend_id()));
  return id_;
}

request_id store::proxy::get_index_from_value(data key, data index) {
  if (!frontend_)
    return 0;
  send_as(proxy_, frontend_, atom::get_v, std::move(key), std::move(index), ++id_);
  return id_;
}

request_id store::proxy::keys() {
  if (!frontend_)
    return 0;
  send_as(proxy_, frontend_, atom::get_v, atom::keys_v, ++id_);
  return id_;
}

mailbox store::proxy::mailbox() {
  return make_mailbox(caf::actor_cast<internal::flare_actor*>(native(proxy_)));
}

store::response store::proxy::receive() {
  auto resp = response{error{}, 0};
  auto fa = caf::actor_cast<internal::flare_actor*>(native(proxy_));
  fa->receive(
    [&](data& x, request_id id) {
      resp = {std::move(x), id};
      fa->extinguish_one();
    },
    [&](const caf::error& e, request_id id) {
      BROKER_ERROR("proxy failed to receive response from store"
                   << id << "->" << to_string(e));
      resp = {facade(e), id};
      fa->extinguish_one();
    });
  return resp;
}

publisher_id store::proxy::frontend_id() const noexcept {
  auto& hdl = native(frontend_);
  return {facade(hdl.node()), hdl.id()};
}

std::vector<store::response> store::proxy::receive(size_t n) {
  std::vector<store::response> rval;
  rval.reserve(n);
  size_t i = 0;
  auto fa = caf::actor_cast<internal::flare_actor*>(native(proxy_));

  fa->receive_for(i, n) (
    [&](data& x, request_id id) {
      rval.emplace_back(store::response{std::move(x), id});
      fa->extinguish_one();
    },
    [&](const caf::error& e, request_id id) {
      BROKER_ERROR("proxy failed to receive response from store" << id);
      rval.emplace_back(store::response{facade(e), id});
      fa->extinguish_one();
    }
  );

  return rval;
}

const std::string& store::name() const {
  return name_;
}

template <class T, class... Ts>
expected<T> store::request(Ts&&... xs) const {
  if (!frontend_)
    return make_error(ec::unspecified, "store not initialized");
  expected<T> res{ec::unspecified};
  auto& hdl = native(frontend_);
  caf::scoped_actor self{hdl->home_system()};
  auto msg = caf::make_message(std::forward<Ts>(xs)...);
  self->request(hdl, timeout::frontend, std::move(msg))
    .receive([&](T& x) { res = std::move(x); },
             [&](caf::error& e) { res = facade(e); });
  return res;
}

expected<data> store::exists(data key) const {
  return request<data>(atom::exists_v, std::move(key));
}

expected<data> store::get(data key) const {
  return request<data>(atom::get_v, std::move(key));
}

expected<data> store::put_unique(data key, data val,
                                 std::optional<timespan> expiry) const {
  if (!frontend_)
    return make_error(ec::unspecified, "store not initialized");

  expected<data> res{ec::unspecified};
  auto& hdl = native(frontend_);
  caf::scoped_actor self{hdl->home_system()};
  auto cmd = make_internal_command<put_unique_command>(
    std::move(key), std::move(val), expiry, facade(caf::actor{self}),
    request_id(-1), frontend_id());
  auto msg = caf::make_message(atom::local_v, std::move(cmd));

  self->send(hdl, std::move(msg));
  self->delayed_send(self, timeout::frontend, atom::tick_v);
  self->receive(
    [&](data& x, request_id) {
      res = std::move(x);
    },
    [&](atom::tick) {
    },
    [&](caf::error& e) {
      res = facade(e);
    }
  );

  return res;
}

expected<data> store::get_index_from_value(data key, data index) const {
  return request<data>(atom::get_v, std::move(key), std::move(index));
}

expected<data> store::keys() const {
  return request<data>(atom::get_v, atom::keys_v);
}

publisher_id store::frontend_id() const noexcept {
  auto& hdl = native(frontend_);
  return {facade(hdl.node()), hdl.id()};
}

void store::put(data key, data value, std::optional<timespan> expiry) const {
  anon_send(native(frontend_), atom::local_v,
            make_internal_command<put_command>(std::move(key), std::move(value),
                                               expiry, frontend_id()));
}

void store::erase(data key) const {
  anon_send(
    native(frontend_), atom::local_v,
    make_internal_command<erase_command>(std::move(key), frontend_id()));
}

void store::add(data key, data value, data::type init_type,
                std::optional<timespan> expiry) const {
  anon_send(native(frontend_), atom::local_v,
            make_internal_command<add_command>(std::move(key), std::move(value),
                                               init_type, expiry,
                                               frontend_id()));
}

void store::subtract(data key, data value,
                     std::optional<timespan> expiry) const {
  anon_send(native(frontend_), atom::local_v,
            make_internal_command<subtract_command>(
              std::move(key), std::move(value), expiry, frontend_id()));
}

void store::clear() const {
  anon_send(native(frontend_), atom::local_v,
            make_internal_command<clear_command>(frontend_id()));
}

store::store(worker hdl, std::string name)
  : frontend_{std::move(hdl)}, name_{std::move(name)} {
  // nop
}

void store::reset() {
  // nop
}

} // namespace broker
