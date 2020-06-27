#include <utility>
#include <string>

#include "broker/logger.hh"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/error.hpp>
#include <caf/make_message.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include "broker/store.hh"
#include "broker/expected.hh"
#include "broker/internal_command.hh"
#include "broker/detail/flare_actor.hh"

/// Checks whether the store has been initialized and returns an error
/// otherwise.
#define CHECK_INITIALIZED()                                                    \
  do {                                                                         \
    if (!initialized())                                                        \
      return make_error(ec::bad_member_function_call,                          \
                        "store not initialized");                              \
  } while (false)

/// Checks whether the store has been initialized and logs an error message
/// otherwise before "returning" void.
#define CHECK_INITIALIZED_VOID()                                               \
  do {                                                                         \
    if (!initialized()) {                                                      \
      BROKER_ERROR(__func__ << "called on an uninitialized store");            \
      return;                                                                  \
    }                                                                          \
  } while (false)

namespace {

template <class T, class... Ts>
auto make_internal_command(Ts&&... xs) {
  using namespace broker;
  return internal_command{0, entity_id::nil(), T{std::forward<Ts>(xs)...}};
}

} // namespace

using namespace broker::detail;

namespace broker {

struct store::state {
  std::string name;
  caf::actor frontend;
  caf::scoped_actor self;

  state(std::string name, caf::actor frontend_hdl)
    : name(std::move(name)),
      frontend(std::move(frontend_hdl)),
      self(frontend->home_system()) {
    // nop
  }

  template <class T, class... Ts>
  expected<T> request(Ts&&... xs) {
    expected<T> res{caf::no_error};
    self->request(frontend, timeout::frontend, std::forward<Ts>(xs)...)
      .receive([&](T& x) { res = std::move(x); },
               [&](caf::error& e) { res = std::move(e); });
    return res;
  }

  template <class... Ts>
  void anon_send(Ts&&... xs) {
    caf::anon_send(frontend, std::forward<Ts>(xs)...);
  }
};

// -- constructors, destructors, and assignment operators ----------------------

store::store() {
  // Required out-of-line for std::shared_ptr<state>.
}

store::store(store&& other) : state_(std::move(other.state_)) {
  // Required out-of-line for std::shared_ptr<state>.
}

store::store(const store& other) : state_(other.state_) {
  // Required out-of-line for std::shared_ptr<state>.
}

store::store(caf::actor frontend, std::string name) {
  if (!frontend) {
    BROKER_ERROR("store::store called with frontend == nullptr");
    return;
  }
  if (name.empty()) {
    BROKER_ERROR("store::store called with empty name");
    return;
  }
  state_ = std::make_shared<state>(std::move(name), std::move(frontend));
}

store& store::operator=(store&& other) {
  // Required out-of-line for std::shared_ptr<state>.
  state_ = std::move(other.state_);
  return *this;
}

store& store::operator=(const store& other) {
  // Required out-of-line for std::shared_ptr<state>.
  state_ = other.state_;
  return *this;
}

store::~store() {
  // Required out-of-line for std::shared_ptr<state>.
}

store::proxy::proxy(store& st) : frontend_{st.frontend()} {
  proxy_ = frontend_.home_system().spawn<flare_actor>();
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

request_id store::proxy::put_unique(data key, data val, optional<timespan> expiry) {
  if (!frontend_)
    return 0;
  send_as(proxy_, frontend_, atom::local_v,
          make_internal_command<put_unique_command>(
            std::move(key), std::move(val), expiry, entity_id::from(proxy_),
            ++id_, frontend_id()));
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

const caf::actor& store::frontend() const {
  BROKER_ASSERT(initialized());
  return state_->frontend;
}

entity_id store::frontend_id() const {
  if (initialized())
    return entity_id::from(state_->frontend);
  return entity_id::nil();
}

caf::actor store::self_hdl() const {
  if (initialized())
    return caf::actor{state_->self.ptr()};
  return caf::actor{};
}

entity_id store::self_id() const {
  if (initialized())
    return entity_id::from(state_->self);
  return entity_id::nil();
}

mailbox store::proxy::mailbox() {
  return make_mailbox(caf::actor_cast<flare_actor*>(proxy_));
}

store::response store::proxy::receive() {
  auto resp = response{error{}, 0};
  auto fa = caf::actor_cast<broker::detail::flare_actor*>(proxy_);
  fa->receive(
    [&](data& x, request_id id) {
      resp = {std::move(x), id};
      fa->extinguish_one();
    },
    [&](caf::error& e, request_id id) {
      BROKER_ERROR("proxy failed to receive response from store" << id);
      resp = {std::move(e), id};
      fa->extinguish_one();
    }
  );
  return resp;
}

std::vector<store::response> store::proxy::receive(size_t n) {
  std::vector<store::response> rval;
  rval.reserve(n);
  size_t i = 0;
  auto fa = caf::actor_cast<broker::detail::flare_actor*>(proxy_);

  fa->receive_for(i, n) (
    [&](data& x, request_id id) {
      rval.emplace_back(store::response{std::move(x), id});
      fa->extinguish_one();
    },
    [&](caf::error& e, request_id id) {
      BROKER_ERROR("proxy failed to receive response from store" << id);
      rval.emplace_back(store::response{std::move(e), id});
      fa->extinguish_one();
    }
  );

  return rval;
}

const std::string& store::name() const {
  BROKER_ASSERT(initialized());
  return state_->name;
}

expected<data> store::exists(data key) const {
  CHECK_INITIALIZED();
  return state_->request<data>(atom::exists_v, std::move(key));
}

expected<data> store::get(data key) const {
  CHECK_INITIALIZED();
  return state_->request<data>(atom::get_v, std::move(key));
}

expected<data> store::put_unique(data key, data val, optional<timespan> expiry) const {
  CHECK_INITIALIZED();
  return state_->request<data>(atom::local_v,
                               make_internal_command<put_unique_command>(
                                 std::move(key), std::move(val), expiry,
                                 self_id(), request_id(-1), frontend_id()));
  /*
  expected<data> res{ec::unspecified};
  caf::scoped_actor self{frontend_->home_system()};
  auto cmd = make_internal_command<put_unique_command>(
    std::move(key), std::move(val), expiry, self, request_id(-1),
    frontend_id());
  auto msg = caf::make_message(atom::local_v, std::move(cmd));

  self->send(frontend_, std::move(msg));
  self->delayed_send(self, timeout::frontend, atom::tick_v);
  self->receive(
    [&](data& x, request_id) {
      res = std::move(x);
    },
    [&](atom::tick) {
    },
    [&](caf::error& e) {
      res = std::move(e);
    }
  );
  return res;
  */
}

expected<data> store::get_index_from_value(data key, data index) const {
  CHECK_INITIALIZED();
  return state_->request<data>(atom::get_v, std::move(key), std::move(index));
}

expected<data> store::keys() const {
  CHECK_INITIALIZED();
  return state_->request<data>(atom::get_v, atom::keys_v);
}

void store::put(data key, data value, optional<timespan> expiry) {
  CHECK_INITIALIZED_VOID();
  state_->anon_send(atom::local_v,
                    make_internal_command<put_command>(
                      std::move(key), std::move(value), expiry, frontend_id()));
}

void store::erase(data key) {
  CHECK_INITIALIZED_VOID();
  state_->anon_send(atom::local_v, make_internal_command<erase_command>(
                                     std::move(key), frontend_id()));
}

void store::add(data key, data value, data::type init_type,
                optional<timespan> expiry) {
  CHECK_INITIALIZED_VOID();
  state_->anon_send(atom::local_v, make_internal_command<add_command>(
                                     std::move(key), std::move(value),
                                     init_type, expiry, frontend_id()));
}

void store::subtract(data key, data value, optional<timespan> expiry) {
  CHECK_INITIALIZED_VOID();
  state_->anon_send(atom::local_v,
                    make_internal_command<subtract_command>(
                      std::move(key), std::move(value), expiry, frontend_id()));
}

void store::clear() {
  CHECK_INITIALIZED_VOID();
  state_->anon_send(atom::local_v,
                    make_internal_command<clear_command>(frontend_id()));
}

} // namespace broker
