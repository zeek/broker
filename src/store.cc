#include "broker/store.hh"

#include <string>
#include <utility>

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/make_message.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include "broker/detail/flare_actor.hh"
#include "broker/detail/store_state.hh"
#include "broker/expected.hh"
#include "broker/internal_command.hh"
#include "broker/logger.hh"

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

template <class... Ts>
broker::expected<broker::data>
fetch(const broker::detail::weak_store_state_ptr& state, Ts&&... xs) {
  using namespace broker;
  if (auto ptr = state.lock())
    return ptr->request<data>(std::forward<Ts>(xs)...);
  return make_error(ec::bad_member_function_call,
                    "store state not initialized");
}

template <class F>
broker::expected<broker::data>
with_state(const broker::detail::weak_store_state_ptr& state, F f) {
  using namespace broker;
  if (auto ptr = state.lock())
    return f(*ptr);
  return make_error(ec::bad_member_function_call,
                    "store state not initialized");
}

} // namespace

using namespace broker::detail;

namespace broker {

// -- constructors, destructors, and assignment operators ----------------------

store::store() {
  // Required out-of-line for weak_store_state_ptr.
}

store::store(store&& other) : state_(std::move(other.state_)) {
  // Required out-of-line for weak_store_state_ptr.
}

store::store(const store& other) : state_(other.state_) {
  if (auto ptr = state_.lock())
    caf::anon_send(ptr->frontend, atom::increment_v, ptr);
}

store::store(caf::actor frontend, std::string name) {
  BROKER_TRACE(BROKER_ARG(frontend) << BROKER_ARG(name));
  if (!frontend) {
    BROKER_ERROR("store::store called with frontend == nullptr");
    return;
  }
  if (name.empty()) {
    BROKER_ERROR("store::store called with empty name");
    return;
  }
  auto ptr = std::make_shared<detail::store_state>(std::move(name), frontend);
  state_ = ptr;
  caf::anon_send(frontend, atom::increment_v, std::move(ptr));
}

store& store::operator=(store&& other) {
  if (auto ptr = state_.lock())
    caf::anon_send(ptr->frontend, atom::decrement_v, ptr);
  state_ = std::move(other.state_);
  return *this;
}

store& store::operator=(const store& other) {
  if (auto ptr = state_.lock())
    caf::anon_send(ptr->frontend, atom::decrement_v, ptr);
  if (auto new_ptr = other.state_.lock()) {
    state_ = new_ptr;
    caf::anon_send(new_ptr->frontend, atom::decrement_v, new_ptr);
  } else {
    state_.reset();
  }
  return *this;
}

store::~store() {
  if (auto ptr = state_.lock())
    caf::anon_send(ptr->frontend, atom::decrement_v, ptr);
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

caf::actor store::frontend() const {
  if (auto ptr = state_.lock())
    return ptr->frontend;
  return {};
}

entity_id store::frontend_id() const {
  if (auto ptr = state_.lock())
    return entity_id::from(ptr->frontend);
  return entity_id::nil();
}

caf::actor store::self_hdl() const {
  if (auto ptr = state_.lock())
    return caf::actor{ptr->self.ptr()};
  return caf::actor{};
}

entity_id store::self_id() const {
  if (auto ptr = state_.lock())
    return entity_id::from(ptr->self);
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

std::string store::name() const {
  if (auto ptr = state_.lock())
    return ptr->name;
  return {};
}

expected<data> store::exists(data key) const {
  return fetch(state_, atom::exists_v, std::move(key));
}

expected<data> store::get(data key) const {
  return fetch(state_, atom::get_v, std::move(key));
}

expected<data> store::put_unique(data key, data val,
                                 optional<timespan> expiry) {
  return with_state(state_, [&](detail::store_state& state) {
    return state.request<data>(atom::local_v,
                               make_internal_command<put_unique_command>(
                                 std::move(key), std::move(val), expiry,
                                 entity_id::from(state.self), state.req_id++,
                                 frontend_id()));
  });
}

expected<data> store::get_index_from_value(data key, data index) const {
  return fetch(state_, atom::get_v, std::move(key), std::move(index));
}

expected<data> store::keys() const {
  return fetch(state_, atom::get_v, atom::keys_v);
}

bool store::initialized() const noexcept {
  return !state_.expired();
}

void store::put(data key, data value, optional<timespan> expiry) {
  if (auto ptr = state_.lock())
    ptr->anon_send(atom::local_v,
                   make_internal_command<put_command>(
                     std::move(key), std::move(value), expiry, frontend_id()));
}

void store::erase(data key) {
  if (auto ptr = state_.lock())
    ptr->anon_send(atom::local_v, make_internal_command<erase_command>(
                                    std::move(key), frontend_id()));
}

void store::add(data key, data value, data::type init_type,
                optional<timespan> expiry) {
  if (auto ptr = state_.lock())
    ptr->anon_send(atom::local_v, make_internal_command<add_command>(
                                    std::move(key), std::move(value), init_type,
                                    expiry, frontend_id()));
}

void store::subtract(data key, data value, optional<timespan> expiry) {
  if (auto ptr = state_.lock())
    ptr->anon_send(atom::local_v,
                   make_internal_command<subtract_command>(
                     std::move(key), std::move(value), expiry, frontend_id()));
}

void store::clear() {
  if (auto ptr = state_.lock())
    ptr->anon_send(atom::local_v,
                   make_internal_command<clear_command>(frontend_id()));
}

bool store::await_idle(timespan timeout) {
  BROKER_TRACE(BROKER_ARG(timeout));
  bool result = false;
  if (auto ptr = state_.lock())
    ptr->self->request(ptr->frontend, timeout, atom::await_v, atom::idle_v)
      .receive([&result](atom::ok) { result = true; },
               []([[maybe_unused]] const error& err) {
                 BROKER_ERROR("await_idle failed: " << err);
               });
  return result;
}

void store::await_idle(std::function<void(bool)> callback, timespan timeout) {
  BROKER_TRACE(BROKER_ARG(timeout));
  if (!callback) {
    BROKER_ERROR("invalid callback received for await_idle");
    return;
  }
  if (auto ptr = state_.lock()) {
    auto await_actor = [cb{std::move(callback)}](caf::event_based_actor* self,
                                                 caf::actor frontend,
                                                 timespan t) {
      self->request(frontend, t, atom::await_v, atom::idle_v)
        .then([cb](atom::ok) { cb(true); }, [cb](const error&) { cb(false); });
    };
    ptr->self->spawn(std::move(await_actor), ptr->frontend, timeout);
  } else {
    callback(false);
  }
}

void store::reset() {
  state_.reset();
}

} // namespace broker
