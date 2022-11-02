#include "broker/store.hh"

#include <optional>
#include <string>
#include <utility>

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/make_message.hpp>
#include <caf/others.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>

#include "broker/expected.hh"
#include "broker/internal/flare_actor.hh"
#include "broker/internal/logger.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"
#include "broker/internal_command.hh"

/// Checks whether the store has been initialized and logs an error message
/// otherwise before "returning" void.
#define CHECK_INITIALIZED_VOID()                                               \
  do {                                                                         \
    if (!initialized()) {                                                      \
      BROKER_ERROR(__func__ << "called on an uninitialized store");            \
      return;                                                                  \
    }                                                                          \
  } while (false)

namespace atom = broker::internal::atom;

using broker::internal::facade;
using broker::internal::native;

namespace broker {

namespace {

class state_impl : public detail::store_state {
public:
  endpoint_id this_peer;
  std::string name;
  caf::actor frontend;
  caf::scoped_actor self;
  request_id req_id = 1;

  state_impl(endpoint_id this_peer, std::string name, caf::actor frontend_hdl)
    : this_peer(this_peer),
      name(std::move(name)),
      frontend(std::move(frontend_hdl)),
      self(frontend->home_system()) {
    BROKER_DEBUG("created state for store" << this->name);
  }

  ~state_impl() override {
    BROKER_DEBUG("destroyed state for store" << name);
  }

  template <class T, class... Ts>
  expected<T> request(Ts&&... xs) {
    expected<T> res{T{}};
    self->request(frontend, timeout::frontend, std::forward<Ts>(xs)...)
      .receive([&](T& x) { res = std::move(x); },
               [&](caf::error& e) { res = facade(e); });
    return res;
  }

  template <class T, class... Ts>
  expected<T> request_tagged(request_id tag, Ts&&... xs) {
    expected<T> res{T{}};
    self->request(frontend, timeout::frontend, std::forward<Ts>(xs)...)
      .receive(
        [&, tag](T& x, request_id res_tag) {
          if (res_tag == tag) {
            res = std::move(x);
          } else {
            BROKER_ERROR("frontend responded with unexpected tag");
            res = make_error(ec::invalid_tag,
                             "frontend responded with unexpected tag");
          }
        },
        [&](caf::error& e) { res = facade(e); });
    return res;
  }

  template <class... Ts>
  void anon_send(Ts&&... xs) {
    caf::anon_send(frontend, std::forward<Ts>(xs)...);
  }

  entity_id self_id() const {
    return entity_id{this_peer, self->id()};
  }

  entity_id frontend_id() const {
    return entity_id{this_peer, frontend.id()};
  }
};

auto& dref(detail::store_state& state) {
  return static_cast<state_impl&>(state);
}

auto& dref(detail::shared_store_state_ptr& state) {
  return dref(*state);
}

} // namespace

} // namespace broker

namespace broker {

// -- private template member functions ----------------------------------------

template <class F>
void store::with_state(F f) const {
  using namespace broker;
  if (auto ptr = state_.lock())
    f(dref(ptr));
}

template <class F>
void store::with_state_ptr(F f) const {
  using namespace broker;
  if (auto ptr = state_.lock())
    f(ptr);
}

template <class F, class G>
auto store::with_state_or(F f, G fallback) const -> decltype(fallback()) {
  using namespace broker;
  if (auto ptr = state_.lock())
    return f(dref(ptr));
  else
    return fallback();
}

template <class... Ts>
expected<data> store::fetch(Ts&&... xs) const {
  using namespace broker;
  return with_state_or(
    [&](state_impl& st) { //
      return st.request<data>(std::forward<Ts>(xs)...);
    },
    []() -> expected<data> {
      return make_error(ec::bad_member_function_call,
                        "store state not initialized");
    });
}

template <class T, class... Ts>
expected<T> store::request(Ts&&... xs) {
  with_state_or(
    [&, this](state_impl& st) {
      expected<T> res{ec::unspecified};
      st.self->request(st.frontend, timeout::frontend, std::forward<Ts>(xs)...)
        .receive([&](T& x) { res = std::move(x); },
                 [&](caf::error& e) { res = facade(e); });
      return res;
    },
    []() -> expected<data> {
      return make_error(ec::unspecified, "store not initialized");
    });
}

// -- constructors, destructors, and assignment operators ----------------------

store::store(const store& other) : state_(other.state_) {
  with_state_ptr([this](detail::shared_store_state_ptr& st) {
    auto frontend = dref(st).frontend;
    caf::anon_send(frontend, atom::increment_v, std::move(st));
  });
}

store::store(endpoint_id this_peer, worker frontend, std::string name) {
  BROKER_TRACE(BROKER_ARG(this_peer)
               << BROKER_ARG(frontend) << BROKER_ARG(name));
  if (!frontend) {
    BROKER_ERROR("store::store called with frontend == nullptr");
    return;
  }
  if (name.empty()) {
    BROKER_ERROR("store::store called with empty name");
    return;
  }
  auto hdl = native(frontend);
  detail::shared_store_state_ptr ptr =
    std::make_shared<state_impl>(this_peer, std::move(name), hdl);
  state_ = ptr;
  caf::anon_send(hdl, atom::increment_v, std::move(ptr));
}

store& store::operator=(store&& other) noexcept {
  with_state_ptr([this](detail::shared_store_state_ptr& st) {
    auto frontend = dref(st).frontend;
    caf::anon_send(frontend, atom::decrement_v, std::move(st));
  });
  state_ = std::move(other.state_);
  return *this;
}

store& store::operator=(const store& other) {
  if (this == &other)
    return *this;
  with_state_ptr([this](detail::shared_store_state_ptr& st) {
    auto frontend = dref(st).frontend;
    caf::anon_send(frontend, atom::decrement_v, std::move(st));
  });
  state_ = other.state_;
  with_state_ptr([this](detail::shared_store_state_ptr& st) {
    auto frontend = dref(st).frontend;
    caf::anon_send(frontend, atom::increment_v, std::move(st));
  });
  return *this;
}

store::~store() {
  with_state_ptr([this](detail::shared_store_state_ptr& st) {
    auto frontend = dref(st).frontend;
    caf::anon_send(frontend, atom::decrement_v, std::move(st));
  });
}

store::proxy::proxy(store& s) {
  s.with_state([this](state_impl& st) {
    frontend_ = facade(st.frontend);
    proxy_ = facade(st.self->spawn<internal::flare_actor>());
    this_peer_ = st.this_peer;
  });
}

request_id store::proxy::exists(data key) {
  if (frontend_) {
    send_as(native(proxy_), native(frontend_), atom::exists_v, std::move(key),
            ++id_);
    return id_;
  } else {
    return 0;
  }
}

request_id store::proxy::get(data key) {
  if (frontend_) {
    send_as(native(proxy_), native(frontend_), atom::get_v, std::move(key),
            ++id_);
    return id_;
  } else {
    return 0;
  }
}

request_id store::proxy::put_unique(data key, data val,
                                    std::optional<timespan> expiry) {
  BROKER_TRACE(BROKER_ARG(key) << BROKER_ARG(val) << BROKER_ARG(expiry)
                               << BROKER_ARG(this_peer_));
  if (frontend_) {
    auto req_id = ++id_;
    BROKER_DEBUG("proxy" << native(proxy_).id()
                         << "sends a put_unique with request ID" << req_id
                         << "to" << frontend_id());
    send_as(native(proxy_), native(frontend_), atom::local_v,
            internal_command_variant{
              put_unique_command{std::move(key), std::move(val), expiry,
                                 entity_id{this_peer_, native(proxy_).id()},
                                 req_id, frontend_id()}});
    return id_;
  } else {
    return 0;
  }
}

request_id store::proxy::get_index_from_value(data key, data index) {
  if (!frontend_)
    return 0;
  send_as(native(proxy_), native(frontend_), atom::get_v, std::move(key),
          std::move(index), ++id_);
  return id_;
}

request_id store::proxy::keys() {
  if (!frontend_)
    return 0;
  send_as(native(proxy_), native(frontend_), atom::get_v, atom::keys_v, ++id_);
  return id_;
}

worker store::frontend() const {
  return with_state_or([](state_impl& st) { return facade(st.frontend); },
                       []() { return worker{}; });
}

entity_id store::frontend_id() const {
  return with_state_or([](state_impl& st) { return st.frontend_id(); },
                       []() { return entity_id::nil(); });
}

mailbox store::proxy::mailbox() {
  return make_mailbox(caf::actor_cast<internal::flare_actor*>(native(proxy_)));
}

store::response store::proxy::receive() {
  BROKER_TRACE("");
  auto resp = response{error{}, 0};
  auto fa = caf::actor_cast<internal::flare_actor*>(native(proxy_));
  fa->receive(
    [&resp, fa](data& x, request_id id) {
      resp = {std::move(x), id};
      fa->extinguish_one();
    },
    [&resp, fa](caf::error& err, request_id id) {
      resp = {facade(err), id};
      fa->extinguish_one();
    },
    caf::others >> [&](caf::message& x) -> caf::skippable_result {
      BROKER_ERROR("proxy" << native(proxy_).id()
                           << "received an unexpected message:" << x);
      // We *must* make sure to consume any and all messages, because the flare
      // actor messes with the mailbox signaling. The flare fires on each
      // enqueued message and the flare actor reports data available as long as
      // the flare count is > 0. However, the blocking actor is unaware of this
      // flare and hence does not extinguish automatically when dequeueing a
      // message from the mailbox. Without this default handler to actively
      // discard unexpected messages, CAF would spin on the mailbox forever when
      // attempting to skip unhandled inputs because flare_actor::await_data
      // would always return `true`.
      fa->extinguish_one();
      auto err = caf::make_error(caf::sec::unexpected_message);
      resp.answer = facade(err);
      return err;
    });
  BROKER_DEBUG("proxy" << native(proxy_).id() << "received a response for ID"
                       << resp.id << "from" << frontend_id() << "->"
                       << resp.answer);
  return resp;
}

entity_id store::proxy::frontend_id() const noexcept {
  auto& hdl = native(frontend_);
  return {this_peer_, hdl.id()};
}

std::vector<store::response> store::proxy::receive(size_t n) {
  BROKER_TRACE(BROKER_ARG(n));
  std::vector<store::response> rval;
  rval.reserve(n);
  for (size_t i = 0; i < n; ++i)
    rval.emplace_back(receive());
  return rval;
}

std::string store::name() const {
  if (auto ptr = state_.lock())
    return dref(ptr).name;
  else
    return {};
}

expected<data> store::exists(data key) const {
  BROKER_TRACE(BROKER_ARG(key));
  return fetch(atom::exists_v, std::move(key));
}

expected<data> store::get(data key) const {
  BROKER_TRACE(BROKER_ARG(key));
  return fetch(atom::get_v, std::move(key));
}

expected<data> store::put_unique(data key, data val,
                                 std::optional<timespan> expiry) {
  BROKER_TRACE(BROKER_ARG(key) << BROKER_ARG(val) << BROKER_ARG(expiry));
  return with_state_or(
    [&, this](state_impl& st) {
      auto tag = st.req_id++;
      return st.request_tagged<data>(
        tag, atom::local_v,
        internal_command_variant{
          put_unique_command{std::move(key), std::move(val), expiry,
                             entity_id{st.this_peer, st.self->id()}, tag,
                             entity_id{st.this_peer, st.frontend.id()}}});
    },
    []() -> expected<data> {
      return make_error(ec::unspecified, "store not initialized");
    });
}

expected<data> store::get_index_from_value(data key, data index) const {
  return fetch(atom::get_v, std::move(key), std::move(index));
}

expected<data> store::keys() const {
  return fetch(atom::get_v, atom::keys_v);
}

bool store::initialized() const noexcept {
  return !state_.expired();
}

void store::put(data key, data value, std::optional<timespan> expiry) {
  with_state([&](state_impl& st) {
    st.anon_send(atom::local_v, internal_command_variant{
                                  put_command{std::move(key), std::move(value),
                                              expiry, st.frontend_id()}});
  });
}

void store::erase(data key) {
  with_state([&](state_impl& st) {
    st.anon_send(atom::local_v,
                 internal_command_variant{
                   erase_command{std::move(key), st.frontend_id()}});
  });
}

void store::add(data key, data value, data::type init_type,
                std::optional<timespan> expiry) {
  with_state([&](state_impl& st) {
    st.anon_send(atom::local_v,
                 internal_command_variant{
                   add_command{std::move(key), std::move(value), init_type,
                               expiry, st.frontend_id()}});
  });
}

void store::subtract(data key, data value, std::optional<timespan> expiry) {
  with_state([&](state_impl& st) {
    st.anon_send(atom::local_v,
                 internal_command_variant{
                   subtract_command{std::move(key), std::move(value), expiry,
                                    st.frontend_id()}});
  });
}

void store::clear() {
  with_state([&](state_impl& st) {
    st.anon_send(atom::local_v,
                 internal_command_variant{clear_command{st.frontend_id()}});
  });
}

bool store::await_idle(timespan timeout) {
  BROKER_TRACE(BROKER_ARG(timeout));
  bool result = false;
  with_state([&](state_impl& st) {
    st.self->request(st.frontend, timeout, atom::await_v, atom::idle_v)
      .receive([&result](atom::ok) { result = true; },
               []([[maybe_unused]] const caf::error& err) {
                 BROKER_ERROR("await_idle failed: " << err);
               });
  });
  return result;
}

void store::await_idle(std::function<void(bool)> callback, timespan timeout) {
  BROKER_TRACE(BROKER_ARG(timeout));
  if (!callback) {
    BROKER_ERROR("invalid callback received for await_idle");
    return;
  }
  with_state_or(
    [&](state_impl& st) {
      auto await_actor = [cb{std::move(callback)}](caf::event_based_actor* self,
                                                   const caf::actor& frontend,
                                                   timespan t) {
        self->request(frontend, t, atom::await_v, atom::idle_v)
          .then([cb](atom::ok) { cb(true); },
                [cb](const caf::error&) { cb(false); });
      };
      st.self->spawn(std::move(await_actor), st.frontend, timeout);
    },
    [&]() { callback(false); });
}

void store::reset() {
  state_.reset();
}

std::string to_string(const store::response& x) {
  return caf::deep_to_string(std::tie(x.answer, x.id));
}

} // namespace broker
