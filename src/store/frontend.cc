#include <caf/all.hpp>

#include "broker/atoms.hh"
#include "broker/store/frontend.hh"

namespace broker {

extern std::unique_ptr<caf::actor_system> broker_system;

static caf::actor& handle_to_actor(void* h) {
  return *static_cast<caf::actor*>(h);
}

namespace store {
namespace detail {

requester::requester(caf::actor_config& cfg, caf::actor backend,
                     identifier master_name, query q, caf::actor queue,
                     std::chrono::duration<double> timeout, void* cookie)
    : caf::event_based_actor{cfg},
      request_(std::move(q)) {
    using namespace std;
    using namespace caf;
    bootstrap_ = {
      after(chrono::seconds::zero()) >> [=] {
        send(backend, std::move(master_name), request_, this);
        become(awaiting_response_);
      }
    };
    awaiting_response_ = {
      [=](const actor&, result& r) {
        send(queue, store::response{std::move(request_), std::move(r), cookie});
        quit();
      },
      after(chrono::duration_cast<chrono::microseconds>(timeout)) >> [=] {
        send(queue, store::response{std::move(request_),
                                    result(result::status::timeout), cookie});
        quit();
      }
    };
  }

caf::behavior requester::make_behavior() {
  return bootstrap_;
}

} // namespace detail

frontend::frontend(const endpoint& e, identifier master_name)
    : master_name_{std::move(master_name)},
      endpoint_{handle_to_actor(e.handle())},
      self_{*broker_system} {
}

frontend::~frontend() = default;

const identifier& frontend::id() const {
  return master_name_;
}

const response_queue& frontend::responses() const {
  return responses_;
}

void frontend::insert(data k, data v) const {
  caf::anon_send(handle_to_actor(handle()), master_name_,
                 caf::make_message(insert_atom::value, std::move(k),
                                   std::move(v)));
}

void frontend::insert(data k, data v, expiration_time t) const {
  caf::anon_send(handle_to_actor(handle()), master_name_,
                 caf::make_message(insert_atom::value, std::move(k),
                                   std::move(v), std::move(t)));
}

void frontend::erase(data k) const {
  caf::anon_send(handle_to_actor(handle()), master_name_,
                 caf::make_message(erase_atom::value, std::move(k)));
}

void frontend::clear() const {
  caf::anon_send(handle_to_actor(handle()), master_name_,
                 caf::make_message(clear_atom::value));
}

void frontend::increment(data k, int64_t by) const {
  caf::anon_send(handle_to_actor(handle()), master_name_,
                 caf::make_message(increment_atom::value, std::move(k), by));
}

void frontend::decrement(data k, int64_t by) const {
  increment(std::move(k), -by);
}

void frontend::add_to_set(data k, data element) const {
  caf::anon_send(handle_to_actor(handle()), master_name_,
                 caf::make_message(set_add_atom::value, std::move(k),
                                   std::move(element)));
}

void frontend::remove_from_set(data k, data element) const {
  caf::anon_send(handle_to_actor(handle()), master_name_,
                 caf::make_message(set_rem_atom::value, std::move(k),
                                   std::move(element)));
}

void frontend::push_left(data k, vector item) const {
  caf::anon_send(handle_to_actor(handle()), master_name_,
                 caf::make_message(lpush_atom::value, std::move(k),
                                   std::move(item)));
}

void frontend::push_right(data k, vector item) const {
  caf::anon_send(handle_to_actor(handle()), master_name_,
                 caf::make_message(rpush_atom::value, std::move(k),
                                   std::move(item)));
}

result frontend::request(query q) const {
  result rval;
  caf::actor store_actor = caf::invalid_actor;
  bool need_master =
    q.type == query::tag::pop_left || q.type == query::tag::pop_right;
  auto& where = need_master ? endpoint_ : handle_to_actor(handle());
  self_->request(where, caf::infinite, store_actor_atom::value,
                 master_name_).receive(
    [&store_actor](caf::actor& sa) { store_actor = std::move(sa); }
  );
  if (!store_actor)
    return rval;
  self_->request(store_actor, caf::infinite, master_name_, std::move(q),
                 self_).receive(
    [&rval](const caf::actor&, result& r) { rval = std::move(r); }
  );
  return rval;
}

void frontend::request(query q, std::chrono::duration<double> timeout,
                       void* cookie) const {
  bool need_master = q.type == query::tag::pop_left
                       || q.type == query::tag::pop_right;
  auto& where = need_master ? endpoint_ : handle_to_actor(handle());
  broker_system->spawn<detail::requester>(where, master_name_, std::move(q),
                                  handle_to_actor(responses_.handle()),
                                  timeout, cookie);
}

void* frontend::handle() const {
  return const_cast<caf::actor*>(&endpoint_);
}

} // namespace store
} // namespace broker

// Begin C API
#include "broker/broker.h"
using std::nothrow;

void broker_deque_of_store_response_delete(broker_deque_of_store_response* d) {
  delete reinterpret_cast<std::deque<broker::store::response>*>(d);
}

size_t
broker_deque_of_store_response_size(const broker_deque_of_store_response* d) {
  auto dd = reinterpret_cast<const std::deque<broker::store::response>*>(d);
  return dd->size();
}

broker_store_response*
broker_deque_of_store_response_at(broker_deque_of_store_response* d,
                                  size_t idx) {
  auto dd = reinterpret_cast<std::deque<broker::store::response>*>(d);
  return reinterpret_cast<broker_store_response*>(&(*dd)[idx]);
}

void broker_deque_of_store_response_erase(broker_deque_of_store_response* d,
                                          size_t idx) {
  auto dd = reinterpret_cast<std::deque<broker::store::response>*>(d);
  dd->erase(dd->begin() + idx);
}

int broker_store_response_queue_fd(const broker_store_response_queue* q) {
  auto qq = reinterpret_cast<const broker::store::response_queue*>(q);
  return qq->fd();
}

broker_deque_of_store_response*
broker_store_response_queue_want_pop(const broker_store_response_queue* q) {
  auto rval = new (nothrow) std::deque<broker::store::response>;
  if (!rval)
    return nullptr;
  auto qq = reinterpret_cast<const broker::store::response_queue*>(q);
  *rval = qq->want_pop();
  return reinterpret_cast<broker_deque_of_store_response*>(rval);
}

broker_deque_of_store_response*
broker_store_response_queue_need_pop(const broker_store_response_queue* q) {
  auto rval = new (nothrow) std::deque<broker::store::response>;
  if (!rval)
    return nullptr;
  auto qq = reinterpret_cast<const broker::store::response_queue*>(q);
  *rval = qq->need_pop();
  return reinterpret_cast<broker_deque_of_store_response*>(rval);
}

broker_store_frontend*
broker_store_frontend_create(const broker_endpoint* e,
                             const broker_string* master_name) {
  auto ee = reinterpret_cast<const broker::endpoint*>(e);
  auto nn = reinterpret_cast<const std::string*>(master_name);
  try {
    auto rval = new broker::store::frontend(*ee, *nn);
    return reinterpret_cast<broker_store_frontend*>(rval);
  } catch (std::bad_alloc&) {
    return nullptr;
  }
}

void broker_store_frontend_delete(broker_store_frontend* f) {
  delete reinterpret_cast<broker::store::frontend*>(f);
}

const broker_string* broker_store_frontend_id(const broker_store_frontend* f) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  return reinterpret_cast<const broker_string*>(&ff->id());
}

const broker_store_response_queue*
broker_store_frontend_responses(const broker_store_frontend* f) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  return reinterpret_cast<const broker_store_response_queue*>(&ff->responses());
}

int broker_store_frontend_insert(const broker_store_frontend* f,
                                 const broker_data* k, const broker_data* v) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto kk = reinterpret_cast<const broker::data*>(k);
  auto vv = reinterpret_cast<const broker::data*>(v);
  try {
    ff->insert(*kk, *vv);
    return 1;
  } catch (std::bad_alloc&) {
    return 0;
  }
}

int broker_store_frontend_insert_with_expiry(
  const broker_store_frontend* f, const broker_data* k, const broker_data* v,
  const broker_store_expiration_time* e) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto kk = reinterpret_cast<const broker::data*>(k);
  auto vv = reinterpret_cast<const broker::data*>(v);
  auto ee = reinterpret_cast<const broker::store::expiration_time*>(e);
  try {
    ff->insert(*kk, *vv, *ee);
    return 1;
  } catch (std::bad_alloc&) {
    return 0;
  }
}

int broker_store_frontend_erase(const broker_store_frontend* f,
                                const broker_data* k) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto kk = reinterpret_cast<const broker::data*>(k);
  try {
    ff->erase(*kk);
    return 1;
  } catch (std::bad_alloc&) {
    return 0;
  }
}

void broker_store_frontend_clear(const broker_store_frontend* f) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  ff->clear();
}

int broker_store_frontend_increment(const broker_store_frontend* f,
                                    const broker_data* k, int64_t by) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto kk = reinterpret_cast<const broker::data*>(k);
  try {
    ff->increment(*kk, by);
    return 1;
  } catch (std::bad_alloc&) {
    return 0;
  }
}

int broker_store_frontend_add_to_set(const broker_store_frontend* f,
                                     const broker_data* k,
                                     const broker_data* element) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto kk = reinterpret_cast<const broker::data*>(k);
  auto ee = reinterpret_cast<const broker::data*>(element);
  try {
    ff->add_to_set(*kk, *ee);
    return 1;
  } catch (std::bad_alloc&) {
    return 0;
  }
}

int broker_store_frontend_remove_from_set(const broker_store_frontend* f,
                                          const broker_data* k,
                                          const broker_data* element) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto kk = reinterpret_cast<const broker::data*>(k);
  auto ee = reinterpret_cast<const broker::data*>(element);
  try {
    ff->remove_from_set(*kk, *ee);
    return 1;
  } catch (std::bad_alloc&) {
    return 0;
  }
}

int broker_store_frontend_push_left(const broker_store_frontend* f,
                                    const broker_data* k,
                                    const broker_vector* items) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto kk = reinterpret_cast<const broker::data*>(k);
  auto ii = reinterpret_cast<const broker::vector*>(items);
  try {
    ff->push_left(*kk, *ii);
    return 1;
  } catch (std::bad_alloc&) {
    return 0;
  }
}

int broker_store_frontend_push_right(const broker_store_frontend* f,
                                     const broker_data* k,
                                     const broker_vector* items) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto kk = reinterpret_cast<const broker::data*>(k);
  auto ii = reinterpret_cast<const broker::vector*>(items);
  try {
    ff->push_right(*kk, *ii);
    return 1;
  } catch (std::bad_alloc&) {
    return 0;
  }
}

broker_store_result*
broker_store_frontend_request_blocking(const broker_store_frontend* f,
                                       const broker_store_query* q) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto qq = reinterpret_cast<const broker::store::query*>(q);
  try {
    auto rval = new broker::store::result;
    *rval = ff->request(*qq);
    return reinterpret_cast<broker_store_result*>(rval);
  } catch (std::bad_alloc&) {
    return 0;
  }
}

int broker_store_frontend_request_nonblocking(const broker_store_frontend* f,
                                              const broker_store_query* q,
                                              double timeout, void* cookie) {
  auto ff = reinterpret_cast<const broker::store::frontend*>(f);
  auto qq = reinterpret_cast<const broker::store::query*>(q);
  try {
    ff->request(*qq, std::chrono::duration<double>(timeout), cookie);
    return 1;
  } catch (std::bad_alloc&) {
    return 0;
  }
}
