#include "broker/detail/store_actor.hh"

using namespace std::string_literals;

namespace broker::detail {

namespace {

template <class T>
constexpr size_t vec_slots() {
  if constexpr (std::is_same<T, publisher_id>::value)
    return 2;
  else
    return 1;
}

template <class T>
void append(vector& xs, const T& x) {
  xs.emplace_back(x);
}

template <class T>
void append(vector& xs, const optional<T>& x) {
  if (x)
    xs.emplace_back(*x);
  else
    xs.emplace_back(nil);
}

void append(vector& xs, const publisher_id& x) {
  if (x) {
    if (auto ep = to<data>(x.endpoint)) {
      xs.emplace_back(std::move(*ep));
      xs.emplace_back(x.object);
      return;
    }
  }
  xs.emplace_back(nil);
  xs.emplace_back(nil);
}

template <class... Ts>
void fill_vector(vector& vec, const Ts&... xs) {
  vec.reserve((vec_slots<Ts>() + ...));
  (append(vec, xs), ...);
}

} // namespace

void store_actor_state::init(caf::event_based_actor* self,
                             endpoint::clock* clock, std::string&& id,
                             caf::actor&& core) {
  BROKER_ASSERT(self != nullptr);
  BROKER_ASSERT(clock != nullptr);
  this->self = self;
  this->clock = clock;
  this->id = std::move(id);
  this->core = std::move(core);
  this->dst = topics::store_events / this->id;
}

void store_actor_state::emit_insert_event(const data& key, const data& value,
                                          const optional<timespan>& expiry,
                                          const publisher_id& publisher) {
  vector xs;
  fill_vector(xs, "insert"s, id, key, value, expiry, publisher);
  self->send(core, atom::publish_v, atom::local_v,
             make_data_message(dst, data{std::move(xs)}));
}

void store_actor_state::emit_update_event(const data& key,
                                          const data& old_value,
                                          const data& new_value,
                                          const optional<timespan>& expiry,
                                          const publisher_id& publisher) {
  vector xs;
  fill_vector(xs, "update"s, id, key, old_value, new_value, expiry, publisher);
  self->send(core, atom::publish_v, atom::local_v,
             make_data_message(dst, data{std::move(xs)}));
}

void store_actor_state::emit_erase_event(const data& key,
                                         const publisher_id& publisher) {
  vector xs;
  fill_vector(xs, "erase"s, id, key, publisher);
  self->send(core, atom::publish_v, atom::local_v,
             make_data_message(dst, data{std::move(xs)}));
}

void store_actor_state::emit_expire_event(const data& key,
                                          const publisher_id& publisher) {
  vector xs;
  fill_vector(xs, "expire"s, id, key, publisher);
  self->send(core, atom::publish_v, atom::local_v,
             make_data_message(dst, data{std::move(xs)}));
}

} // namespace broker::detail
