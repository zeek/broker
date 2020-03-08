#include "broker/detail/store_actor.hh"

namespace broker::detail {

void store_actor_state::init(caf::event_based_actor* self,
                             endpoint::clock* clock, std::string&& id,
                             caf::actor&& core) {
  BROKER_ASSERT(self != nullptr);
  BROKER_ASSERT(clock != nullptr);
  this->self = self;
  this->clock = clock;
  this->id = std::move(id);
  this->core = std::move(core);
}

void store_actor_state::emit_add_event(const data& key, const data& value,
                                       const caf::optional<timespan>& expiry) {
  vector xs;
  xs.reserve(4);
  xs.emplace_back("insert");
  xs.emplace_back(key);
  xs.emplace_back(value);
  if (expiry)
    xs.emplace_back(*expiry);
  else
    xs.emplace_back(nil);
  self->send(core, atom::publish::value, atom::local::value,
             make_data_message(topics::store_events, data{std::move(xs)}));
}

void store_actor_state::emit_put_event(const data& key, const data& value,
                                       const caf::optional<timespan>& expiry) {
  vector xs;
  xs.reserve(4);
  xs.emplace_back("update");
  xs.emplace_back(key);
  xs.emplace_back(value);
  if (expiry)
    xs.emplace_back(*expiry);
  else
    xs.emplace_back(nil);
  self->send(core, atom::publish::value, atom::local::value,
             make_data_message(topics::store_events, data{std::move(xs)}));
}

void store_actor_state::emit_erase_event(const data& key) {
  vector xs;
  xs.reserve(2);
  xs.emplace_back("erase");
  xs.emplace_back(key);
  self->send(core, atom::publish::value, atom::local::value,
             make_data_message(topics::store_events, data{std::move(xs)}));
}

} // namespace broker::detail
