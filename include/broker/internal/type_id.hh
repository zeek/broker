#pragma once

#include "broker/cow_tuple.hh"
#include "broker/detail/store_state.hh"
#include "broker/error.hh"
#include "broker/fwd.hh"
#include "broker/internal/fwd.hh"
#include "broker/internal/native.hh"
#include "broker/time.hh"

#include <caf/config.hpp>
#include <caf/fwd.hpp>
#include <caf/is_error_code_enum.hpp>
#include <caf/type_id.hpp>

// -- imported atoms -----------------------------------------------------------

// NOLINTBEGIN
#define BROKER_CAF_ATOM_ALIAS(name)                                            \
  using name = caf::name##_atom;                                               \
  constexpr auto name##_v = caf::name##_atom_v;
// NOLINTEND

namespace broker::internal::atom {

BROKER_CAF_ATOM_ALIAS(add)
BROKER_CAF_ATOM_ALIAS(connect)
BROKER_CAF_ATOM_ALIAS(get)
BROKER_CAF_ATOM_ALIAS(join)
BROKER_CAF_ATOM_ALIAS(leave)
BROKER_CAF_ATOM_ALIAS(ok)
BROKER_CAF_ATOM_ALIAS(publish)
BROKER_CAF_ATOM_ALIAS(put)
BROKER_CAF_ATOM_ALIAS(subscribe)
BROKER_CAF_ATOM_ALIAS(tick)
BROKER_CAF_ATOM_ALIAS(unsubscribe)
BROKER_CAF_ATOM_ALIAS(update)

} // namespace broker::internal::atom

#undef BROKER_CAF_ATOM_ALIAS

// -- type announcements and custom atoms --------------------------------------

// Our type aliases for `timespan` and `timestamp` are identical to
// `caf::timespan` and `caf::timestamp`. Hence, these types should have a type
// ID assigned by CAF.

static_assert(caf::has_type_id_v<broker::timespan>,
              "broker::timespan != caf::timespan");

static_assert(caf::has_type_id_v<broker::timestamp>,
              "broker::timestamp != caf::timestamp");

#define BROKER_ADD_ATOM(name)                                                  \
  CAF_ADD_ATOM(broker_internal, broker::internal::atom, name)

#define BROKER_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(broker_internal, type)

CAF_BEGIN_TYPE_ID_BLOCK(broker_internal, caf::first_custom_type_id)

  // -- atoms for generic communication ----------------------------------------

  BROKER_ADD_ATOM(ack)
  BROKER_ADD_ATOM(default_)
  BROKER_ADD_ATOM(get_filter)
  BROKER_ADD_ATOM(id)
  BROKER_ADD_ATOM(init)
  BROKER_ADD_ATOM(listen)
  BROKER_ADD_ATOM(name)
  BROKER_ADD_ATOM(network)
  BROKER_ADD_ATOM(peer)
  BROKER_ADD_ATOM(read)
  BROKER_ADD_ATOM(retry)
  BROKER_ADD_ATOM(run)
  BROKER_ADD_ATOM(shutdown)
  BROKER_ADD_ATOM(status)
  BROKER_ADD_ATOM(unpeer)
  BROKER_ADD_ATOM(write)
  BROKER_ADD_ATOM(attach_client)

  // -- atoms for communication with workers -----------------------------------

  BROKER_ADD_ATOM(resume)

  // -- atoms for communication with stores ------------------------------------

  BROKER_ADD_ATOM(attach)
  BROKER_ADD_ATOM(await)
  BROKER_ADD_ATOM(clear)
  BROKER_ADD_ATOM(clone)
  BROKER_ADD_ATOM(data_store)
  BROKER_ADD_ATOM(decrement)
  BROKER_ADD_ATOM(erase)
  BROKER_ADD_ATOM(exists)
  BROKER_ADD_ATOM(expire)
  BROKER_ADD_ATOM(idle)
  BROKER_ADD_ATOM(increment)
  BROKER_ADD_ATOM(keys)
  BROKER_ADD_ATOM(local)
  BROKER_ADD_ATOM(master)
  BROKER_ADD_ATOM(mutable_check)
  BROKER_ADD_ATOM(resolve)
  BROKER_ADD_ATOM(restart)
  BROKER_ADD_ATOM(stale_check)
  BROKER_ADD_ATOM(subtract)
  BROKER_ADD_ATOM(sync_point)

  // -- atoms for communciation with the core actor ----------------------------

  BROKER_ADD_ATOM(no_events)
  BROKER_ADD_ATOM(snapshot)
  BROKER_ADD_ATOM(subscriptions)

  // -- Broker type announcements ----------------------------------------------

  BROKER_ADD_TYPE_ID((broker::ack_clone_command))
  BROKER_ADD_TYPE_ID((broker::add_command))
  BROKER_ADD_TYPE_ID((broker::address))
  BROKER_ADD_TYPE_ID((broker::alm::multipath))
  BROKER_ADD_TYPE_ID((broker::attach_writer_command))
  BROKER_ADD_TYPE_ID((broker::backend))
  BROKER_ADD_TYPE_ID((broker::backend_options))
  BROKER_ADD_TYPE_ID((broker::clear_command))
  BROKER_ADD_TYPE_ID((broker::command_message))
  BROKER_ADD_TYPE_ID((broker::cumulative_ack_command))
  BROKER_ADD_TYPE_ID((broker::data))
  BROKER_ADD_TYPE_ID((broker::data_message))
  BROKER_ADD_TYPE_ID((broker::detail::shared_store_state_ptr))
  BROKER_ADD_TYPE_ID((broker::ec))
  BROKER_ADD_TYPE_ID((broker::endpoint_id))
  BROKER_ADD_TYPE_ID((broker::endpoint_info))
  BROKER_ADD_TYPE_ID((broker::enum_value))
  BROKER_ADD_TYPE_ID((broker::erase_command))
  BROKER_ADD_TYPE_ID((broker::expire_command))
  BROKER_ADD_TYPE_ID((broker::filter_type))
  BROKER_ADD_TYPE_ID((broker::internal::command_consumer_res))
  BROKER_ADD_TYPE_ID((broker::internal::command_producer_res))
  BROKER_ADD_TYPE_ID((broker::internal::connector_event_id))
  BROKER_ADD_TYPE_ID((broker::internal::data_consumer_res))
  BROKER_ADD_TYPE_ID((broker::internal::data_producer_res))
  BROKER_ADD_TYPE_ID((broker::internal::node_consumer_res))
  BROKER_ADD_TYPE_ID((broker::internal::node_producer_res))
  BROKER_ADD_TYPE_ID((broker::internal::pending_connection_ptr))
  BROKER_ADD_TYPE_ID((broker::internal::retry_state))
  BROKER_ADD_TYPE_ID((broker::internal_command))
  BROKER_ADD_TYPE_ID((broker::internal_command_variant))
  BROKER_ADD_TYPE_ID((broker::keepalive_command))
  BROKER_ADD_TYPE_ID((broker::nack_command))
  BROKER_ADD_TYPE_ID((broker::network_info))
  BROKER_ADD_TYPE_ID((broker::node_message))
  BROKER_ADD_TYPE_ID((broker::none))
  BROKER_ADD_TYPE_ID((broker::peer_info))
  BROKER_ADD_TYPE_ID((broker::port))
  BROKER_ADD_TYPE_ID((broker::put_command))
  BROKER_ADD_TYPE_ID((broker::put_unique_command))
  BROKER_ADD_TYPE_ID((broker::put_unique_result_command))
  BROKER_ADD_TYPE_ID((broker::retransmit_failed_command))
  BROKER_ADD_TYPE_ID((broker::sc))
  BROKER_ADD_TYPE_ID((broker::set))
  BROKER_ADD_TYPE_ID((broker::shutdown_options))
  BROKER_ADD_TYPE_ID((broker::snapshot))
  BROKER_ADD_TYPE_ID((broker::status))
  BROKER_ADD_TYPE_ID((broker::subnet))
  BROKER_ADD_TYPE_ID((broker::subtract_command))
  BROKER_ADD_TYPE_ID((broker::table))
  BROKER_ADD_TYPE_ID((broker::topic))
  BROKER_ADD_TYPE_ID((broker::vector))

  // -- STD/CAF type announcements ---------------------------------------------

  BROKER_ADD_TYPE_ID((std::optional<broker::endpoint_id>) )
  BROKER_ADD_TYPE_ID((std::optional<broker::timespan>) )
  BROKER_ADD_TYPE_ID((std::optional<broker::timestamp>) )
  BROKER_ADD_TYPE_ID((std::shared_ptr<broker::filter_type>) )
  BROKER_ADD_TYPE_ID((std::shared_ptr<std::promise<void>>) )
  BROKER_ADD_TYPE_ID((std::vector<broker::peer_info>) )

CAF_END_TYPE_ID_BLOCK(broker_internal)

// -- enable opt-in features for Broker types ----------------------------------

CAF_ERROR_CODE_ENUM(broker::ec)

namespace caf {

template <class... Ts>
struct inspector_access<broker::cow_tuple<Ts...>> {
  using value_type = broker::cow_tuple<Ts...>;

  template <class Inspector>
  static bool apply(Inspector& f, value_type& x) {
    if constexpr (Inspector::is_loading)
      return f.tuple(x.unshared());
    else
      return f.tuple(x.data());
  }

  template <class Inspector>
  static bool save_field(Inspector& f, string_view field_name, value_type& x) {
    return detail::save_field(f, field_name, detail::as_mutable_ref(x.data()));
  }

  template <class Inspector, class IsPresent, class Get>
  static bool save_field(Inspector& f, string_view field_name,
                         IsPresent& is_present, Get& get) {
    if constexpr (std::is_lvalue_reference_v<decltype(get())>) {
      auto get_data = [&get]() -> decltype(auto) {
        return detail::as_mutable_ref(get().data());
      };
      return detail::save_field(f, field_name, is_present, get_data);
    } else {
      auto get_data = [&get] {
        auto tmp = get();
        return std::move(tmp.unshared());
      };
      return detail::save_field(f, field_name, is_present, get_data);
    }
  }

  template <class Inspector, class IsValid, class SyncValue>
  static bool load_field(Inspector& f, string_view field_name, value_type& x,
                         IsValid& is_valid, SyncValue& sync_value) {
    return detail::load_field(f, field_name, x.unshared(), is_valid,
                              sync_value);
  }

  template <class Inspector, class IsValid, class SyncValue, class SetFallback>
  static bool load_field(Inspector& f, string_view field_name, value_type& x,
                         IsValid& is_valid, SyncValue& sync_value,
                         SetFallback& set_fallback) {
    return detail::load_field(f, field_name, x.unshared(), is_valid, sync_value,
                              set_fallback);
  }
};

} // namespace caf

// -- cleanup ------------------------------------------------------------------

#undef BROKER_ADD_ATOM
#undef BROKER_ADD_TYPE_ID

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::detail::shared_store_state_ptr)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::internal::command_consumer_res)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::internal::command_producer_res)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::internal::data_consumer_res)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::internal::data_producer_res)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::internal::node_consumer_res)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::internal::node_producer_res)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(broker::internal::pending_connection_ptr)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<broker::filter_type>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<std::promise<void>>)
