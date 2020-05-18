#pragma once

#include "broker/detail/core_recorder.hh"
#include "broker/filter_type.hh"

namespace broker::mixin {

template <class Base, class Subtype>
class recorder : public Base {
public:
  using super = Base;

  using extended_base = recorder;

  using message_type = typename super::message_type;

  using peer_id_type = typename super::peer_id_type;

  using communication_handle_type = typename super::communication_handle_type;

  template <class... Ts>
  explicit recorder(Ts&&... xs)
    : super(std::forward<Ts>(xs)...), rec_(super::self()) {
    // nop
  }

  void ship(message_type& msg) {
    if (rec_)
      rec_.try_record(msg);
    super::ship(msg);
  }

  void ship(data_message& msg, const peer_id_type& receiver) {
    // TODO: extend recording interface to cover direct messages
    super::ship(msg, receiver);
  }

  void subscribe(const filter_type& what) {
    if (rec_)
      rec_.record_subscription(what);
    super::subscribe(what);
  }

  void peer_connected(const peer_id_type& remote_id,
                      const communication_handle_type& hdl) {
    if (rec_)
      rec_.record_peer(remote_id);
    super::peer_connected(remote_id, hdl);
  }

private:
  detail::core_recorder rec_;
};

} // namespace broker::mixin
