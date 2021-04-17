#pragma once

#include "broker/fwd.hh"
#include "broker/message.hh"

#include <caf/type_id.hpp>

/// A `node_message` as it used to be pre-ALM.
struct legacy_node_message {

  /// Content of the message.
  broker::node_message_content content;

  /// Time-to-life counter.
  uint16_t ttl;
};

template <class Inspector>
bool inspect(Inspector& f, legacy_node_message& x) {
  return f.object(x).fields(f.field("content", x.content),
                            f.field("ttl", x.ttl));
}

CAF_BEGIN_TYPE_ID_BLOCK(micro_benchmarks, caf::id_block::broker::end)

  CAF_ADD_TYPE_ID(micro_benchmarks, (legacy_node_message))

CAF_END_TYPE_ID_BLOCK(micro_benchmarks)
