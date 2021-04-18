#pragma once

#include "broker/data.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"
#include "broker/time.hh"

#include <caf/type_id.hpp>

#include <random>

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

  CAF_ADD_TYPE_ID(micro_benchmarks, (caf::stream<legacy_node_message>))
  CAF_ADD_TYPE_ID(micro_benchmarks, (legacy_node_message))
  CAF_ADD_TYPE_ID(micro_benchmarks, (std::vector<legacy_node_message>))

CAF_END_TYPE_ID_BLOCK(micro_benchmarks)


class generator {
public:
  generator();

  broker::endpoint_id next_endpoint_id();

  static auto make_endpoint_id() {
    generator g;
    return g.next_endpoint_id();
  }

  broker::count next_count();

  std::string next_string(size_t length);

  broker::timestamp next_timestamp();

  // Generates events for one of three possible types:
  // 1. Trivial data consisting of a number and a string.
  // 2. More complex data that resembles a line in a conn.log.
  // 3. Large tables of size 100 by 10, filled with random strings.
  broker::data next_data(size_t event_type);

  static auto make_data(size_t event_type) {
    generator g;
    return g.next_data(event_type);
  }

private:
  std::minstd_rand rng_;
  broker::timestamp ts_;
};

void run_streaming_benchmark();
