#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <iterator>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <caf/actor_system_config.hpp>
#include <caf/behavior.hpp>
#include <caf/config_option_adder.hpp>
#include <caf/deep_to_string.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/init_global_meta_objects.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/term.hpp>
#include <caf/type_id.hpp>
#include <caf/uri.hpp>

#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/endpoint_id.hh"
#include "broker/internal/configuration_access.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/type_id.hh"
#include "broker/publisher.hh"
#include "broker/status.hh"
#include "broker/subscriber.hh"
#include "broker/topic.hh"

#include <fmt/chrono.h>
#include <fmt/color.h>
#include <fmt/core.h>

using std::string;

using broker::count;
using broker::data;
using broker::topic;
using broker::internal::native;

namespace atom = broker::internal::atom;

// -- additional message and atom types ----------------------------------------

#define BROKER_NODE_ADD_ATOM(name, text)                                       \
  CAF_ADD_ATOM(broker_node, broker::atom, name, text)

CAF_BEGIN_TYPE_ID_BLOCK(broker_node, id_block::broker_internal::end)

  BROKER_NODE_ADD_ATOM(blocking, "blocking")
  BROKER_NODE_ADD_ATOM(relay, "relay")
  BROKER_NODE_ADD_ATOM(stream, "stream")

CAF_END_TYPE_ID_BLOCK(broker_node)

// -- process-wide state -------------------------------------------------------

namespace {

string node_name;

} // namespace

// -- URI support for Broker ---------------------------------------------------

namespace broker {

bool convert(const caf::uri& from, network_info& to) {
  if (from.empty())
    return false;
  if (from.scheme() != "tcp")
    return false;
  const auto& auth = from.authority();
  if (auth.empty())
    return false;
  to.address = caf::visit(
    [](const auto& what) {
      using what_t = std::decay_t<decltype(what)>;
      if constexpr (std::is_same_v<what_t, std::string>)
        return what;
      else
        return to_string(what);
    },
    auth.host);
  to.port = auth.port;
  return true;
}

} // namespace broker

// -- CAF setup ----------------------------------------------------------------

using namespace caf;

namespace {

// -- global flags -------------------------------------------------------------

std::atomic<bool> verbose;

// -- constants ----------------------------------------------------------------

const auto error_style = fg(fmt::color::red);

const auto verbose_style = fg(fmt::color::blue);

constexpr size_t max_cap = std::numeric_limits<size_t>::max();

constexpr size_t default_payload_size = 0;

constexpr timespan default_rendezvous_retry = std::chrono::milliseconds(250);

constexpr size_t default_ping_count = 100;

// -- type aliases -------------------------------------------------------------

using uri_list = std::vector<uri>;

using topic_list = std::vector<topic>;

using string_list = std::vector<string>;

using mode_fun = void (*)(broker::endpoint&, topic_list);

// -- program options ----------------------------------------------------------

void extend_config(broker::configuration& broker_cfg) {
  auto& cfg = broker::internal::configuration_access(&broker_cfg).cfg();
  caf::config_option_adder{cfg.custom_options(), "global"}
    .add<bool>("verbose,v", "print status and debug output")
    .add<bool>("rate,r", "print rate once per second ('relay' mode only)")
    .add<string>("name,N", "set node name in verbose output")
    .add<string_list>("topics,t", "topics for sending/receiving messages")
    .add<std::string>("mode,m", "'relay' (default), 'ping', or 'pong'")
    .add<size_t>("payload-size,s",
                 "additional number of bytes for the ping message")
    .add<timespan>("rendezvous-retry",
                   "timeout before repeating the first rendezvous ping "
                   "message (default: 50ms)")
    .add<size_t>("num-messages,n",
                 "number of pings (default: 100, 'ping' mode only)")
    .add<uri_list>("peers", "list of peers we connect to on startup in "
                            "<tcp://$host:$port> notation")
    .add<broker::port>("port,p", "port to listen for incoming Broker peerings")
    .add<string>("endpoint-id", "forces the endpoint to use this ID")
    .add<string_list>("master-stores", "list of stores to attach as masters")
    .add<string_list>("clone-stores", "list of stores to attach as clones");
}

// -- convenience get_or and get_if overloads for enpoint ----------------------

template <class T>
auto get_or(broker::endpoint& d, string_view key, const T& default_value) {
  auto& cfg = broker::internal::endpoint_access(&d).cfg();
  return caf::get_or(cfg, key, default_value);
}

template <class T>
auto get_as(broker::endpoint& d, string_view key) {
  auto& cfg = broker::internal::endpoint_access(&d).cfg();
  return caf::get_as<T>(cfg, key);
}

// -- message creation and introspection ---------------------------------------

/// @pre `is_ping_msg(x) || is_pong_msg(x)`
count msg_id(const broker::data& x) {
  auto& vec = get<broker::vector>(x);
  return get<count>(vec[1]);
}

bool is_ping_msg(const broker::data& x) {
  if (auto vec = get_if<broker::vector>(&x)) {
    if (vec->size() == 3) {
      auto& xs = *vec;
      auto str = broker::get_if<string>(&xs[0]);
      return str && *str == "ping" && broker::is<count>(xs[1])
             && broker::is<string>(xs[2]);
    }
  }
  return false;
}

bool is_pong_msg(const broker::data& x) {
  if (auto vec = get_if<broker::vector>(&x)) {
    if (vec->size() == 2) {
      auto& xs = *vec;
      auto str = broker::get_if<string>(&xs[0]);
      return str && *str == "pong" && broker::is<count>(xs[1]);
    }
  }
  return false;
}

bool is_pong_msg(const broker::data& x, count id) {
  return is_pong_msg(x) && msg_id(x) == id;
}

bool is_stop_msg(const broker::data& x) {
  auto str = get_if<string>(&x);
  return str && *str == "stop";
}

broker::data make_ping_msg(count id, size_t payload_size) {
  return broker::vector{"ping", id, string(payload_size, 'x')};
}

broker::data make_pong_msg(count id) {
  return broker::vector{"pong", id};
}

broker::data make_stop_msg() {
  return "stop";
}

// -- mode implementations -----------------------------------------------------

void relay_mode(broker::endpoint& ep, topic_list topics) {
  if (verbose)
    fmt::print(verbose_style, "relay messages\n");
  auto handle_message = [&](const broker::data_message& x) {
    auto& val = get_data(x);
    if (is_ping_msg(val)) {
      if (verbose)
        fmt::print(verbose_style, "received ping {}\n", msg_id(val));
    } else if (is_pong_msg(val)) {
      if (verbose)
        fmt::print(verbose_style, "received pong {}\n", msg_id(val));
    } else if (is_stop_msg(val)) {
      if (verbose)
        fmt::print(verbose_style, "received stop\n");
      return false;
    }
    return true;
  };
  auto in = ep.make_subscriber(std::move(topics));
  auto& cfg = broker::internal::endpoint_access{&ep}.cfg();
  if (get_or(cfg, "verbose", false) && get_or(cfg, "rate", false)) {
    auto timeout = std::chrono::system_clock::now();
    timeout += std::chrono::seconds(1);
    size_t received = 0;
    for (;;) {
      if (auto maybe_msg = in.get(timeout)) {
        auto msg = std::move(*maybe_msg);
        if (!handle_message(msg))
          return;
        ++received;
      } else {
        if (verbose)
          fmt::print(verbose_style, "{}/s\n", received);
        timeout += std::chrono::seconds(1);
        received = 0;
      }
    }
  } else {
    for (;;) {
      auto x = in.get();
      if (!handle_message(x))
        return;
    }
  }
}

void ping_mode(broker::endpoint& ep, topic_list topics) {
  assert(topics.size() > 0);
  auto topic = topics[0];
  if (verbose)
    fmt::print(verbose_style, "send pings to topic {}\n", topic.string());
  std::vector<timespan> xs;
  auto n = get_or(ep, "num-messages", default_ping_count);
  auto s = get_or(ep, "payload-size", default_payload_size);
  if (n == 0) {
    fmt::print(error_style, "send no pings: n = 0\n");
    return;
  }
  auto in = ep.make_subscriber({topic});
  // Rendezvous between ping and pong. The first ping (id 0) is not part of our
  // measurement. We repeat this initial message until we receive a pong to
  // make sure all broker nodes are up and running.
  bool connected = false;
  auto retry_timeout = get_or(ep, "rendezvous-retry", default_rendezvous_retry);
  ep.publish(topic, make_ping_msg(0, 0));
  while (!connected) {
    auto x = in.get(retry_timeout);
    if (x) {
      auto msg = std::move(*x);
      if (is_pong_msg(get_data(msg), 0))
        connected = true;
    } else {
      ep.publish(topic, make_ping_msg(0, 0));
    }
  }
  // Measurement.
  timespan total_time{0};
  for (count i = 1; i <= n; ++i) {
    bool done = false;
    auto t0 = std::chrono::system_clock::now();
    ep.publish(topic, make_ping_msg(i, s));
    do {
      auto x = in.get();
      done = is_pong_msg(get_data(x), i);
    } while (!done);
    auto t1 = std::chrono::system_clock::now();
    auto roundtrip = std::chrono::duration_cast<timespan>(t1 - t0);
    total_time += roundtrip;
    fmt::print("{}\n", roundtrip.count());
  }
  if (verbose)
    fmt::print(verbose_style, "AVG: {}\n", total_time / n);
}

void pong_mode(broker::endpoint& ep, topic_list topics) {
  assert(topics.size() > 0);
  if (verbose)
    fmt::print(verbose_style, "receive pings from topic {}\n",
               topics[0].string());
  auto in = ep.make_subscriber(std::move(topics));
  for (;;) {
    auto x = in.get();
    auto& val = get_data(x);
    if (is_ping_msg(val)) {
      if (verbose)
        fmt::print(verbose_style, "received ping {}\n", msg_id(val));
      ep.publish(get_topic(x), make_pong_msg(msg_id(val)));
    } else if (is_stop_msg(val)) {
      if (verbose)
        fmt::print(verbose_style, "received stop\n");
      return;
    }
  }
}

} // namespace

// Converts plain port numbers and Zeek-style "<num>/<proto>" notation into a
// 16-bit port number.
std::optional<uint16_t> to_port(std::string str) {
  caf::config_value val{std::move(str)};
  if (auto port_num = caf::get_as<uint16_t>(val))
    return *port_num;
  if (auto port_obj = caf::get_as<broker::port>(val))
    return port_obj->number();
  return std::nullopt;
}

// -- main function ------------------------------------------------------------

int main(int argc, char** argv) try {
  broker::endpoint::system_guard sys_guard; // Initialize global state.
  setvbuf(stdout, nullptr, _IOLBF, 0);      // Always line-buffer stdout.
  // Parse CLI parameters using our config.
  broker::configuration cfg{broker::skip_init};
  extend_config(cfg);
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    fmt::print(error_style, "{}\n", ex.what());
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed())
    return EXIT_SUCCESS;
  // Pick up BROKER_PORT environment variable.
  if (auto env = getenv("BROKER_PORT")) {
    cfg.set("global.port", string{env});
  }
  // Construct the endpoint.
  broker::endpoint_id eid;
  if (auto eid_str = get_as<string>(cfg, "endpoint-id")) {
    if (!convert(*eid_str, eid)) {
      fmt::print(error_style, "endpoint-id must be a valid UUID\n");
      return EXIT_FAILURE;
    }
  } else {
    eid = broker::endpoint_id::random();
  }
  broker::endpoint ep{std::move(cfg), eid};
  // Get mode.
  auto mode = get_or(ep, "mode", "relay");
  // Get process name, using the mode name as fallback.
  node_name = get_or(ep, "name", mode);
  // Get topics (mandatory) and make sure this endpoint at least forwards them.
  topic_list topics;
  { // Lifetime scope of temporary variables.
    auto topic_names = get_or(ep, "topics", string_list{});
    if (topic_names.empty()) {
      fmt::print(error_style, "no topics specified\n");
      return EXIT_FAILURE;
    }
    for (auto& topic_name : topic_names)
      topics.emplace_back(std::move(topic_name));
  }
  ep.forward(topics);
  // Enable verbose output if demanded by user.
  actor verbose_logger;
  if (get_or(ep, "verbose", false)) {
    verbose = true;
    // Launch background worker that prints status and error events when running
    // in verbose mode.
    ep.subscribe({topic::errors(), topic::statuses()},
                 [](const broker::data_message& msg) {
                   if (verbose)
                     fmt::print(verbose_style, "{}\n", to_string(msg));
                 });
  }
  // Publish endpoint at demanded port.
  if (auto port = get_as<broker::port>(ep, "port")) {
    if (verbose)
      fmt::print(verbose_style, "listen for peers on port {}\n",
                 port->number());
    ep.listen({}, port->number());
  }
  // Select function f based on the mode.
  mode_fun f = nullptr;
  if (mode == "relay") {
    f = relay_mode;
  } else if (mode == "ping") {
    f = ping_mode;
  } else if (mode == "pong") {
    f = pong_mode;
  } else {
    fmt::print(error_style, "invalid mode: {}\n", mode);
    return EXIT_FAILURE;
  }
  // Attach master stores.
  std::vector<broker::store> stores;
  for (const auto& name : get_or(ep, "master-stores", string_list{})) {
    if (auto maybe_store = ep.attach_master(name, broker::backend::memory)) {
      stores.emplace_back(std::move(*maybe_store));
    } else {
      fmt::print(error_style, "failed to attach master store for {}: {}\n",
                 name, to_string(maybe_store.error()));
      return EXIT_FAILURE;
    }
  }
  // Attach clone stores.
  for (const auto& name : get_or(ep, "clone-stores", string_list{})) {
    if (auto maybe_store = ep.attach_clone(name)) {
      stores.emplace_back(std::move(*maybe_store));
    } else {
      fmt::print(error_style, "failed to attach clone store for {}: {}\n", name,
                 to_string(maybe_store.error()));
      return EXIT_FAILURE;
    }
  }
  // Connect to peers.
  auto peers = get_or(ep, "peers", uri_list{});
  for (auto& peer : peers) {
    if (auto info = broker::to<broker::network_info>(peer)) {
      if (verbose)
        fmt::print(verbose_style, "connect to {} on port {} ...\n",
                   info->address, info->port);
      if (!ep.peer(*info))
        fmt::print(error_style, "unable to connect to <{}>\n", to_string(peer));
    } else {
      fmt::print(error_style, "invalid URI: (tcp://$authority): <{}>\n",
                 to_string(peer));
    }
  }
  f(ep, std::move(topics));
  // Disconnect from peers.
  for (auto& peer : peers) {
    auto& auth = peer.authority();
    if (peer.scheme() == "tcp" && !auth.empty()) {
      auto host = caf::deep_to_string(auth.host);
      auto port = auth.port;
      if (verbose)
        fmt::print(verbose_style, "disconnect from {} on port {} ...\n", host,
                   port);
      ep.unpeer_nosync(host, port);
    }
  }
  // Stop utility actors.
  anon_send_exit(verbose_logger, exit_reason::user_shutdown);
} catch (std::exception& ex) {
  std::cerr << "*** exception: " << ex.what() << "\n";
  return EXIT_FAILURE;
}
