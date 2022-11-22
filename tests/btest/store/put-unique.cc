#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/internal/configuration_access.hh"
#include "broker/internal/endpoint_access.hh"
#include "broker/internal/type_id.hh"
#include "broker/publisher.hh"
#include "broker/status.hh"
#include "broker/subscriber.hh"
#include "broker/topic.hh"

#include <caf/actor_system_config.hpp>
#include <caf/behavior.hpp>
#include <caf/config_option_adder.hpp>
#include <caf/deep_to_string.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/init_global_meta_objects.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/type_id.hpp>

#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace broker;
using namespace std::literals;

// -- program options ----------------------------------------------------------

namespace {

std::string mode; // "master" or "clone"

size_t clone_index; // when in "clone" mode: the index of this clone (0-based)

size_t num_clones;

} // namespace

void extend_config(broker::configuration& broker_cfg) {
  auto& cfg = broker::internal::configuration_access(&broker_cfg).cfg();
  caf::config_option_adder{cfg.custom_options(), "global"}
    .add(mode, "mode,m", "'master' or 'clone'")
    .add(clone_index, "clone-index,c", "0-based index of the clone")
    .add(num_clones, "num-clones,n", "number of clones");
}

uint16_t rd_env_port(const char* env_var) {
  // Pick up the environment variable.
  if (auto env = getenv(env_var)) {
    // We accept plain port numbers and Zeek-style "<num>/<proto>" notation.
    caf::config_value val{env};
    if (auto port_num = caf::get_as<uint16_t>(val)) {
      return *port_num;
    } else if (auto port_obj = caf::get_as<broker::port>(val)) {
      return port_obj->number();
    } else {
      fprintf(stderr,
              "invalid value for %s: (expected a non-zero port number)\n",
              env_var);
      ::abort();
    }
  } else {
    fprintf(stderr, "need environment variable %s\n", env_var);
    ::abort();
  }
}

void master_mode(endpoint& ep, std::vector<uint16_t> cl_ports) {
  error err;
  auto port = ep.listen({}, rd_env_port("MASTER_PORT"), &err);
  if (port == 0) {
    auto err_str = to_string(err);
    fprintf(stderr, "endpiont::listen failed: %s\n", err_str.c_str());
    ::abort();
  }
  auto sub = ep.make_status_subscriber(true);
  for (auto cl_port : cl_ports)
    ep.peer_nosync("localhost", cl_port, 1s);
  unsigned connected = 0;
  store foobar;
  bool running = true;
  while (running) {
    auto f = [&](const auto& ev) {
      printf("event: %s\n", to_string(ev).c_str());
      using ev_t = std::decay_t<decltype(ev)>;
      if constexpr (std::is_same_v<ev_t, error>) {
        auto err_str = to_string(ev);
        fprintf(stderr, "ERROR: %s\n", err_str.c_str());
      } else if constexpr (std::is_same_v<ev_t, status>) {
        auto code = ev.code();
        if (code == sc::peer_added) {
          ++connected;
          printf("connected: %u\n", connected);
          if (connected == num_clones) {
            foobar = *ep.attach_master("foobar", backend::memory);
            puts("attached master for 'foobar'");
          }
        } else if (code == sc::peer_removed || code == sc::peer_lost) {
          --connected;
          printf("connected: %u\n", connected);
          if (connected == 0) {
            puts("no clone left: done");
            running = false;
          }
        }
      }
    };
    std::visit(f, sub.get());
  }
}

void run_puts(store::proxy px) {
  // For random delays between the clones.
  std::random_device rd;
  auto sleep_some = [&rd] {
    std::uniform_int_distribution<int> dist(1, 250);
    std::this_thread::sleep_for(std::chrono::milliseconds{dist(rd)});
  };
  // Put the values and wait for responds.
  std::map<std::string, data> entries;
  entries.emplace("key1", "value1");
  entries.emplace("key2", 42);
  entries.emplace("key3", 1.2);
  entries.emplace("key4", "hello world");
  for (auto& [key, val] : entries) {
    sleep_some();
    auto req_id = px.put_unique(data{key}, val);
    printf("%s has request ID %d\n", key.c_str(), static_cast<int>(req_id));
  }
  for (auto i = 0; i < 4; ++i) {
    auto res = px.receive();
    auto answer_str = to_string(res.answer);
    printf("%d -> %s\n", static_cast<int>(res.id), answer_str.c_str());
  }
}

void clone_mode(endpoint& ep, std::vector<uint16_t> cl_ports) {
  error err;
  auto port = ep.listen({}, cl_ports[clone_index], &err);
  if (port == 0) {
    auto err_str = to_string(err);
    fprintf(stderr, "endpiont::listen failed: %s\n", err_str.c_str());
    ::abort();
  }
  for (size_t index = 0; index < num_clones; ++index)
    if (index != clone_index)
      ep.peer_nosync("localhost", cl_ports[index], 1s);
  ep.peer_nosync("localhost", rd_env_port("MASTER_PORT"), 1s);
  store foobar;
  foobar = *ep.attach_clone("foobar");
  puts("attached clone for 'foobar'");
  run_puts(store::proxy{foobar});
}

int main(int argc, char** argv) {
  broker::endpoint::system_guard sys_guard; // Initialize global state.
  setvbuf(stdout, NULL, _IOLBF, 0);         // Always line-buffer stdout.
  // Parse CLI parameters using our config.
  broker::configuration cfg{broker::skip_init};
  extend_config(cfg);
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    fprintf(stderr, "%s\n", ex.what());
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed())
    return EXIT_SUCCESS;
  // Parse environment variables.
  std::vector<uint16_t> cl_ports;
  for (size_t i = 0; i < num_clones; ++i) {
    std::string env_var = "CL_";
    env_var += std::to_string(i);
    env_var += "_PORT";
    cl_ports.push_back(rd_env_port(env_var.c_str()));
  }
  // Run.
  endpoint ep{std::move(cfg)};
  if (mode == "master") {
    master_mode(ep, std::move(cl_ports));
  } else if (mode == "clone") {
    clone_mode(ep, std::move(cl_ports));
  } else {
    fprintf(stderr, "expected mode 'master' or 'clone'\n");
    return EXIT_FAILURE;
  }
  puts("fin");
  return EXIT_SUCCESS;
}
