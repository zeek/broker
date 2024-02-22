#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <vector>

#include <caf/string_algorithms.hpp>
#include <caf/term.hpp>
#include <caf/uri.hpp>

#include "broker/configuration.hh"
#include "broker/domain_options.hh"
#include "broker/gateway.hh"

using namespace broker;

// -- local type aliases -------------------------------------------------------

using uri_list = std::vector<caf::uri>;

// -- I/O utility (TODO: copy-pasted from broker-node.cc -> consolidate) -------

namespace detail {

namespace {

std::mutex ostream_mtx;

} // namespace

int print_impl(std::ostream& ostr, const char* x) {
  ostr << x;
  return 0;
}

int print_impl(std::ostream& ostr, const std::string& x) {
  ostr << x;
  return 0;
}

int print_impl(std::ostream& ostr, const caf::term& x) {
  ostr << x;
  return 0;
}

template <class T>
int print_impl(std::ostream& ostr, const T& x) {
  return print_impl(ostr, caf::deep_to_string(x));
}

template <class... Ts>
void println(std::ostream& ostr, Ts&&... xs) {
  std::unique_lock<std::mutex> guard{ostream_mtx};
  std::initializer_list<int>{print_impl(ostr, std::forward<Ts>(xs))...};
  ostr << caf::term::reset_endl;
}

} // namespace detail

namespace out {

template <class... Ts>
void println(Ts&&... xs) {
  ::detail::println(std::cout, std::forward<Ts>(xs)...);
}

} // namespace out

namespace err {

template <class... Ts>
void println(Ts&&... xs) {
  ::detail::println(std::cerr, caf::term::red, std::forward<Ts>(xs)...);
}

} // namespace err

namespace verbose {

namespace {

std::atomic<bool> enabled;

} // namespace

template <class... Ts>
void println(Ts&&... xs) {
  if (enabled)
    ::detail::println(std::clog, caf::term::blue,
                      std::chrono::system_clock::now(), ": ",
                      std::forward<Ts>(xs)...);
}

} // namespace verbose

// -- configuration ------------------------------------------------------------

class config : public configuration {
public:
  using super = configuration;

  config() : super(skip_init) {
    opt_group{custom_options_, "global"} //
      .add<bool>("verbose,v", "print status and debug output")
      .add<size_t>("retry-interval",
                   "time between peering connection attempts in seconds");
    opt_group{custom_options_, "internal"}
      .add<uri_list>("peers", "list of peers to connect to on startup in "
                              "tcp://$host:$port notation")
      .add<uint16_t>("port", "local port to listen for incoming peerings ")
      .add(internal.disable_forwarding, "disable-forwarding",
           "disable peer-to-peer message forwarding in the internal domain");
    opt_group{custom_options_, "external"}
      .add<uri_list>("peers", "list of peers to connect to on startup in "
                              "tcp://$host:$port notation")
      .add<uint16_t>("port", "local port to listen for incoming peerings ")
      .add(external.disable_forwarding, "disable-forwarding",
           "disable peer-to-peer message forwarding in the external domain");
  }

  using super::init;

  domain_options internal;

  domain_options external;
};

// -- setup and main loop ------------------------------------------------------

int run(gateway& gw) {
  using caf::get_as;
  using caf::get_or;
  auto& cfg = gw.config();
  auto try_listen = [&](caf::string_view key) {
    if (auto local_port = get_as<uint16_t>(cfg, key)) {
      auto p = caf::starts_with(key, "internal.")
                 ? gw.listen_internal({}, *local_port)
                 : gw.listen_external({}, *local_port);
      if (p == 0) {
        err::println("unable to open port ", *local_port);
        return false;
      }
      verbose::println("listen for peers on port ", p);
    }
    return true;
  };
  if (!try_listen("internal.local-port") || !try_listen("external.local-port"))
    return EXIT_FAILURE;
  if (auto peering_failures =
        gw.peer(get_or(cfg, "internal.peers", uri_list{}),
                get_or(cfg, "internal.peers", uri_list{}),
                timeout::seconds{get_or(cfg, "retry-interval", size_t{10})});
      !peering_failures.empty()) {
    for (const auto& [locator, reason] : peering_failures)
      err::println("*** unable to peer with ", locator, ": ", reason);
  }
  out::println("*** gateway up and running, press <enter> to quit");
  getchar();
  return EXIT_SUCCESS;
}

int main(int argc, char** argv) {
  endpoint::system_guard sys_guard;
  // Parse CLI parameters using our config.
  config cfg;
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    err::println("*** unable to initialize config: ", ex.what());
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed)
    return EXIT_SUCCESS;
  if (get_or(cfg, "verbose", false))
    verbose::enabled = true;
  // Create gateway and run.
  auto [internal, external] = std::tie(cfg.internal, cfg.external);
  if (auto gw = gateway::make(std::move(cfg), internal, external)) {
    return run(*gw);
  } else {
    err::println("*** unable to create gateway: ", gw.error());
    return EXIT_FAILURE;
  }
}
