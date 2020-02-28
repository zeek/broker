#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <sys/select.h>
#include <utility>
#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <cassert>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <caf/atom.hpp>
#include <caf/behavior.hpp>
#include <caf/config_option_adder.hpp>
#include <caf/deep_to_string.hpp>
#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/send.hpp>
#pragma GCC diagnostic pop

#include "broker/atoms.hh"
#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/publisher.hh"
#include "broker/status.hh"
#include "broker/subscriber.hh"
#include "broker/topic.hh"

using broker::data;
using broker::data_message;
using broker::make_data_message;
using broker::topic;

using namespace caf;

namespace {

using publish_atom = atom_constant<atom("publish")>;
using subscribe_atom = atom_constant<atom("subscribe")>;

using blocking_atom = atom_constant<atom("blocking")>;
using select_atom = atom_constant<atom("select")>;
using stream_atom = atom_constant<atom("stream")>;

std::mutex cout_mtx;

using guard_type = std::unique_lock<std::mutex>;

bool rate = false;
std::atomic<size_t> msg_count{0};

void print_line(std::ostream& out, const std::string& line) {
  guard_type guard{cout_mtx};
  out << line << std::endl;
}

class config : public broker::configuration {
public:
  using super = broker::configuration;

  atom_value mode = atom("");
  atom_value impl = atom("blocking");
  std::string topic;
  std::vector<std::string> peers;
  uint16_t local_port = 0;
  size_t message_cap = std::numeric_limits<size_t>::max();
  config() {
    opt_group{custom_options_, "global"}
    .add<bool>(rate, "rate,r",
               "print the rate of messages once per second instead of the "
               "message content")
    .add(peers, "peers,p",
         "list of peers we connect to on startup (host:port notation)")
    .add(local_port, "local-port,l",
         "local port for publishing this endpoint at (ignored if 0)")
    .add(topic, "topic,t",
         "topic for sending/receiving messages")
    .add(mode, "mode,m",
         "set mode ('publish' or 'subscribe')")
    .add(impl, "impl,i",
         "set mode implementation ('blocking', 'select', or 'stream')")
    .add(message_cap, "message-cap,c",
         "set a maximum for received/sent messages");
  }
};

void publish_mode_blocking(broker::endpoint& ep, const std::string& topic_str,
                           size_t cap) {
  auto out = ep.make_publisher(topic_str);
  std::string line;
  size_t i = 0;
  while (std::getline(std::cin, line) && i++ < cap) {
    out.publish(std::move(line));
    ++msg_count;
  }
}

void publish_mode_select(broker::endpoint& ep, const std::string& topic_str,
                         size_t cap) {
  auto out = ep.make_publisher(topic_str);
  auto fd = out.fd();
  std::string line;
  fd_set readset;
  size_t i = 0;
  while (i < cap) {
    FD_ZERO(&readset);
    FD_SET(fd, &readset);
    if (select(fd + 1, &readset, NULL, NULL, NULL) <= 0) {
      print_line(std::cerr, "select() failed, errno: " + std::to_string(errno));
      return;
    }
    auto num = std::min(cap - i, out.free_capacity());
    assert(num > 0);
    for (size_t j = 0; j < num; ++j)
      if (!std::getline(std::cin, line))
        return; // Reached end of STDIO.
      else
        out.publish(std::move(line));
    i += num;
    msg_count += num;
  }
}

void publish_mode_stream(broker::endpoint& ep, const std::string& topic_str,
                         size_t cap) {
  auto worker = ep.publish_all(
    [](size_t& msgs) {
      msgs = 0;
    },
    [=](size_t& msgs, downstream<data_message>& out,
        size_t hint) {
      auto num = std::min(cap - msgs, hint);
      std::string line;
      for (size_t i = 0; i < num; ++i)
        if (!std::getline(std::cin, line)) {
          // Reached end of STDIO.
          msgs = cap;
          return;
        } else {
          out.push(make_data_message(topic_str, std::move(line)));
        }
      msgs += num;
      msg_count += num;
    },
    [=](const size_t& msgs) {
      return msgs == cap;
    }
  );
  scoped_actor self{ep.system()};
  self->wait_for(worker);
}

void subscribe_mode_blocking(broker::endpoint& ep, const std::string& topic_str,
                    size_t cap) {
  auto in = ep.make_subscriber({topic_str});
  std::string line;
  for (size_t i = 0; i < cap; ++i) {
    auto msg = in.get();
    if (!rate)
      print_line(std::cout, deep_to_string(std::move(msg)));
    ++msg_count;
  }
}

void subscribe_mode_select(broker::endpoint& ep, const std::string& topic_str,
                    size_t cap) {
  auto in = ep.make_subscriber({topic_str});
  auto fd = in.fd();
  fd_set readset;
  size_t i = 0;
  while (i < cap) {
    FD_ZERO(&readset);
    FD_SET(fd, &readset);
    if (select(fd + 1, &readset, NULL, NULL, NULL) <= 0) {
      print_line(std::cerr, "select() failed, errno: " + std::to_string(errno));
      return;
    }
    auto num = std::min(cap - i, in.available());
    for (size_t j = 0; j < num; ++j) {
      auto msg = in.get();
      if (!rate)
        print_line(std::cout, deep_to_string(std::move(msg)));
    }
    i += num;
    msg_count += num;
  }
}

void subscribe_mode_stream(broker::endpoint& ep, const std::string& topic_str,
                    size_t cap) {
  auto worker = ep.subscribe(
    {topic_str},
    [](size_t& msgs) {
      msgs = 0;
    },
    [=](size_t& msgs, data_message x) {
      ++msg_count;
      if (!rate)
        print_line(std::cout, deep_to_string(x));
      if (++msgs >= cap)
        throw std::runtime_error("Reached cap");
    },
    [=](size_t&, const caf::error&) {
      // nop
    }
  );
  scoped_actor self{ep.system()};
  self->wait_for(worker);
}

behavior event_listener(event_based_actor* self) {
  self->join(self->system().groups().get_local("broker/errors"));
  self->join(self->system().groups().get_local("broker/statuses"));
  auto print = [](std::string what) {
    what.insert(0, "*** ");
    what.push_back('\n');
    guard_type guard{cout_mtx};
    std::cerr << what;
  };
  return {
    [=](broker::atom::local, error& x) {
      print(self->system().render(x));
    },
    [=](broker::atom::local, broker::status& x) {
      print(to_string(x));
    }
  };
}

} // namespace <anonymous>

int main(int argc, char** argv) {
  // Parse CLI parameters using our config.
  config cfg;
  if (auto err = cfg.parse(argc, argv)) {
    std::cerr << "*** error while reading config: " << cfg.render(err)
              << std::endl;
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed)
    return EXIT_SUCCESS;
  cfg.parse(argc, argv);
  broker::endpoint ep{std::move(cfg)};
  auto el = ep.system().spawn(event_listener);
  // Publish endpoint at demanded port.
  if (cfg.local_port != 0)
    ep.listen({}, cfg.local_port);
  // Connect to the requested peers.
  for (auto& p : cfg.peers) {
    std::vector<std::string> fields;
    split(fields, p, ':');
    if (fields.size() != 2) {
      guard_type guard{cout_mtx};
      std::cerr << "*** invalid peer: " << p << std::endl;
      continue;
    }
    uint16_t port;
    try {
      port = static_cast<uint16_t>(std::stoi(fields.back()));
    } catch(std::exception&) {
      guard_type guard{cout_mtx};
      std::cerr << "*** invalid port: " << fields.back() << std::endl;
      continue;
    }
    ep.peer(fields.front(), port);
  }
  // Run requested mode.
  auto dummy_mode = [](broker::endpoint&, const std::string&, size_t) {
    guard_type guard{cout_mtx};
    std::cerr << "*** invalid mode or implementation setting\n";
  };
  if (rate) {
    auto rate_printer = std::thread{[]{
        size_t msg_count_prev = msg_count;
        while (true) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
          size_t current = msg_count;
          std::cout << current - msg_count_prev << std::endl;
          msg_count_prev = current;
        }
    }};
    rate_printer.detach();
  }
  using mode_fun = void (*)(broker::endpoint&, const std::string&, size_t);
  mode_fun fs[] = {
    publish_mode_blocking,
    publish_mode_select,
    publish_mode_stream,
    subscribe_mode_blocking,
    subscribe_mode_select,
    subscribe_mode_stream,
    dummy_mode
  };
  std::pair<atom_value, atom_value> as[] = {
    {publish_atom::value, blocking_atom::value},
    {publish_atom::value, select_atom::value},
    {publish_atom::value, stream_atom::value},
    {subscribe_atom::value, blocking_atom::value},
    {subscribe_atom::value, select_atom::value},
    {subscribe_atom::value, stream_atom::value}
  };
  auto b = std::begin(as);
  auto i = std::find(b, std::end(as), std::make_pair(cfg.mode, cfg.impl));
  auto f = fs[std::distance(b, i)];
  f(ep, cfg.topic, cfg.message_cap);
  anon_send_exit(el, exit_reason::user_shutdown);
}
