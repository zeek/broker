#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <sys/select.h>
#include <utility>
#include <algorithm>
#include <exception>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include <mutex>
#include <cassert>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <caf/atom.hpp>
#include <caf/behavior.hpp>
#include <caf/deep_to_string.hpp>
#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/send.hpp>
#include <caf/config_option_adder.hpp>
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

void print_line(std::ostream& out, const std::string& line) {
  guard_type guard{cout_mtx};
  out << line << std::endl;
}

class config : public broker::configuration {
public:
  atom_value mode = atom("");
  atom_value impl = atom("blocking");
  std::string topic;
  std::vector<std::string> peers;
  uint16_t local_port = 0;
  size_t message_cap = std::numeric_limits<size_t>::max();
  config() {
    opt_group{custom_options_, "global"}
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
  while (std::getline(std::cin, line) && i++ < cap)
    out.publish(std::move(line));
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
  }
}

void publish_mode_stream(broker::endpoint& ep, const std::string& topic_str,
                         size_t cap) {
  auto worker = ep.publish_all(
    [](size_t& msgs) {
      msgs = 0;
    },
    [=](size_t& msgs, downstream<std::pair<topic, data>>& out, size_t hint) {
      auto num = std::min(cap - msgs, hint);
      std::string line;
      for (size_t i = 0; i < num; ++i)
        if (!std::getline(std::cin, line)) {
          // Reached end of STDIO.
          msgs = cap;
          return;
        } else {
          out.push(std::make_pair(topic_str, std::move(line)));
        }
      msgs += num;
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
  for (size_t i = 0; i < cap; ++i)
    print_line(std::cout, deep_to_string(in.get()));
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
    for (size_t j = 0; j < num; ++j)
      print_line(std::cout, deep_to_string(in.get()));
    i += num;
  }
}

void subscribe_mode_stream(broker::endpoint& ep, const std::string& topic_str,
                    size_t cap) {
  auto worker = ep.subscribe(
    {topic_str},
    [](size_t& msgs) {
      msgs = 0;
    },
    [=](size_t& msgs, std::pair<topic, data> x) {
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

