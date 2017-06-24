#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <map>
#include <mutex>
#include <thread>
#include <cassert>
#include <iostream>

#include <caf/all.hpp>

#include "broker/broker.hh"

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

class config : public broker::configuration {
public:
  atom_value mode;
  atom_value impl = atom("blocking");
  std::string topic;
  std::vector<std::string> peers;
  uint16_t local_port;
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
         "set mode implementation ('blocking' [default], 'select', or 'stream')");
  }
};

void publish_mode_blocking(broker::endpoint& ep, const std::string& topic_str) {
  auto out = ep.make_publisher(topic_str);
  std::string line;
  while (std::getline(std::cin, line)) {
    out.publish(std::move(line));
  }
}

void publish_mode_select(broker::endpoint& ep, const std::string& topic_str) {
  auto out = ep.make_publisher(topic_str);
  auto fd = out.fd();
  std::string line;
  fd_set readset;
  for (;;) {
    FD_ZERO(&readset);
    FD_SET(fd, &readset);
    if (select(fd + 1, &readset, NULL, NULL, NULL) <= 0) {
      std::cerr << "select() failed, errno: " << errno << std::endl;
      return;
    }
    auto j = out.free_capacity();
    assert(j > 0);
    for (size_t i = 0; i < j; ++i)
      if (!std::getline(std::cin, line))
        return; // Reached end of STDIO.
      else
        out.publish(std::move(line));
  }
}

void publish_mode_stream(broker::endpoint& ep, const std::string& topic_str) {
  auto worker = ep.publish_all(
    [](bool& at_end) {
      at_end = false;
    },
    [=](bool& at_end, downstream<std::pair<topic, data>>& out, size_t num) {
      std::string line;
      for (size_t i = 0; i < num; ++i)
        if (!std::getline(std::cin, line)) {
          at_end = true;
          return; // Reached end of STDIO.
        } else {
          out.push(std::make_pair(topic_str, std::move(line)));
        }
    },
    [](const bool& at_end) {
      return at_end;
    },
    // Handle result of the stream.
    [](expected<void>) {
      // nop
    }
  );
  scoped_actor self{ep.system()};
  self->wait_for(worker);
}


void subscribe_mode(broker::endpoint& ep, const std::string& topic_str) {
  auto in = ep.make_subscriber({topic_str});
  std::string line;
  for (;;) {
    line = deep_to_string(in.get());
    guard_type guard{cout_mtx};
    std::cout << line << std::endl;
  }
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
  auto dummy_mode = [](broker::endpoint&, const std::string&) {
    guard_type guard{cout_mtx};
    std::cerr << "*** invalid mode or implementation setting\n";
  };
  using mode_fun = void (*)(broker::endpoint&, const std::string&);
  mode_fun fs[] = {
    publish_mode_blocking,
    publish_mode_select,
    publish_mode_stream,
    subscribe_mode,
    subscribe_mode,
    subscribe_mode,
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
  f(ep, cfg.topic);
  anon_send_exit(el, exit_reason::user_shutdown);
}

