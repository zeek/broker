#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <iterator>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "broker/config.hh"
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

#ifndef BROKER_WINDOWS
#  include <sys/select.h>
#endif // BROKER_WINDOWS

namespace atom = broker::internal::atom;

using broker::data;
using broker::data_message;
using broker::make_data_message;
using broker::topic;

namespace {

std::mutex cout_mtx;

using guard_type = std::unique_lock<std::mutex>;

bool rate = false;

std::atomic<size_t> msg_count{0};

void print_line(std::ostream& out, const std::string& line) {
  guard_type guard{cout_mtx};
  out << line << std::endl;
}

struct parameters {
  std::string mode;
  std::string impl = "blocking";
  std::string topic;
  std::vector<std::string> peers;
  uint64_t local_port = 0;
  uint64_t message_cap = std::numeric_limits<uint64_t>::max();
};

// Adds custom configuration options to the config object.
void extend_config(parameters& param, broker::configuration& cfg) {
  cfg.add_option(&rate, "rate,r",
                 "print the rate of messages once per second instead of the "
                 "message content");
  cfg.add_option(&param.peers, "peers,p",
                 "list of peers we connect to on startup (host:port notation)");
  cfg.add_option(&param.local_port, "local-port,l",
                 "local port for publishing this endpoint at (ignored if 0)");
  cfg.add_option(&param.topic, "topic,t",
                 "topic for sending/receiving messages");
  cfg.add_option(&param.mode, "mode,m", "set mode ('publish' or 'subscribe')");
  cfg.add_option(&param.impl, "impl,i",
                 "set mode implementation ('blocking', 'select', or 'stream')");
  cfg.add_option(&param.message_cap, "message-cap,c",
                 "set a maximum for received/sent messages");
}

void publish_mode_blocking(broker::endpoint& ep, const std::string& topic_str,
                           size_t cap) {
  auto out = ep.make_publisher(topic_str);
  std::string line;
  size_t i = 0;
  while (std::getline(std::cin, line) && i++ < cap) {
    out.publish(std::move(line));
    ++msg_count;
  }
  std::cout << "*** published " << msg_count << " messages, byte\n";
}

#ifdef BROKER_WINDOWS

void publish_mode_select(broker::endpoint&, const std::string&, size_t) {
  std::cerr << "*** select mode not available in MSVC version of Broker\n";
}

#else // BROKER_WINDOWS

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
    if (select(fd + 1, &readset, nullptr, nullptr, nullptr) <= 0) {
      print_line(std::cerr, "select() failed, errno: " + std::to_string(errno));
      return;
    }
    auto num = std::min(cap - i, out.free_capacity());
    assert(num > 0);
    for (size_t j = 0; j < num; ++j)
      if (!std::getline(std::cin, line))
        return; // Reached end of STDIO.
      else
        out.publish(line);
    i += num;
    msg_count += num;
  }
}

#endif // BROKER_WINDOWS

void subscribe_mode_blocking(broker::endpoint& ep, const std::string& topic_str,
                             size_t cap) {
  auto in = ep.make_subscriber({topic_str});
  std::string line;
  for (size_t i = 0; i < cap; ++i) {
    auto msg = in.get();
    if (!rate)
      print_line(std::cout, to_string(msg));
    ++msg_count;
  }
}

#ifdef BROKER_WINDOWS

void subscribe_mode_select(broker::endpoint&, const std::string&, size_t) {
  std::cerr << "*** select mode not available in MSVC version of Broker\n";
}

#else // BROKER_WINDOWS

void subscribe_mode_select(broker::endpoint& ep, const std::string& topic_str,
                           size_t cap) {
  auto in = ep.make_subscriber({topic_str});
  auto fd = in.fd();
  fd_set readset;
  size_t i = 0;
  while (i < cap) {
    FD_ZERO(&readset);
    FD_SET(fd, &readset);
    if (select(fd + 1, &readset, nullptr, nullptr, nullptr) <= 0) {
      print_line(std::cerr, "select() failed, errno: " + std::to_string(errno));
      return;
    }
    auto num = std::min(cap - i, in.available());
    for (size_t j = 0; j < num; ++j) {
      auto msg = in.get();
      if (!rate)
        print_line(std::cout, to_string(msg));
    }
    i += num;
    msg_count += num;
  }
}

#endif // BROKER_WINDOWS

void subscribe_mode_stream(broker::endpoint& ep, const std::string& topic_str,
                           size_t cap) {
  auto worker = ep.subscribe(
    // Filter.
    {topic_str},
    // Init.
    [](size_t& msgs) { msgs = 0; },
    // OnNext.
    [cap](size_t& msgs, const data_message& x) {
      ++msg_count;
      if (!rate)
        print_line(std::cout, to_string(x));
      if (++msgs >= cap)
        throw std::runtime_error("Reached cap");
    },
    [=](size_t&, const broker::error&) {
      // nop
    });
  ep.wait_for(worker);
}
void split(std::vector<std::string>& result, std::string_view str,
           std::string_view delims, bool keep_all = true) {
  size_t pos = 0;
  size_t prev = 0;
  while ((pos = str.find_first_of(delims, prev)) != std::string::npos) {
    auto substr = str.substr(prev, pos - prev);
    if (keep_all || !substr.empty())
      result.emplace_back(substr);
    prev = pos + 1;
  }
  if (prev < str.size())
    result.emplace_back(str.substr(prev));
  else if (keep_all)
    result.emplace_back();
}

} // namespace

int main(int argc, char** argv) try {
  broker::endpoint::system_guard sys_guard;
  // Parse CLI parameters using our config.
  parameters params;
  broker::configuration cfg{broker::skip_init};
  extend_config(params, cfg);
  try {
    cfg.init(argc, argv);
  } catch (std::exception& ex) {
    std::cerr << "*** error while reading config: " << ex.what() << '\n';
    return EXIT_FAILURE;
  }
  if (cfg.cli_helptext_printed()) {
    return EXIT_SUCCESS;
  } else if (!cfg.remainder().empty()) {
    std::cerr << "*** too many arguments\n\n";
    return EXIT_FAILURE;
  }
  broker::endpoint ep{std::move(cfg)};
  ep.subscribe(
    {topic::errors(), topic::statuses()},
    [] {
      // Init: nop.
    },
    [=](const data_message& x) {
      // OnNext: print the message.
      std::string what = to_string(x);
      what.insert(0, "*** ");
      what.push_back('\n');
      guard_type guard{cout_mtx};
      std::cerr << what;
    },
    [=](const broker::error&) {
      // Cleanup: nop.
    });
  // Publish endpoint at demanded port.
  if (params.local_port != 0)
    ep.listen({}, params.local_port);
  // Connect to the requested peers.
  for (auto& p : params.peers) {
    std::vector<std::string> fields;
    split(fields, p, ":");
    if (fields.size() != 2) {
      guard_type guard{cout_mtx};
      std::cerr << "*** invalid peer: " << p << std::endl;
      continue;
    }
    uint16_t port;
    try {
      port = static_cast<uint16_t>(std::stoi(fields.back()));
    } catch (std::exception&) {
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
    auto rate_printer = std::thread{[] {
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
  mode_fun fs[] = {publish_mode_blocking,   publish_mode_select,
                   subscribe_mode_blocking, subscribe_mode_select,
                   subscribe_mode_stream,   dummy_mode};
  std::pair<std::string, std::string> as[] = {
    {"publish", "blocking"}, {"publish", "select"},   {"subscribe", "blocking"},
    {"subscribe", "select"}, {"subscribe", "stream"},
  };
  auto b = std::begin(as);
  auto i = std::find(b, std::end(as), std::make_pair(params.mode, params.impl));
  auto f = fs[std::distance(b, i)];
  f(ep, params.topic, params.message_cap);
} catch (std::exception& ex) {
  std::cerr << "*** exception: " << ex.what() << "\n";
  return EXIT_FAILURE;
}
