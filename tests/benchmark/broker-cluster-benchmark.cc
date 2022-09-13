#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/after.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/string_algorithms.hpp>
#include <caf/term.hpp>

#include "broker/configuration.hh"
#include "broker/detail/filesystem.hh"
#include "broker/endpoint.hh"
#include "broker/fwd.hh"
#include "broker/internal/generator_file_reader.hh"
#include "broker/internal/generator_file_writer.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"
#include "broker/subscriber.hh"

using broker::internal::native;
using caf::actor_system_config;
using caf::expected;
using caf::get;
using caf::get_if;
using caf::holds_alternative;
using std::string;
using std::chrono::duration_cast;

using string_list = std::vector<string>;

namespace atom = broker::internal::atom;

// -- global constants and type aliases ----------------------------------------

namespace {

using fractional_seconds = std::chrono::duration<double>;

constexpr size_t max_nodes = 500;

struct quoted {
  caf::string_view str;
};

} // namespace

// -- I/O utility --------------------------------------------------------------

namespace detail {

namespace {

std::mutex ostream_mtx;

} // namespace

int print_impl(std::ostream& ostr, const char* x) {
  ostr << x;
  return 0;
}

int print_impl(std::ostream& ostr, const string& x) {
  ostr << x;
  return 0;
}

int print_impl(std::ostream& ostr, const caf::string_view& x) {
  ostr.write(x.data(), x.size());
  return 0;
}

int print_impl(std::ostream& ostr, const quoted& x) {
  ostr << '"' << x.str << '"';
  return 0;
}

int print_impl(std::ostream& ostr, const fractional_seconds& x) {
  ostr << x.count();
  ostr << "s";
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
  detail::println(std::cout, std::forward<Ts>(xs)...);
}

} // namespace out

namespace err {

template <class... Ts>
void println(Ts&&... xs) {
  detail::println(std::cerr, caf::term::red, std::forward<Ts>(xs)...,
                  caf::term::reset);
}

} // namespace err

namespace warn {

template <class... Ts>
void println(Ts&&... xs) {
  detail::println(std::cerr, caf::term::yellow, std::forward<Ts>(xs)...,
                  caf::term::reset);
}

} // namespace warn

namespace verbose {

namespace {

std::atomic<bool> is_enabled;

} // namespace

bool enabled() {
  return is_enabled;
}

template <class... Ts>
void println(Ts&&... xs) {
  if (is_enabled)
    detail::println(std::clog, caf::term::blue, std::forward<Ts>(xs)...,
                    caf::term::reset);
}

} // namespace verbose

// -- utility functions --------------------------------------------------------

namespace {

std::string trim(std::string x) {
  auto predicate = [](int ch) { return !std::isspace(ch); };
  x.erase(x.begin(), std::find_if(x.begin(), x.end(), predicate));
  x.erase(std::find_if(x.rbegin(), x.rend(), predicate).base(), x.end());
  return x;
}

} // namespace

// -- configuration setup ------------------------------------------------------

namespace {

struct config : actor_system_config {
  config() {
    opt_group{custom_options_, "global"}
      .add<std::string>("cluster-config-file,c",
                        "path to the cluster configuration file")
      .add<string>(
        "mode",
        "one of: benchmark (default), dump-stats (print stats for generator "
        "files), generate-config (create a config for given recording), or "
        "shrink-generator-file (reduce entries in a .dat file)")
      .add<bool>("verbose,v", "enable verbose output")
      .add<string_list>("excluded-nodes,e",
                        "excludes given nodes from the setup");
    set("caf.scheduler.max-threads", 1);
    set("caf.logger.file.verbosity", "quiet");
  }

  string usage() {
    return custom_options_.help_text(true);
  }
};

using broker::detail::is_directory;
using broker::detail::is_file;
using broker::detail::read;
using broker::detail::readlines;

} // namespace

// -- data structures for the cluster setup ------------------------------------

using inputs_by_node_map = std::map<std::string, size_t>;

/// A node in the Broker publish/subscribe layer.
struct node {
  /// Stores the unique name of this node.
  std::string name;

  /// Stores the network-wide identifier for this node.
  caf::uri id;

  /// Stores the names of all Broker endpoints we connect to at startup.
  std::set<std::string> peers;

  /// Stores the topics we subscribe to at startup.
  std::vector<std::string> topics;

  /// Optionally stores a path to a generator file.
  std::string generator_file;

  /// Stores how many messages we expect on this node during measurement.
  size_t num_inputs = 0;

  /// Stores whether this node disables forwarding of subscriptions.
  bool disable_forwarding = true;

  /// Stores how many messages we produce using the gernerator file. If `none`,
  /// we produce the number of messages in the generator file.
  std::optional<size_t> num_outputs;

  /// Stores parent nodes in the pub/sub topology.
  std::vector<node*> left;

  /// Stores child nodes in the pub/sub topology. These nodes are the peers we
  /// connect to at startup.
  std::vector<node*> right;

  /// Points to an actor that manages the Broker endpoint.
  caf::actor mgr;

  /// Stores how many inputs we receive per node.
  inputs_by_node_map inputs_by_node;

  /// Stores the CAF log level for this node.
  std::string log_verbosity = "quiet";
};

bool is_sender(const node& x) {
  return !x.generator_file.empty();
}

bool is_receiver(const node& x) {
  return x.num_inputs > 0;
}

bool is_sender_and_receiver(const node& x) {
  return is_sender(x) && is_receiver(x);
}

std::vector<broker::topic> topics(const node& x) {
  std::vector<broker::topic> result;
  for (auto& t : x.topics)
    result.emplace_back(t);
  return result;
}

template <class T>
struct strip_optional {
  using type = T;
};

template <class T>
struct strip_optional<std::optional<T>> {
  using type = T;
};

#define SET_FIELD(field, qualifier)                                            \
  {                                                                            \
    std::string field_name = #field;                                           \
    caf::replace_all(field_name, "_", "-");                                    \
    using field_type = typename strip_optional<decltype(result.field)>::type;  \
    if (auto value = caf::get_as<field_type>(parameters, field_name))          \
      result.field = std::move(*value);                                        \
    else if (auto type_erased_value = caf::get_if(&parameters, field_name))    \
      return make_error(caf::sec::invalid_argument, result.name,               \
                        "illegal type for field", field_name);                 \
    else if (strcmp(#qualifier, "mandatory") == 0)                             \
      return make_error(caf::sec::invalid_argument, result.name,               \
                        "no entry for mandatory field", field_name);           \
  }

expected<node> make_node(const string& name, const caf::settings& parameters) {
  node result;
  result.name = name;
  SET_FIELD(id, mandatory);
  SET_FIELD(peers, optional);
  SET_FIELD(topics, mandatory);
  SET_FIELD(generator_file, optional);
  SET_FIELD(num_inputs, optional);
  SET_FIELD(disable_forwarding, optional);
  SET_FIELD(num_outputs, optional);
  SET_FIELD(inputs_by_node, optional);
  SET_FIELD(log_verbosity, optional);
  if (!result.generator_file.empty() && !is_file(result.generator_file))
    return make_error(caf::sec::invalid_argument, result.name,
                      "generator file does not exist", result.generator_file);
  if (!result.inputs_by_node.empty()) {
    auto plus = [](size_t n, const inputs_by_node_map::value_type& kvp) {
      return n + kvp.second;
    };
    auto total = std::accumulate(result.inputs_by_node.begin(),
                                 result.inputs_by_node.end(), size_t{0}, plus);
    if (total != result.num_inputs)
      warn::println("inconsistent data: ", name, " expects ", result.num_inputs,
                    " messages but inputs-by-node only sums up to ", total);
  }
  return result;
}

struct node_manager_state {
  node* this_node = nullptr;
  union {
    broker::endpoint ep;
  };
  broker::internal::generator_file_reader_ptr generator;
  std::vector<caf::actor> children;

  node_manager_state() {
    // nop
  }

  ~node_manager_state() {
    if (this_node != nullptr)
      ep.~endpoint();
  }

  void init(node* this_node_ptr) {
    BROKER_ASSERT(this_node_ptr != nullptr);
    this_node = this_node_ptr;
    broker::broker_options opts;
    opts.disable_forwarding = this_node_ptr->disable_forwarding;
    opts.disable_ssl = true;
    opts.ignore_broker_conf = true; // Make sure no one messes with our setup.
    broker::configuration cfg{opts};
    cfg.set("caf.middleman.workers", uint64_t{0u});
    cfg.set("caf.logger.file.path", this_node->name + ".log");
    cfg.set("caf.logger.file.verbosity", this_node->log_verbosity);
    new (&ep) broker::endpoint(std::move(cfg));
  }
};

using node_manager_actor = caf::stateful_actor<node_manager_state>;

struct generator_state {
  static inline const char* name = "broker.benchmark.generator";
};

void generator(caf::stateful_actor<generator_state>* self, node* this_node,
               caf::actor core,
               broker::internal::generator_file_reader_ptr ptr) {
  using generator_ptr = broker::internal::generator_file_reader_ptr;
  using value_type = broker::node_message::value_type;
  if (this_node->num_outputs != caf::none) {
    struct state {
      generator_ptr gptr;
      size_t remaining;
      size_t pushed = 0;
    };
    attach_stream_source(
      self, core,
      [&](state& st) {
        // Take ownership of `ptr`.
        st.gptr = std::move(ptr);
        st.remaining = *this_node->num_outputs;
      },
      [=](state& st, caf::downstream<value_type>& out, size_t hint) {
        if (st.gptr == nullptr)
          return;
        auto n = std::min(hint, st.remaining);
        for (size_t i = 0; i < n; ++i) {
          if (st.gptr->at_end())
            st.gptr->rewind();
          value_type x;
          if (auto err = st.gptr->read(x)) {
            err::println("error while parsing ", this_node->generator_file,
                         ": ", to_string(err));
            st.gptr = nullptr;
            st.remaining = 0;
            return;
          }
          out.push(std::move(x));
        }
        st.remaining -= n;
        if (st.pushed / 1000 != (st.pushed + n) / 1000)
          verbose::println(this_node->name, " pushed ", st.pushed + n,
                           " messages");
        st.pushed += n;
      },
      [](const state& st) { return st.remaining == 0; });
  } else {
    struct state {
      generator_ptr gptr;
      size_t pushed = 0;
    };
    auto done = [](const state& st) {
      // We are done when gptr becomes null as result of an error or when
      // reacing the end of the generator file.
      return st.gptr == nullptr || st.gptr->at_end();
    };
    attach_stream_source(
      self, core,
      [&](state& st) {
        // Take ownership of `ptr`.
        st.gptr = std::move(ptr);
      },
      [=](state& st, caf::downstream<value_type>& out, size_t hint) {
        size_t n = 0;
        for (; n < hint; ++n) {
          if (done(st))
            break;
          value_type x;
          if (auto err = st.gptr->read(x)) {
            err::println("error while parsing ", this_node->generator_file,
                         ": ", to_string(err));
            st.gptr = nullptr;
            break;
          }
          out.push(std::move(x));
        }
        // Make some noise every 1k messages or when done.
        if (done(st) || st.pushed / 1000 != (st.pushed + n) / 1000)
          verbose::println(this_node->name, " pushed ", st.pushed + n,
                           " messages");
        st.pushed += n;
      },
      done);
  }
}

void run_send_mode(node_manager_actor* self, caf::actor observer) {
  auto this_node = self->state.this_node;
  verbose::println(this_node->name, " starts publishing");
  auto t0 = std::chrono::steady_clock::now();
  auto g = self->spawn(generator, this_node, native(self->state.ep.core()),
                       std::move(self->state.generator));
  g->attach_functor([this_node, t0, observer]() mutable {
    auto t1 = std::chrono::steady_clock::now();
    anon_send(observer, atom::ok_v, atom::write_v, this_node->name,
              duration_cast<caf::timespan>(t1 - t0));
  });
}

struct consumer_state {
  consumer_state(caf::event_based_actor* self) : self(self) {
    start = std::chrono::steady_clock::now();
  }

  ~consumer_state() {
    if (received > this_node->num_inputs)
      warn::println(this_node->name, " received ", received,
                    " messages but only expected ", this_node->num_inputs);
  }

  void handle_messages(size_t n) {
    // Make some noise every 1k messages.
    if (received / 1000 != (received + n) / 1000)
      verbose::println(this_node->name, " got ", received + n, " messages");
    // Inform the observer when reaching the node's limit.
    auto limit = this_node->num_inputs;
    if (received < limit && received + n >= limit) {
      auto stop = std::chrono::steady_clock::now();
      anon_send(observer, atom::ok_v, atom::read_v, this_node->name,
                duration_cast<caf::timespan>(stop - start));
      verbose::println(this_node->name, " reached its limit");
    }
    received += n;
  }

  template <class T>
  void attach_sink(caf::stream<T> in, caf::actor observer) {
    if (++connected_streams == 2) {
      self->send(observer, atom::ack_v);
      verbose::println(this_node->name, " waits for messages");
    }
    attach_stream_sink(
      self, in,
      [](caf::unit_t&) {
        // nop
      },
      [=](caf::unit_t&, std::vector<T>& xs) { handle_messages(xs.size()); },
      [=](caf::unit_t&, const caf::error& err) {
        verbose::println(this_node->name, " stops receiving ",
                         caf::type_name_v<T>);
      });
  }

  size_t received = 0;
  node* this_node;
  caf::event_based_actor* self;
  size_t connected_streams = 0;
  std::chrono::steady_clock::time_point start;
  caf::actor observer;

  static inline const char* name = "broker.benchmark.consumer";
};

caf::behavior consumer(caf::stateful_actor<consumer_state>* self,
                       node* this_node, caf::actor core, caf::actor observer) {
  self->state.this_node = this_node;
  self->state.observer = observer;
  self->send(self * core, atom::join_v, topics(*this_node));
  self->send(self * core, atom::join_v, atom::data_store_v, topics(*this_node));
  if (!verbose::enabled())
    return {
      [=](caf::stream<broker::data_message> in) {
        self->state.attach_sink(in, observer);
      },
      [=](caf::stream<broker::command_message> in) {
        self->state.attach_sink(in, observer);
      },
    };
  size_t last_printed_count = 0;
  return {
    [=](caf::stream<broker::data_message> in) {
      self->state.attach_sink(in, observer);
    },
    [=](caf::stream<broker::command_message> in) {
      self->state.attach_sink(in, observer);
    },
    caf::after(std::chrono::seconds(1)) >>
      [=]() mutable {
        if (last_printed_count != self->state.received) {
          verbose::println(this_node->name,
                           " received nothing for 1s, last count: ",
                           self->state.received);
          last_printed_count = self->state.received;
        }
      },
  };
}

void run_receive_mode(node_manager_actor* self, caf::actor observer) {
  auto this_node = self->state.this_node;
  auto core = native(self->state.ep.core());
  auto c = self->spawn(consumer, this_node, core, observer);
  self->state.children.emplace_back(c);
}

// Using to_string on the host directly can result in surrounding quotes if the
// host type is stored as a string.
std::string host_to_string(const caf::uri::host_type& x) {
  auto f = [](const auto& inner) -> std::string {
    using inner_type = std::decay_t<decltype(inner)>;
    if constexpr (std::is_same_v<inner_type, std::string>)
      return inner;
    else
      return to_string(inner);
  };
  return caf::visit(f, x);
}

caf::error try_connect(broker::endpoint& ep, broker::status_subscriber& ss,
                       const node* this_node, const node* peer) {
  const auto& authority = peer->id.authority();
  auto host = host_to_string(authority.host);
  ep.peer(host, authority.port, broker::timeout::seconds(1));
  for (;;) {
    auto ss_res = ss.get();
    using namespace broker;
    if (holds_alternative<none>(ss_res))
      continue;
    if (auto err = get_if<error>(&ss_res))
      return internal::native(*err);
    BROKER_ASSERT(holds_alternative<status>(ss_res));
    auto& ss_stat = get<status>(ss_res);
    auto code = ss_stat.code();
    if (code == sc::unspecified)
      continue;
    if (code == sc::peer_removed || code == sc::peer_lost)
      return caf::make_error(caf::sec::runtime_error, this_node->name,
                             "lost connection to a peer");
    BROKER_ASSERT(code == sc::peer_added);
    if (auto ctx = ss_stat.context<endpoint_info>()) {
      auto& net = ctx->network;
      if (net && net->address == host && net->port == authority.port)
        return caf::none;
    }
  }
}

caf::behavior node_manager(node_manager_actor* self, node* this_node) {
  self->state.init(this_node);
  // Make sure we subscribe to all topics locally *before* we initiate peering.
  // Otherwise, we get a race on the topics and can "loose" initial messages.
  // Despite its name, endpoint::forward does not force any forwarding. It only
  // makes sure that the topic is in our local filter.
  if (is_receiver(*this_node))
    self->state.ep.forward(topics(*this_node));
  return {
    [=](atom::init) -> caf::result<atom::ok> {
      // Open up the ports and start peering.
      auto& st = self->state;
      if (this_node->id.scheme() == "tcp") {
        auto& authority = this_node->id.authority();
        auto addr = host_to_string(authority.host);
        verbose::println(this_node->name, " starts listening at ", addr, ":",
                         authority.port);
        auto port = st.ep.listen(addr, authority.port);
        if (port != authority.port) {
          err::println(this_node->name, " opened port ", port, " instead of ",
                       authority.port);
          return make_error(caf::sec::runtime_error, this_node->name,
                            "listening failed");
        }
      }
      // Connect to all peers and wait for handshake success.
      if (!this_node->right.empty()) {
        auto ss = st.ep.make_status_subscriber(true);
        for (const auto* peer : this_node->right) {
          verbose::println(this_node->name, " starts peering to ",
                           peer->id.authority(), " (", peer->name, ")");
          // Try to connect up to 5 times per peer before giving up.
          auto connected = false;
          for (int i = 1; !connected && i <= 5; ++i) {
            if (auto err = try_connect(st.ep, ss, this_node, peer)) {
              if (i == 5) {
                err::println(this_node->name,
                             " received an error while trying to peer to ",
                             peer->name, " on the 5th try: ", err);
                return std::move(err);
              } else {
                verbose::println(this_node->name,
                                 " received an error while trying to peer to ",
                                 peer->name, " (try again): ", err);
              }
            } else {
              verbose::println(this_node->name, " successfully peered to ",
                               peer->id, " (", peer->name, ")");
              connected = true;
            }
          }
        }
      }
      if (is_sender(*this_node)) {
        using broker::internal::make_generator_file_reader;
        st.generator = make_generator_file_reader(this_node->generator_file);
        if (st.generator == nullptr)
          return make_error(caf::sec::cannot_open_file,
                            this_node->generator_file);
      }
      verbose::println(this_node->name, " up and running");
      return atom::ok_v;
    },
    [=](atom::read, caf::actor observer) { run_receive_mode(self, observer); },
    [=](atom::write, caf::actor observer) { run_send_mode(self, observer); },
    [=](atom::shutdown) -> caf::result<atom::ok> {
      for (auto& child : self->state.children)
        self->send_exit(child, caf::exit_reason::user_shutdown);
      // Tell broker to shutdown. This is a blocking function call.
      self->state.ep.shutdown();
      verbose::println(this_node->name, " down");
      return atom::ok_v;
    },
  };
}

void launch(caf::actor_system& sys, node& x) {
  x.mgr = sys.spawn<caf::detached>(node_manager, &x);
}

// -- utility functions --------------------------------------------------------

node* node_by_name(std::vector<node>& nodes, const string& name) {
  auto predicate = [&](const node& x) { return x.name == name; };
  auto i = std::find_if(nodes.begin(), nodes.end(), predicate);
  if (i == nodes.end())
    return nullptr;
  return &(*i);
}

bool build_node_tree(std::vector<node>& nodes) {
  for (auto& x : nodes) {
    for (auto& peer_name : x.peers) {
      auto peer = node_by_name(nodes, peer_name);
      if (peer == nullptr) {
        err::println(x.name, " cannot peer to unknown node ", peer_name);
        return false;
      }
      if (&x == peer) {
        err::println(x.name, " cannot peer with itself");
        return false;
      }
      x.right.emplace_back(peer);
      peer->left.emplace_back(&x);
    }
  }
  // Sanity check: each node must be part of the multi-root tree.
  for (auto& x : nodes) {
    if (x.left.empty() && x.right.empty()) {
      err::println(x.name, " has no peering relation to any other node");
      return false;
    }
  }
  // Reduce the number of connections on startup to a minimum: if A peers to B
  // and B peers to A, then we can safely drop the "B peers to A" part from the
  // config.
  for (auto& x : nodes) {
    for (auto& y : x.right) {
      auto& y_peers = y->right;
      if (auto i = std::find(y_peers.begin(), y_peers.end(), &x);
          i != y_peers.end()) {
        // x peers to y and y peers to x on startup -> drop the latter relation.
        y->peers.erase(x.name);
        y_peers.erase(i);
        if (auto j = std::find(x.left.begin(), x.left.end(), y);
            j != x.left.end())
          x.left.erase(j);
      }
    }
  }
  return true;
}

bool verify_node_tree(std::vector<node>& nodes) {
  // Sanity check: there must at least one receiver.
  if (std::none_of(nodes.begin(), nodes.end(), is_receiver)) {
    err::println("no node expects to receive any data");
    return false;
  }
  // Sanity check: there must at least one sender.
  if (std::none_of(nodes.begin(), nodes.end(), is_sender)) {
    err::println("no node has a generator file for publishing data");
    return false;
  }
  // Sanity check: each nodes must send and/or receive.
  auto predicate = [](const node& n) { return is_sender(n) || is_receiver(n); };
  { // Lifetime scope of i.
    auto i = std::find_if_not(nodes.begin(), nodes.end(), predicate);
    if (i != nodes.end()) {
      err::println(i->name, " neither receives nor sends");
      return false;
    }
  }
  return true;
}

int generate_config(string_list directories) {
  constexpr const char* required_files[] = {
    "/id.txt", "/messages.dat", "/peers.txt", "/topics.txt", "/broker.conf",
  };
  // Make sure we always produce a stable config file that does not depend on
  // argument ordering.
  std::sort(directories.begin(), directories.end());
  // Remove trailing slashes to make working with the directories easier.
  verbose::println("scan ", directories.size(), " directories");
  for (auto& directory : directories) {
    while (caf::ends_with(directory, "/"))
      directory.pop_back();
    if (!is_directory(directory)) {
      err::println('\"', directory, "\" is not a directory");
      return EXIT_FAILURE;
    }
  }
  // Use the directory name as node name and read directory contents.
  verbose::println("read recorded files and build node tree");
  std::map<std::string, std::string> node_to_handle;
  std::map<std::string, std::string> handle_to_node;
  std::vector<node> nodes;
  verbose::println("first pass: extract IDs, config and subscriptions");
  for (const auto& directory : directories) {
    verbose::println("scan ", directory);
    for (auto fname : required_files) {
      auto fpath = directory + fname;
      if (!is_file(fpath)) {
        err::println("missing file: ", fpath);
        return EXIT_FAILURE;
      }
    }
    verbose::println("extract the node name and check uniqueness");
    std::string name;
    auto sep = directory.find_last_of('/');
    if (sep != std::string::npos)
      name = directory.substr(sep + 1);
    else
      name = directory;
    if (node_by_name(nodes, name) != nullptr) {
      err::println("node name \"", name, "\" appears twice");
      return EXIT_FAILURE;
    }
    nodes.emplace_back();
    auto& node = nodes.back();
    node.name = name;
    verbose::println("read id.txt and make sure it contains a unique ID");
    auto handle = trim(read(directory + "/id.txt"));
    if (handle.empty()) {
      err::println("empty file: ", directory + "/id.txt");
      return EXIT_FAILURE;
    }
    auto predicate = [&](const std::pair<const std::string, std::string>& x) {
      return x.second == handle;
    };
    if (handle_to_node.count(handle) != 0) {
      err::println("node ID: ", handle, " appears twice");
      return EXIT_FAILURE;
    }
    node_to_handle.emplace(name, handle);
    handle_to_node.emplace(handle, name);
    // Set various node fields.
    auto to_set = [](std::vector<std::string>&& xs) {
      return std::set<std::string>{std::make_move_iterator(xs.begin()),
                                   std::make_move_iterator(xs.end())};
    };
    node.generator_file = directory + "/messages.dat";
    node.peers = to_set(readlines(directory + "/peers.txt", false));
    verbose::println("read and de-duplicate topics from topics.txt");
    node.topics = readlines(directory + "/topics.txt", false);
    std::sort(node.topics.begin(), node.topics.end());
    auto e = std::unique(node.topics.begin(), node.topics.end());
    if (e != node.topics.end())
      node.topics.erase(e, node.topics.end());
    verbose::println("fetch config parameters for this node from broker.conf");
    auto conf_file = directory + "/broker.conf";
    if (auto conf = actor_system_config::parse_config_file(conf_file.c_str())) {
      // Older versions of Broker use 'broker.forward' as config parameter.
      if (auto val = caf::get_if<bool>(std::addressof(*conf), "broker.forward"))
        node.disable_forwarding = !*val;
      else
        node.disable_forwarding =
          caf::get_or(*conf, "broker.disable-forwarding", false);
    } else {
      err::println("unable to parse ", quoted{conf_file}, ": ",
                   to_string(conf.error()));
      return EXIT_FAILURE;
    }
  }
  verbose::println("second pass: resolve all peer handles");
  for (auto& node : nodes) {
    // Currently, node.peers contains CAF node IDs. Now that we computed unique
    // names for each peer, we replace the cryptic IDs with the names.
    std::set<std::string> peer_names;
    for (auto& peer : node.peers) {
      if (handle_to_node.count(peer) == 0) {
        err::println("missing data: cannot resolve peer ID ", peer);
        return EXIT_FAILURE;
      }
      auto peer_name = handle_to_node[peer];
      if (peer_name == node.name) {
        err::println("corrupted data: ", peer, " cannot peer with itself");
        return EXIT_FAILURE;
      }
      peer_names.emplace(std::move(peer_name));
    }
    std::swap(node.peers, peer_names);
  }
  verbose::println("reconstruct node tree");
  if (!build_node_tree(nodes))
    return EXIT_FAILURE;
  // Compute for each node how many messages it produces per topic.
  verbose::println("read generator files and compute outputs per node",
                   " (may take a while)");
  using output_map = std::map<std::string, size_t>;
  std::map<std::string, output_map> outputs;
  for (const auto& node : nodes) {
    auto gptr =
      broker::internal::make_generator_file_reader(node.generator_file);
    if (gptr == nullptr) {
      err::println("unable to open generator file: ", node.generator_file);
      return EXIT_FAILURE;
    }
    auto& out = outputs[node.name];
    broker::internal::generator_file_reader::value_type value;
    while (!gptr->at_end()) {
      if (auto err = gptr->read(value)) {
        err::println("error while reading generator file ", node.generator_file,
                     ": ", err);
        return EXIT_FAILURE;
      }
      out[get_topic(value).string()] += 1;
    }
  }
  // Now we compute the inputs at each node.
  using filter = std::vector<std::string>;
  auto concat_filters = [](const filter& x, const filter& y) {
    filter result;
    std::set_intersection(x.begin(), x.end(), y.begin(), y.end(),
                          std::back_inserter(result));
    return result;
  };
  auto step = [](node& src, node& dst, const output_map& out, const filter& f) {
    using caf::starts_with;
    size_t num_inputs = 0;
    for (auto& kvp : out) {
      auto matches = [&](const string& x) { return starts_with(kvp.first, x); };
      if (std::any_of(f.begin(), f.end(), matches))
        num_inputs += kvp.second;
    }
    if (num_inputs > 0) {
      dst.num_inputs += num_inputs;
      dst.inputs_by_node[src.name] += num_inputs;
    }
  };
  using walk_fun = std::function<std::vector<node*>(const node&)>;
  walk_fun walk_left = [](const node& n) { return n.left; };
  walk_fun walk_right = [](const node& n) { return n.right; };
  using traverse_t = void(node&, node&, const output_map&, const filter&,
                          walk_fun);
  std::function<traverse_t> traverse;
  traverse = [&](node& src, node& dst, const output_map& out, const filter& f,
                 walk_fun walk) {
    step(src, dst, out, f);
    if (dst.disable_forwarding)
      return;
    // TODO: take TTL counter into consideration
    for (auto peer : walk(dst)) {
      auto f_peer = concat_filters(f, peer->topics);
      if (!f_peer.empty())
        traverse(src, *peer, out, f, walk);
    }
  };
  for (auto& node : nodes) {
    const auto& out = outputs[node.name];
    step(node, node, out, node.topics);
    for (auto peer : node.left)
      traverse(node, *peer, out, peer->topics, walk_left);
    for (auto peer : node.right)
      traverse(node, *peer, out, peer->topics, walk_right);
  }
  verbose::println("compute inputs per node");
  // Finally, we need to assign IDs. We simply use localhost with increasing
  // port number for any node with incoming connections.
  verbose::println("generate IDs for all nodes");
  uint16_t port = 8000;
  for (auto& node : nodes) {
    std::string uri_str;
    if (node.left.empty()) {
      uri_str += "local:";
      uri_str += node.name;
    } else {
      uri_str += "tcp://127.0.0.1:";
      uri_str += std::to_string(port++);
    }
    if (auto err = caf::parse(uri_str, node.id)) {
      err::println("generated invalid URI ID: \"", uri_str, "\"");
      return EXIT_FAILURE;
    }
  }
  // Print generated config and return.
  verbose::println("done 🎉");
  auto print_field = [&](const char* name, const auto& xs) {
    if (xs.empty())
      return;
    out::println("    ", name, " = [");
    for (const auto& x : xs)
      out::println("      ", quoted{x}, ",");
    out::println("    ]");
  };
  out::println("nodes {");
  for (const auto& node : nodes) {
    out::println("  ", node.name, " {");
    out::println("    id = <", node.id, ">");
    if (node.num_inputs > 0) {
      out::println("    num-inputs = ", node.num_inputs);
      out::println("    inputs-by-node {");
      for (auto& kvp : node.inputs_by_node)
        out::println("      ", kvp.first, " = ", kvp.second);
      out::println("    }");
    }
    out::println("    disable_forwarding = ", node.disable_forwarding);
    print_field("topics", node.topics);
    print_field("peers", node.peers);
    if (!node.generator_file.empty() && !outputs[node.name].empty())
      out::println("    generator-file = ", quoted{node.generator_file});
    out::println("  }");
  }
  out::println("}");
  return EXIT_SUCCESS;
}

int shrink_generator_file(const string& in_file, const string& out_file,
                          size_t new_size) {
  if (!broker::detail::is_file(in_file)) {
    err::println("input file ", in_file, " not found");
    return EXIT_FAILURE;
  }
  if (broker::detail::is_file(out_file)) {
    err::println("output file ", out_file, " already exists");
    return EXIT_FAILURE;
  }
  auto gptr = broker::internal::make_generator_file_reader(in_file);
  if (gptr == nullptr) {
    err::println("unable to open ", in_file, " as generator file");
    return EXIT_FAILURE;
  }
  auto out = fopen(out_file.c_str(), "w");
  if (out == nullptr) {
    err::println("unable to open ", out_file, " for writing");
    return EXIT_FAILURE;
  }
  using format = broker::internal::generator_file_writer::format;
  auto out_guard = caf::detail::make_scope_guard([out] { fclose(out); });
  auto header = format::header();
  if (fwrite(header.data(), 1, header.size(), out) != header.size()) {
    err::println("unable to write to ", out_file);
    return EXIT_FAILURE;
  }
  int return_code = EXIT_SUCCESS;
  using value_type = broker::internal::generator_file_reader::value_type;
  using bytes = caf::span<const caf::byte>;
  auto f = [&, i{size_t{0}}](value_type* val, bytes chunk) mutable {
    if (fwrite(chunk.data(), 1, chunk.size(), out) != chunk.size()) {
      err::println("unable to write to ", out_file);
      return_code = EXIT_FAILURE;
      return false;
    }
    if (val && ++i == new_size)
      return false;
    return true;
  };
  if (auto err = gptr->read_raw(f)) {
    err::println("error while reading the generator file ", to_string(err));
    return EXIT_FAILURE;
  }
  return return_code;
}

int shrink_generator_file(string_list args) {
  if (args.size() != 3) {
    err::println("invalid arguments to shrink-generator-file mode");
    err::println("expected three positional arguments: INPUT OUTPUT NEW_SIZE");
    return EXIT_FAILURE;
  }
  size_t new_size;
  try {
    new_size = std::stoul(args[2]);
  } catch (std::exception& ex) {
    err::println("unable to parse NEW_SIZE argument: ", ex.what());
    err::println("expected three positional arguments: INPUT OUTPUT NEW_SIZE");
    return EXIT_FAILURE;
  }
  return shrink_generator_file(args[0], args[1], new_size);
}

// -- main ---------------------------------------------------------------------

void print_peering_node(const std::string& prefix, const node& x, bool is_last,
                        std::set<std::string>& printed_nodes) {
  caf::string_view first_prefix;
  caf::string_view inner_prefix;
  auto print_topics = [&] {
    if (printed_nodes.count(x.name) != 0) {
      verbose::println(prefix, inner_prefix, "│   └── (see above)");
      return;
    }
    auto num_topics = x.topics.size();
    if (num_topics > 0) {
      for (size_t i = 0; i < num_topics - 1; ++i)
        verbose::println(prefix, inner_prefix, "│   ├── ", x.topics[i]);
      verbose::println(prefix, inner_prefix, "│   └── ",
                       x.topics[num_topics - 1]);
    }
  };
  std::string next_prefix = prefix;
  if (x.left.empty()) {
    next_prefix += "    ";
  } else if (is_last) {
    first_prefix = "└── ";
    inner_prefix = "    ";
    next_prefix += "        ";
  } else {
    first_prefix = "├── ";
    inner_prefix = "│   ";
    next_prefix += "│       ";
  }
  verbose::println(prefix, first_prefix, x.name);
  verbose::println(prefix, inner_prefix, "├── topics:");
  print_topics();
  verbose::println(prefix, inner_prefix, "└── peers:");
  printed_nodes.emplace(x.name);
  if (x.right.empty()) {
    verbose::println(next_prefix, "└── (none)");
    return;
  }
  for (size_t i = 0; i < x.right.size(); ++i)
    print_peering_node(next_prefix, *x.right[i], i == x.right.size() - 1,
                       printed_nodes);
}

enum program_mode_t {
  invalid_mode,
  benchmark_mode,
  dump_stats_mode,
  generate_config_mode,
  shrink_generator_file_mode,
};

program_mode_t get_mode(const config& cfg) {
  auto mode_str = get_if<std::string>(&cfg, "mode");
  if (!mode_str || *mode_str == "benchmark")
    return benchmark_mode;
  else if (*mode_str == "dump-stats")
    return dump_stats_mode;
  else if (*mode_str == "generate-config")
    return generate_config_mode;
  else if (*mode_str == "shrink-generator-file")
    return shrink_generator_file_mode;
  else
    return invalid_mode;
}

int main(int argc, char** argv) {
  broker::configuration::init_global_state();
  // Read CAF configuration.
  config cfg;
  if (auto err = cfg.parse(argc, argv)) {
    err::println("unable to parse CAF config: ", to_string(err));
    return EXIT_FAILURE;
  }
  // Exit for `--help` etc.
  if (cfg.cli_helptext_printed)
    return EXIT_SUCCESS;
  // Enable global flags and fetch mode of operation.
  if (get_or(cfg, "verbose", false))
    verbose::is_enabled = true;
  auto mode = get_mode(cfg);
  if (mode == invalid_mode) {
    err::println("invalid mode");
    return EXIT_FAILURE;
  }
  // Dispatch to modes that don't read a cluster config.
  if (mode == generate_config_mode)
    return generate_config(cfg.remainder);
  else if (mode == shrink_generator_file_mode)
    return shrink_generator_file(cfg.remainder);
  // Read cluster config.
  auto excluded_nodes = get_or(cfg, "excluded-nodes", string_list{});
  auto is_excluded = [&](const string& node_name) {
    auto e = excluded_nodes.end();
    return std::find(excluded_nodes.begin(), e, node_name) != e;
  };
  caf::settings cluster_config;
  if (auto path = get_if<string>(&cfg, "cluster-config-file")) {
    if (*path == "-") {
      if (auto file_content = config::parse_config(std::cin)) {
        cluster_config = std::move(*file_content);
      } else {
        err::println("unable to parse cluster config from STDIN");
        return EXIT_FAILURE;
      }
    } else if (auto file_content = config::parse_config_file(path->c_str())) {
      cluster_config = std::move(*file_content);
    } else {
      err::println("unable to parse cluster config file: ",
                   to_string(file_content.error()));
      return EXIT_FAILURE;
    }
  } else {
    err::println("cluster-config-file missing");
    out::println();
    out::println(cfg.usage());
    return EXIT_FAILURE;
  }
  // Check for dump-stats mode.
  if (mode == dump_stats_mode) {
    std::vector<string> file_names;
    std::function<void(const caf::settings&)> read_file_names;
    read_file_names = [&](const caf::settings& xs) {
      for (const auto& kvp : xs) {
        if (kvp.first == "generator-file"
            && holds_alternative<string>(kvp.second)) {
          file_names.emplace_back(get<string>(kvp.second));
        } else if (auto submap = get_if<caf::settings>(&kvp.second)) {
          read_file_names(*submap);
        }
      }
    };
    read_file_names(cluster_config["nodes"].as_dictionary());
    if (file_names.empty()) {
      err::println("no generator files found in config");
      return EXIT_FAILURE;
    }
    for (const auto& file_name : file_names) {
      auto gptr = broker::internal::make_generator_file_reader(file_name);
      if (gptr == nullptr) {
        err::println("unable to open generator file: ", file_name);
        continue;
      }
      size_t total_entries = 0;
      size_t data_entries = 0;
      size_t command_entries = 0;
      std::map<broker::topic, size_t> entries_by_topic;
      broker::node_message_content x;
      while (!gptr->at_end()) {
        if (auto err = gptr->read(x)) {
          err::println("error while parsing ", file_name, ": ", to_string(err));
          return EXIT_FAILURE;
        }
        ++total_entries;
        if (is_data_message(x))
          ++data_entries;
        else
          ++command_entries;
        entries_by_topic[get_topic(x)] += 1;
      }
      out::println(file_name);
      out::println("├── entries: ", total_entries);
      out::println("|   ├── data-entries: ", data_entries);
      out::println("|   └── command-entries: ", command_entries);
      out::println("└── topics:");
      if (!entries_by_topic.empty()) {
        auto i = entries_by_topic.begin();
        auto e = std::prev(entries_by_topic.end());
        for (; i != e; ++i)
          out::println("    ├── ", i->first.string(), " (", i->second, ")");
        out::println("    └── ", i->first.string(), " (", i->second, ")");
      }
    }
    return EXIT_SUCCESS;
  }
  // Generate nodes from cluster config.
  std::vector<node> nodes;
  for (auto& kvp : cluster_config["nodes"].as_dictionary()) {
    if (is_excluded(kvp.first))
      continue;
    if (auto x = make_node(kvp.first, kvp.second.as_dictionary())) {
      nodes.emplace_back(std::move(*x));
    } else {
      err::println("invalid config for node '", kvp.first,
                   "': ", to_string(x.error()));
      return EXIT_FAILURE;
    }
  }
  // Fix settings when running only a partial setup.
  if (!excluded_nodes.empty()) {
    if (nodes.empty()) {
      err::println("no nodes left after applying node filter");
      return EXIT_FAILURE;
    }
    auto plus = [&](size_t n, const inputs_by_node_map::value_type& kvp) {
      if (is_excluded(kvp.first))
        return n;
      return n + kvp.second;
    };
    for (auto& n : nodes) {
      if (n.num_inputs > 0 && n.inputs_by_node.empty()) {
        err::println("cannot run partial setup without inputs-by-node fields");
        return EXIT_FAILURE;
      }
      auto new_total = std::accumulate(n.inputs_by_node.begin(),
                                       n.inputs_by_node.end(), size_t{0}, plus);
      n.num_inputs = new_total;
      for (auto i = n.peers.begin(); i != n.peers.end();) {
        if (is_excluded(*i))
          i = n.peers.erase(i);
        else
          ++i;
      }
    }
  }
  // Sanity check: we need to have at least two nodes.
  if (nodes.size() < 2) {
    err::println("at least two nodes required");
    return EXIT_FAILURE;
  }
  if (nodes.size() >= max_nodes) {
    err::println("must configure less than ", max_nodes, " nodes");
    return EXIT_FAILURE;
  }
  // Build the node tree.
  if (!build_node_tree(nodes) || !verify_node_tree(nodes))
    return EXIT_FAILURE;
  // Print the node setup in verbose mode.
  if (verbose::enabled()) {
    std::vector<const node*> root_nodes;
    for (const auto& x : nodes)
      if (x.left.empty())
        root_nodes.emplace_back(&x);
    if (root_nodes.empty()) {
      verbose::println("note: topology does not form a tree");
    } else {
      verbose::println("peering tree (multiple roots are allowed):");
      std::set<std::string> tmp;
      for (const auto x : root_nodes)
        print_peering_node("", *x, true, tmp);
      verbose::println();
    }
  }
  // Get rollin'.
  caf::actor_system sys{cfg};
  for (auto& x : nodes)
    launch(sys, x);
  caf::scoped_actor self{sys};
  auto wait_for_ack_messages = [&](size_t num) {
    size_t i = 0;
    self->receive_for(i, num)(
      [](atom::ack) {
        // All is well.
      },
      [&](caf::error& err) { throw std::move(err); });
  };
  auto wait_for_ok_messages = [&](size_t num) {
    size_t i = 0;
    self->receive_for(i, num)(
      [](atom::ok) {
        // All is well.
      },
      [](atom::ok, atom::write, const std::string& node_name,
         caf::timespan runtime) {
        out::println(node_name, " (sending): ",
                     duration_cast<fractional_seconds>(runtime));
      },
      [](atom::ok, atom::read, const std::string& node_name,
         caf::timespan runtime) {
        out::println(node_name, " (receiving): ",
                     duration_cast<fractional_seconds>(runtime));
      },
      [&](caf::error& err) { throw std::move(err); });
  };
  try {
    // Initialize all nodes.
    for (auto& x : nodes)
      self->send(x.mgr, atom::init_v);
    wait_for_ok_messages(nodes.size());
    verbose::println("all nodes are up and running, run benchmark");
    // First, we spin up all readers to make sure they receive published data.
    size_t receiver_acks = 0;
    for (auto& x : nodes)
      if (is_receiver(x)) {
        self->send(x.mgr, atom::read_v, self);
        ++receiver_acks;
      }
    wait_for_ack_messages(receiver_acks);
    // Start actual benchmark by spinning up all senders.
    auto t0 = std::chrono::steady_clock::now();
    for (auto& x : nodes)
      if (is_sender(x))
        self->send(x.mgr, atom::write_v, self);
    auto ok_count = [](size_t interim, const node& x) {
      return interim + (is_sender_and_receiver(x) ? 2 : 1);
    };
    wait_for_ok_messages(
      std::accumulate(nodes.begin(), nodes.end(), size_t{0}, ok_count));
    auto t1 = std::chrono::steady_clock::now();
    out::println("system: ", duration_cast<fractional_seconds>(t1 - t0));
    // Shutdown all endpoints.
    verbose::println("shut down all nodes");
    for (auto& x : nodes)
      self->send(x.mgr, atom::shutdown_v);
    wait_for_ok_messages(nodes.size());
    for (auto& x : nodes)
      self->send_exit(x.mgr, caf::exit_reason::user_shutdown);
    for (auto& x : nodes) {
      self->wait_for(x.mgr);
      x.mgr = nullptr;
    }
    verbose::println("all nodes done, bye 👋");
    return EXIT_SUCCESS;
  } catch (caf::error err) {
    err::println("fatal error: ", to_string(err));
    abort();
  }
}
