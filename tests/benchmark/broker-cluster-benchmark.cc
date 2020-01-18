#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "caf/actor_system.hpp"
#include "caf/actor_system_config.hpp"
#include "caf/event_based_actor.hpp"
#include "caf/settings.hpp"
#include "caf/stateful_actor.hpp"
#include "caf/string_algorithms.hpp"
#include "caf/term.hpp"

#include "broker/atoms.hh"
#include "broker/detail/filesystem.hh"
#include "broker/detail/generator_file_reader.hh"
#include "broker/endpoint.hh"
#include "broker/subscriber.hh"

using caf::actor_system_config;
using caf::expected;
using caf::get;
using caf::get_if;
using caf::holds_alternative;
using std::chrono::duration_cast;
using std::string;

using string_list = std::vector<string>;

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

std::string trim(std::string x){
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
      .add<bool>("dump-stats", "prints stats for all given generator files")
      .add<bool>("verbose,v", "enable verbose output")
      .add<bool>("generate-config",
                 "creates a config file from given recording directories")
      .add<string_list>("excluded-nodes,e",
                        "excludes given nodes from the setup");
    set("scheduler.max-threads", 1);
    set("logger.file-verbosity", caf::atom("quiet"));
    broker::configuration::add_message_types(*this);
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
  std::vector<std::string> peers;

  /// Stores the topics we subscribe to at startup.
  std::vector<std::string> topics;

  /// Optionally stores a path to a generator file.
  std::string generator_file;

  /// Stores how many messages we expect on this node during measurement.
  size_t num_inputs = 0;

  /// Stores whether this node regularly forwards Broker events.
  bool forward = true;

  /// Stores how many messages we produce using the gernerator file. If `none`,
  // we produce the number of messages in the generator file.
  caf::optional<size_t> num_outputs;

  /// Stores parent nodes in the pub/sub topology.
  std::vector<node*> left;

  /// Stores child nodes in the pub/sub topology. These nodes are our peers we
  // connect to at startup.
  std::vector<node*> right;

  /// Points to an actor that manages the Broker endpoint.
  caf::actor mgr;

  /// Stores how many inputs we receive per node.
  inputs_by_node_map inputs_by_node;

  /// Stores the CAF log level for this node.
  caf::atom_value log_verbosity = caf::atom("quiet");
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

size_t max_left_depth(const node& x, size_t interim = 0) {
  if (interim > max_nodes)
    return interim;
  size_t result = interim;
  for (const auto y : x.left)
    result = std::max(result, max_left_depth(*y, interim + 1));
  return result;
}

size_t max_right_depth(const node& x, size_t interim = 0) {
  if (interim > max_nodes)
    return interim;
  size_t result = interim;
  for (const auto y : x.right)
    result = std::max(result, max_right_depth(*y, interim + 1));
  return result;
}

template <class T>
struct strip_optional {
  using type = T;
};

template <class T>
struct strip_optional<caf::optional<T>> {
  using type = T;
};

#define SET_FIELD(field, qualifier)                                            \
  {                                                                            \
    std::string field_name = #field;                                           \
    caf::replace_all(field_name, "_", "-");                                    \
    using field_type = typename strip_optional<decltype(result.field)>::type;  \
    if (auto value = get_if<field_type>(&parameters, field_name))              \
      result.field = std::move(*value);                                        \
    else if (auto type_erased_value = get_if(&parameters, field_name))         \
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
  SET_FIELD(forward, optional);
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
  broker::detail::generator_file_reader_ptr generator;
  std::vector<caf::actor> children;

  node_manager_state() {
    // nop
  }

  ~node_manager_state() {
    ep.~endpoint();
  }

  void init(node* this_node_ptr) {
    BROKER_ASSERT(this_node_ptr != nullptr);
    this_node = this_node_ptr;
    broker::broker_options opts;
    opts.forward = this_node_ptr->forward;
    opts.disable_ssl = true;
    opts.ignore_broker_conf = true; // Make no one messes with our setup.
    broker::configuration cfg{opts};
    cfg.set("middleman.workers", 0);
    cfg.set("logger.file-name", this_node->name + ".log");
    cfg.set("logger.file-verbosity", this_node->log_verbosity);
    new (&ep) broker::endpoint(std::move(cfg));
  }
};

using node_manager_actor = caf::stateful_actor<node_manager_state>;

struct generator_state {
  static const char* name;
};

const char* generator_state::name = "generator";

void generator(caf::stateful_actor<generator_state>* self, node* this_node,
               caf::actor core, broker::detail::generator_file_reader_ptr ptr) {
  using generator_ptr = broker::detail::generator_file_reader_ptr;
  using value_type = broker::node_message::value_type;
  if (this_node->num_outputs != caf::none) {
    struct state {
      generator_ptr gptr;
      size_t remaining;
      size_t pushed = 0;
    };
    self->make_source(
      core,
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
                         ": ", self->system().render(err));
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
    self->make_source(
      core,
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
                         ": ", self->system().render(err));
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
  auto g = self->spawn(generator, this_node, self->state.ep.core(),
                       std::move(self->state.generator));
  g->attach_functor([this_node, t0, observer]() mutable {
    auto t1 = std::chrono::steady_clock::now();
    anon_send(observer, broker::atom::ok::value, broker::atom::write::value,
              this_node->name, duration_cast<caf::timespan>(t1 - t0));
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
      anon_send(observer, broker::atom::ok::value, broker::atom::read::value,
                this_node->name, duration_cast<caf::timespan>(stop - start));
      verbose::println(this_node->name, " reached its limit");
    }
    received += n;
  }

  template <class T>
  void attach_sink(caf::stream<T> in, caf::actor observer) {
    if (++connected_streams == 2) {
      self->send(observer, broker::atom::ack::value);
      verbose::println(this_node->name, " waits for messages");
    }
    self->make_sink(
      in,
      [](caf::unit_t&) {
        // nop
      },
      [=](caf::unit_t&, std::vector<T>& xs) { handle_messages(xs.size()); },
      [=](caf::unit_t&, const caf::error& err) {
        auto& types = self->system().types();
        verbose::println(this_node->name, " stops receiving ",
                         types.portable_name(caf::make_rtti_pair<T>()));
      });
  }

  size_t received = 0;
  node* this_node;
  caf::event_based_actor* self;
  size_t connected_streams = 0;
  static const char* name;
  std::chrono::steady_clock::time_point start;
  caf::actor observer;
};

const char* consumer_state::name = "consumer";

caf::behavior consumer(caf::stateful_actor<consumer_state>* self,
                       node* this_node, caf::actor core, caf::actor observer) {
  self->state.this_node = this_node;
  self->state.observer = observer;
  self->send(self * core, broker::atom::join::value, topics(*this_node));
  self->send(self * core, broker::atom::join::value, broker::atom::store::value,
             topics(*this_node));
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
          verbose::println(
            this_node->name,
            " received nothing for 1s, last count: ", self->state.received);
          last_printed_count = self->state.received;
        }
      },
  };
}

void run_receive_mode(node_manager_actor* self, caf::actor observer) {
  auto this_node = self->state.this_node;
  auto core = self->state.ep.core();
  auto c = self->spawn(consumer, this_node, core, observer);
  self->state.children.emplace_back(c);
}

caf::error try_connect(broker::endpoint& ep, broker::status_subscriber& ss,
                       const node* this_node, const node* peer) {
  const auto& authority = peer->id.authority();
  auto host = to_string(authority.host);
  ep.peer(host, authority.port, broker::timeout::seconds(1));
  for (;;) {
    auto ss_res = ss.get();
    using namespace broker;
    if (holds_alternative<none>(ss_res))
      continue;
    if (auto err = get_if<error>(&ss_res))
      return std::move(*err);
    BROKER_ASSERT(holds_alternative<status>(ss_res));
    auto& ss_stat = get<status>(ss_res);
    auto code = ss_stat.code();
    if (code == sc::unspecified)
      continue;
    if (code == sc::peer_removed || code == sc::peer_lost)
      return make_error(caf::sec::runtime_error, this_node->name,
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
  if (is_receiver(*this_node) || this_node->forward)
    self->state.ep.forward(topics(*this_node));
  return {
    [=](broker::atom::init) -> caf::result<caf::atom_value> {
      // Open up the ports and start peering.
      auto& st = self->state;
      if (this_node->id.scheme() == "tcp") {
        auto& authority = this_node->id.authority();
        verbose::println(this_node->name, " starts listening at ", authority);
        auto port = st.ep.listen(to_string(authority.host), authority.port);
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
        using broker::detail::make_generator_file_reader;
        st.generator = make_generator_file_reader(this_node->generator_file);
        if (st.generator == nullptr)
          return make_error(caf::sec::cannot_open_file,
                            this_node->generator_file);
      }
      verbose::println(this_node->name, " up and running");
      return broker::atom::ok::value;
    },
    [=](broker::atom::read, caf::actor observer) {
      run_receive_mode(self, observer);
    },
    [=](broker::atom::write, caf::actor observer) {
      run_send_mode(self, observer);
    },
    [=](broker::atom::shutdown) -> caf::result<caf::atom_value> {
      for (auto& child : self->state.children)
        self->send_exit(child, caf::exit_reason::user_shutdown);
      // Tell broker to shutdown. This is a blocking function call.
      self->state.ep.shutdown();
      verbose::println(this_node->name, " down");
      return broker::atom::ok::value;
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
      err::println(x.name, " has no peers");
      return false;
    }
  }
  // Sanity check: there must be no loop.
  auto max_depth = nodes.size() - 1;
  for (auto& x : nodes) {
    if (max_left_depth(x) > max_depth || max_right_depth(x) > max_depth) {
      err::println("starting at node '", x.name, "' results in a loop");
      return false;
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

int generate_config(std::vector<std::string> directories) {
  constexpr const char* required_files[] = {
    "/id.txt",
    "/messages.dat",
    "/peers.txt",
    "/topics.txt",
    "/broker.conf",
  };
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
  for (const auto& directory : directories) {
    // Sanity check.
    for (auto fname : required_files) {
      auto fpath = directory + fname;
      if (!is_file(fpath)) {
        err::println("missing file: ", fpath);
        return EXIT_FAILURE;
      }
    }
    // Extract the node name and check uniqueness.
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
    // Get the node_id for this node and make sure its unique.
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
    node.generator_file = directory + "/messages.dat";
    node.peers = readlines(directory + "/peers.txt", false);
    // Read and de-duplicate topics.
    node.topics = readlines(directory + "/topics.txt", false);
    std::sort(node.topics.begin(), node.topics.end());
    auto e = std::unique(node.topics.begin(), node.topics.end());
    if (e != node.topics.end())
      node.topics.erase(e, node.topics.end());
    // Fetch crucial config parameters.
    auto conf_file = directory + "/broker.conf";
    if (auto conf = actor_system_config::parse_config_file(conf_file.c_str())) {
      node.forward = caf::get_or(*conf, "broker.forward", true);
    } else {
      err::println("unable to parse ", quoted{conf_file}, ": ",
                   actor_system_config::render(conf.error()));
      return EXIT_FAILURE;
    }
  }
  // We are done with our first pass. Now, we resolve all the peer handles.
  for (auto& node : nodes) {
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
      peer = peer_name;
    }
  }
  // Check whether our nodes connect to a valid tree.
  if (!build_node_tree(nodes))
    return EXIT_FAILURE;
  // Compute for each node how many messages it produces per topic.
  verbose::println("read generator files and compute outputs per node",
                   " (may take a while)");
  using output_map = std::map<std::string, size_t>;
  std::map<std::string, output_map> outputs;
  for (const auto& node : nodes) {
    auto gptr = broker::detail::make_generator_file_reader(node.generator_file);
    if (gptr == nullptr) {
      err::println("unable to open generator file: ", node.generator_file);
      return EXIT_FAILURE;
    }
    auto& out = outputs[node.name];
    broker::detail::generator_file_reader::value_type value;
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
  using traverse_t
    = void(node&, node&, const output_map&, const filter&, walk_fun);
  std::function<traverse_t> traverse;
  traverse = [&](node& src, node& dst, const output_map& out, const filter& f,
                 walk_fun walk) {
    step(src, dst, out, f);
    if (!dst.forward)
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
      uri_str += "tcp://[::1]:";
      uri_str += std::to_string(port++);
    }
    if (auto err = caf::parse(uri_str, node.id)) {
      err::println("generated invalid URI ID: \"", uri_str, "\"");
      return EXIT_FAILURE;
    }
  }
  // Print generated config and return.
  verbose::println("done 🎉");
  auto print_field = [&](const char* name, const std::vector<std::string>& xs) {
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
    out::println("    forward = ", node.forward);
    print_field("topics", node.topics);
    print_field("peers", node.peers);
    if (!node.generator_file.empty() && !outputs[node.name].empty())
      out::println("    generator-file = ", quoted{node.generator_file});
    out::println("  }");
  }
  out::println("}");
  return EXIT_SUCCESS;
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
  if (x.right.empty()) {
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
  if (x.left.empty()) {
    verbose::println(next_prefix, "└── (none)");
    return;
  }
  for (size_t i = 0; i < x.left.size(); ++i)
    print_peering_node(next_prefix, *x.left[i], i == x.left.size() - 1,
                       printed_nodes);
}

int main(int argc, char** argv) {
  // Read CAF configuration.
  config cfg;
  if (auto err = cfg.parse(argc, argv)) {
    err::println("unable to parse CAF config: ", cfg.render(err));
    return EXIT_FAILURE;
  }
  // Exit for `--help` etc.
  if (cfg.cli_helptext_printed)
    return EXIT_SUCCESS;
  // Enable global flags.
  if (get_or(cfg, "verbose", false))
    verbose::is_enabled = true;
  // Generate config file when demanded.
  if (get_or(cfg, "generate-config", false))
    return generate_config(cfg.remainder);
  // Read cluster config.
  auto excluded_nodes = get_or(cfg, "excluded-nodes", string_list{});
  auto is_excluded = [&](const string& node_name) {
    auto e = excluded_nodes.end();
    return std::find(excluded_nodes.begin(), e, node_name) != e;
  };
  caf::settings cluster_config;
  if (auto path = get_if<string>(&cfg, "cluster-config-file")) {
    if (auto file_content = config::parse_config_file(path->c_str())) {
      cluster_config = std::move(*file_content);
    } else {
      err::println("unable to parse cluster config file: ",
                   cfg.render(file_content.error()));
      return EXIT_FAILURE;
    }
  } else {
    err::println("cluster-config-file missing");
    out::println();
    out::println(cfg.usage());
    return EXIT_FAILURE;
  }
  // Check for dump-stats mode.
  if (get_or(cfg, "dump-stats", false)) {
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
      auto gptr = broker::detail::make_generator_file_reader(file_name);
      if (gptr == nullptr) {
        err::println("unable to open generator file: ", file_name);
        continue;
      }
      size_t total_entries = 0;
      size_t data_entries = 0;
      size_t command_entries = 0;
      std::map<broker::topic, size_t> entries_by_topic;
      broker::node_message::value_type x;
      while (!gptr->at_end()) {
        if (auto err = gptr->read(x)) {
          err::println("error while parsing ", file_name, ": ",
                       cfg.render(err));
          return EXIT_FAILURE;
        }
        ++total_entries;
        if (is_data_message(x))
          ++data_entries;
        else
          ++ command_entries;
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
                   "': ", cfg.render(x.error()));
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
      n.peers.erase(std::remove_if(n.peers.begin(), n.peers.end(), is_excluded),
                    n.peers.end());
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
    verbose::println("Peering tree (multiple roots are allowed):");
    std::vector<const node*> root_nodes;
    for (const auto& x : nodes)
      if (x.right.empty())
        root_nodes.emplace_back(&x);
    std::set<std::string> tmp;
    for (const auto x : root_nodes)
      print_peering_node("", *x, true, tmp);
    verbose::println();
  }
  // Get rollin'.
  caf::actor_system sys{cfg};
  for (auto& x : nodes)
    launch(sys, x);
  caf::scoped_actor self{sys};
  auto wait_for_ack_messages = [&](size_t num) {
    size_t i = 0;
    self->receive_for(i, num)(
      [](broker::atom::ack) {
        // All is well.
      },
      [&](caf::error& err) {
        throw std::move(err);
      });
  };
  auto wait_for_ok_messages = [&](size_t num) {
    size_t i = 0;
    self->receive_for(i, num)(
      [](broker::atom::ok) {
        // All is well.
      },
      [](broker::atom::ok, broker::atom::write, const std::string& node_name,
         caf::timespan runtime) {
        out::println(node_name, " (sending): ",
                     duration_cast<fractional_seconds>(runtime));
      },
      [](broker::atom::ok, broker::atom::read, const std::string& node_name,
         caf::timespan runtime) {
        out::println(node_name, " (receiving): ",
                     duration_cast<fractional_seconds>(runtime));
      },
      [&](caf::error& err) {
        throw std::move(err);
      });
  };
  try {
    // Initialize all nodes.
    for (auto& x : nodes)
      self->send(x.mgr, broker::atom::init::value);
    wait_for_ok_messages(nodes.size());
    verbose::println("all nodes are up and running, run benchmark");
    // First, we spin up all readers to make sure they receive published data.
    size_t receiver_acks = 0;
    for (auto& x : nodes)
      if (is_receiver(x)) {
        self->send(x.mgr, broker::atom::read::value, self);
        ++receiver_acks;
      }
    wait_for_ack_messages(receiver_acks);
    // Start actual benchmark by spinning up all senders.
    auto t0 = std::chrono::steady_clock::now();
    for (auto& x : nodes)
      if (is_sender(x))
        self->send(x.mgr, broker::atom::write::value, self);
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
      self->send(x.mgr, broker::atom::shutdown::value);
    wait_for_ok_messages(nodes.size());
    for (auto& x : nodes)
      self->send_exit(x.mgr, caf::exit_reason::user_shutdown);
    for (auto& x : nodes) {
      self->wait_for(x.mgr);
      x.mgr = nullptr;
    }
    verbose::println("all nodes done, bye 👋");
  } catch (caf::error err) {
    err::println("fatal eror: ", sys.render(err));
    for (auto& x : nodes)
      self->send_exit(x.mgr, caf::exit_reason::user_shutdown);
    for (auto& x : nodes) {
      self->wait_for(x.mgr);
      x.mgr = nullptr;
    }
    return EXIT_FAILURE;
  }
}
