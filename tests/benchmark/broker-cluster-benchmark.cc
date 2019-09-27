#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include <unistd.h>

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

using caf::expected;
using caf::get;
using caf::get_if;
using caf::holds_alternative;
using std::string;
using std::chrono::duration_cast;

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

bool exists(const char* filename) {
  return access(filename, F_OK) != -1;
}

bool exists(const string& filename) {
  return exists(filename.c_str());
}

} // namespace

// -- configuration setup ------------------------------------------------------

namespace {

struct config : caf::actor_system_config {
  config() {
    opt_group{custom_options_, "global"}
      .add<std::string>("cluster-config-file,c",
                        "path to the cluster configuration file")
      .add<bool>("dump-stats", "prints stats for all given generator files")
      .add<bool>("verbose,v", "enable verbose output")
      .add<bool>("generate-config",
                 "creates a config file from given recording directories");
    set("scheduler.max-threads", 1);
    broker::configuration::add_message_types(*this);
  }

  string usage() {
    return custom_options_.help_text(true);
  }
};

} // namespace

// -- data structures for the cluster setup ------------------------------------

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
  SET_FIELD(num_outputs, optional);
  if (!result.generator_file.empty() && !exists(result.generator_file))
    return make_error(caf::sec::invalid_argument, result.name,
                      "generator file does not exist", result.generator_file);
  return result;
}

struct node_manager_state {
  node* this_node = nullptr;
  broker::endpoint ep;
  broker::detail::generator_file_reader_ptr generator;

  node_manager_state() : ep(broker::configuration{0, nullptr}) {
    // nop
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
  auto t0 = std::chrono::system_clock::now();
  auto g = self->spawn(generator, this_node, self->state.ep.core(),
                       std::move(self->state.generator));
  g->attach_functor([this_node, t0, observer]() mutable {
    auto t1 = std::chrono::system_clock::now();
    anon_send(observer, broker::atom::ok::value, this_node->name,
              duration_cast<caf::timespan>(t1 - t0));
  });
}

struct consumer_state {
  consumer_state(caf::event_based_actor* self) : self(self) {
    // nop
  }

  void handle_messages(size_t n) {
    // Make some noise every 1k messages.
    if (received / 1000 != (received + n) / 1000)
      verbose::println(this_node->name, " got ", received + n, " messages");
    received += n;
    // Stop when receiving the node's limit.
    if (received == this_node->num_inputs) {
      verbose::println(this_node->name, " reached its limit: quit");
      self->quit();
    }
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
        verbose::println("done receiving ",
                         types.portable_name(caf::make_rtti_pair<T>()));
      });
  }

  size_t received = 0;
  node* this_node;
  caf::event_based_actor* self;
  size_t connected_streams = 0;
  static const char* name;
};

const char* consumer_state::name = "consumer";

caf::behavior consumer(caf::stateful_actor<consumer_state>* self,
                       node* this_node, caf::actor core, caf::actor observer) {
  self->state.this_node = this_node;
  self->send(self * core, broker::atom::join::value, topics(*this_node));
  self->send(self * core, broker::atom::join::value, broker::atom::store::value,
             topics(*this_node));
  return {
    [=](caf::stream<broker::data_message> in) {
      self->state.attach_sink(in, observer);
    },
    [=](caf::stream<broker::command_message> in) {
      self->state.attach_sink(in, observer);
    },
  };
}

void run_receive_mode(node_manager_actor* self, caf::actor observer) {
  auto t0 = std::chrono::system_clock::now();
  auto this_node = self->state.this_node;
  auto c = self->spawn(consumer, this_node, self->state.ep.core(), observer);
  c->attach_functor([this_node, t0, observer]() mutable {
    auto t1 = std::chrono::system_clock::now();
    anon_send(observer, broker::atom::ok::value, this_node->name,
              duration_cast<caf::timespan>(t1 - t0));
  });
}

caf::behavior node_manager(node_manager_actor* self, node* this_node) {
  self->state.this_node = this_node;
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
      for (const auto* peer : this_node->right) {
        verbose::println(this_node->name, " starts peering to ",
                         peer->id.authority(), " (", peer->name, ")");
        st.ep.peer(to_string(peer->id.authority().host),
                   peer->id.authority().port);
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
    [=](broker::atom::run, caf::actor observer) {
      if (is_sender(*this_node))
        run_send_mode(self, observer);
      else
        run_receive_mode(self, observer);
    },
    [=](broker::atom::shutdown) -> caf::result<caf::atom_value> {
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

int generate_config(std::vector<std::string> directories) {
  using broker::detail::is_directory;
  using broker::detail::is_file;
  // Remove trailing slashes to make working with the directories easier.
  for (auto& directory : directories) {
    while (caf::ends_with(directory, "/"))
      directory.pop_back();
    if (!is_directory(directory)) {
      err::println('\"', directory, "\" is not a directory");
      return EXIT_FAILURE;
    }
  }
  // Use the directory name as node name and read directory contents.
  std::map<std::string, node> nodes;
  for (const auto& directory : directories) {
    std::string node_name;
    auto sep = directory.find_last_of('/');
    if (sep != std::string::npos)
      node_name = directory.substr(sep + 1);
    else
      node_name = directory;
    if (nodes.count(node_name) > 0) {
      err::println("node name \"", node_name, "\" appears twice");
      return EXIT_FAILURE;
    }
    auto& node = nodes.emplace(node_name, ::node{}).first->second;
    auto generator_file = directory + "/messages.dat";
    if (is_file(generator_file))
      node.generator_file = std::move(generator_file);
    // Read and de-duplicate topics.
    std::string topic;
    std::ifstream topics_file{directory + "/topics.txt"};
    while (std::getline(topics_file, topic))
      if (!topic.empty())
        node.topics.emplace_back(topic);
    std::sort(node.topics.begin(), node.topics.end());
    auto e = std::unique(node.topics.begin(), node.topics.end());
    if (e != node.topics.end())
      node.topics.erase(e, node.topics.end());
  }
  // Print generated config and return.
  out::println("nodes {");
  for (const auto& kvp : nodes) {
    auto& node = kvp.second;
    out::println("  ", kvp.first, " {");
    if (!node.topics.empty()) {
      out::println("    topics = [");
      for (const auto& topic : node.topics)
        out::println("      ", quoted{topic}, ",");
      out::println("    ]");
    }
    if (!node.generator_file.empty())
      out::println("    generator-file = ", quoted{node.generator_file});
    out::println("  }");
  }
  out::println("}");
  return EXIT_SUCCESS;
}

// -- main ---------------------------------------------------------------------

void print_peering_node(const std::string& prefix, const node& x,
                        bool is_last) {
  std::string next_prefix;
  if (x.left.empty()) {
    verbose::println(prefix, x.name, ", topics: ", x.topics);
  } else {
    verbose::println(prefix, is_last ? "└── " : "├── ", x.name,
                     ", topics: ", x.topics);
    next_prefix = prefix + (is_last ? "    " : "│   ");
  }
  for (size_t i = 0; i < x.right.size(); ++i)
    print_peering_node(next_prefix, *x.right[i], i == x.right.size() - 1);
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
      } else {
        if (auto err = gptr->skip_to_end()) {
          err::println("error while parsing generator file: ", cfg.render(err));
          continue;
        }
        const auto& topics = gptr->topics();
        out::println(file_name);
        out::println("├── entries: ", gptr->entries());
        out::println("|   ├── data-entries: ", gptr->data_entries());
        out::println("|   └── command-entries: ", gptr->command_entries());
        out::println("└── topics:");
        if (!topics.empty()) {
          for (size_t i = 0; i < topics.size() - 1; ++i)
            out::println("    ├── ", topics[i].string());
          out::println("    └── ", topics.back().string());
        }
      }
    }
    return EXIT_SUCCESS;
  }
  // Generate nodes from cluster config.
  std::vector<node> nodes;
  for (auto& kvp : cluster_config["nodes"].as_dictionary()) {
    if (auto x = make_node(kvp.first, kvp.second.as_dictionary())) {
      nodes.emplace_back(std::move(*x));
    } else {
      err::println("invalid config for node '", kvp.first,
                   "': ", cfg.render(x.error()));
      return EXIT_FAILURE;
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
  auto node_by_name = [&](const string& name) -> node* {
    auto predicate = [&](const node& x) { return x.name == name; };
    auto i = std::find_if(nodes.begin(), nodes.end(), predicate);
    if (i == nodes.end()) {
      err::println("invalid node name: ", name);
      exit(EXIT_FAILURE);
    }
    return &(*i);
  };
  for (auto& x : nodes) {
    for (auto& peer_name : x.peers) {
      auto peer = node_by_name(peer_name);
      if (&x == peer) {
        err::println(x.name, " cannot peer with itself");
        return EXIT_FAILURE;
      }
      x.right.emplace_back(peer);
      peer->left.emplace_back(&x);
    }
  }
  // Sanity check: each node must be part of the multi-root tree.
  for (auto& x : nodes) {
    if (x.left.empty() && x.right.empty()) {
      err::println(x.name, " has no peers");
      return EXIT_FAILURE;
    }
  }
  // Sanity check: there must be no loop.
  auto max_depth = nodes.size() - 1;
  for (auto& x : nodes) {
    if (max_left_depth(x) > max_depth || max_right_depth(x) > max_depth) {
      err::println("starting at node '", x.name, "' results in a loop");
      return EXIT_FAILURE;
    }
  }
  // Sanity check: there must at least one receiver.
  if (std::none_of(nodes.begin(), nodes.end(), is_receiver)) {
    err::println("no node expects to receive any data");
    return EXIT_FAILURE;
  }
  // Sanity check: there must at least one sender.
  if (std::none_of(nodes.begin(), nodes.end(), is_sender)) {
    err::println("no node has a generator file for publishing data");
    return EXIT_FAILURE;
  }
  // Sanity check: nodes can't play sender and receiver at the same time (yet).
  { // Lifetime scope of i.
    auto i = std::find_if(nodes.begin(), nodes.end(), is_sender_and_receiver);
    if (i != nodes.end()) {
      err::println(i->name,
                   " is configured to send at receive at the same time");
      return EXIT_FAILURE;
    }
  }
  // Print the node setup in verbose mode.
  if (verbose::enabled()) {
    verbose::println("Peering tree (multiple roots are allowed):");
    std::vector<const node*> root_nodes;
    for (const auto& x : nodes)
      if (x.left.empty())
        root_nodes.emplace_back(&x);
    for (const auto x : root_nodes)
      print_peering_node("", *x, true);
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
      [&](const caf::error& err) {
        err::println("eror while waiting for ACK messages: ", sys.render(err));
      });
  };
  auto wait_for_ok_messages = [&]() {
    size_t i = 0;
    self->receive_for(i, nodes.size())(
      [](broker::atom::ok) {
        // All is well.
      },
      [](broker::atom::ok, const std::string& node_name,
         caf::timespan runtime) {
        out::println(node_name, ": ",
                     duration_cast<fractional_seconds>(runtime));
      },
      [&](const caf::error& err) {
        err::println("eror while waiting for OK messages: ", sys.render(err));
      });
  };
  // Initialize all nodes.
  for (auto& x : nodes)
    self->send(x.mgr, broker::atom::init::value);
  wait_for_ok_messages();
  verbose::println("all nodes are up and running, run benchmark");
  // First, we spin up all readers to make sure they receive published data.
  size_t receiver_acks = 0;
  for (auto& x : nodes)
    if (is_receiver(x)) {
      self->send(x.mgr, broker::atom::run::value, self);
      ++receiver_acks;
    }
  wait_for_ack_messages(receiver_acks);
  // Start actual benchmark by spinning up all senders.
  auto t0 = std::chrono::system_clock::now();
  for (auto& x : nodes)
    if (is_sender(x))
      self->send(x.mgr, broker::atom::run::value, self);
  wait_for_ok_messages();
  auto t1 = std::chrono::system_clock::now();
  out::println("system: ", duration_cast<fractional_seconds>(t1 - t0));
  // Shutdown all endpoints.
  verbose::println("shut down all nodes");
  for (auto& x : nodes)
    self->send(x.mgr, broker::atom::shutdown::value);
  wait_for_ok_messages();
  for (auto& x : nodes)
    self->send_exit(x.mgr, caf::exit_reason::user_shutdown);
  for (auto& x : nodes) {
    self->wait_for(x.mgr);
    x.mgr = nullptr;
  }
  verbose::println("all nodes done, bye 👋");
}
