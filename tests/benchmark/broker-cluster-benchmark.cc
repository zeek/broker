#include <atomic>
#include <iostream>
#include <string>

#include "caf/actor_system_config.hpp"
#include "caf/settings.hpp"
#include "caf/term.hpp"

#include "broker/broker.hh"

using caf::expected;
using caf::get_if;
using std::string;

// -- global constants ---------------------------------------------------------

namespace {

constexpr size_t max_nodes = 500;

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

// -- configuration setup ------------------------------------------------------

namespace {

struct config : caf::actor_system_config {
  config() {
    opt_group opts{custom_options_, "global"};
    opts.add<std::string>("cluster-config-file,c",
                          "path to the cluster configuration file");
    opts.add<bool>("verbose,v", "enable verbose output");
  }

  string usage() {
    return custom_options_.help_text(true);
  }
};

} // namespace

// -- main ---------------------------------------------------------------------

// A node in the Broker publish/subscribe layer.
struct node {
  std::string name;
  caf::uri id;
  std::vector<std::string> peers;
  std::vector<std::string> topics;
  std::vector<node*> left;
  std::vector<node*> right;
};

#define SET_FIELD(field, qualifier)                                            \
  if (auto value = get_if<decltype(result.field)>(&parameters, #field))        \
    result.field = std::move(*value);                                          \
  else if (auto type_erased_value = get_if(&parameters, #field))               \
    return make_error(caf::sec::invalid_argument,                              \
                      "illegal type for " + name + "." #field);                \
  else if (strcmp(#qualifier, "mandatory") == 0)                               \
    return make_error(caf::sec::invalid_argument,                              \
                      "no entry for mandatory field " #field);

expected<node> make_node(const string& name, const caf::settings& parameters) {
  node result;
  result.name = name;
  SET_FIELD(id, mandatory);
  SET_FIELD(peers, optional);
  SET_FIELD(topics, mandatory);
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
  if (get_or(cfg, "verbose", false))
    verbose::is_enabled = true;
  // Read cluster config.
  caf::settings cluster_config;
  if (auto path = get_if<string>(&cfg, "cluster-config-file")) {
    if (auto file_content = config::parse_config_file(*path)) {
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
    if (x.left.empty() && x.right.empty()){
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
  return EXIT_SUCCESS;
}
