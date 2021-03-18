#include "broker/alm/multipath.hh"
#include "broker/alm/routing_table.hh"
#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/endpoint.hh"

#include <benchmark/benchmark.h>

#include <atomic>
#include <limits>
#include <random>

// For the benchmarking, we use a simple tree structure topology:
//
//                           .  .  .  ..  .  .   .
//                            \ /   \ / \ /   \ /
// [n = 2]                     O --- O   O --- O
//                              \   /     \   /
//                               \ /       \ /
// [n = 1]                        O ------- O
//                                 \       /
//                                  \     /
//                                   \   /
//                                    \ /
// [this node]                         O
//
// This results in a "network" with (0 > n > 11):
// - nodes: f(1) = 2, f(n) = f(n-1) + 2^n (2, 6, 14, ...)
// - connections: f(1) = 3, f(n) = f(n-1) + 2^(n-1)*3 (3, 9, 21, ...)

namespace {

using broker::endpoint_id;

using table_type = broker::alm::routing_table;

using path_type = std::vector<endpoint_id>;

struct id_generator {
  using array_type = caf::hashed_node_id::host_id_type;

  id_generator() : rng(0xB7E57) {
    // nop
  }

  endpoint_id next() {
    using value_type = array_type::value_type;
    std::uniform_int_distribution<> d{0,
                                      std::numeric_limits<value_type>::max()};
    array_type result;
    for (auto& x : result)
      x = static_cast<value_type>(d(rng));
    return caf::make_node_id(d(rng), result);
  }

  std::minstd_rand rng;
};

class routing_table : public benchmark::Fixture {
public:
  routing_table() {
    topologies.resize(10);
    //v2_topologies.resize(10);
    ids.resize(10);
    receivers_10p.resize(10);
    id_generator g;
    auto on_new_node = [this](size_t level, const endpoint_id& leaf_id) {
      ids[level].emplace_back(leaf_id);
    };
    for (size_t index = 0; index < 10; ++index)
      fill_tbl(topologies[index], g, {}, 0, index, on_new_node);
    // for (size_t index = 0; index < 10; ++index)
    //   fill_tbl(v2_topologies[index], g, {}, 0, index, [](auto&&...) {});
    // Sanity checking
#ifndef NDEBUG
    for (size_t index = 0; index < 10; ++index)
      assert(ids[index].size() == (1 << (index + 1)));
#endif
    std::vector<endpoint_id> flat_ids;
    for (size_t index = 0; index < 10; ++index) {
      auto& vec = ids[index];
      flat_ids.insert(flat_ids.end(), vec.begin(), vec.end());
      std::shuffle(flat_ids.begin(), flat_ids.end(), g.rng);
      auto p10 = std::max(flat_ids.size() / 10, size_t{1});
      receivers_10p[index].assign(flat_ids.begin(), flat_ids.begin() + p10);
    }
  }

  // Topologies  by level.
  std::vector<table_type> topologies;

  // IDs by level.
  std::vector<std::vector<endpoint_id>> ids;

  // Receivers for the different generate_paths setup (10 percent).
  std::vector<std::vector<endpoint_id>> receivers_10p;

  static auto make_vt(size_t n) {
    broker::alm::vector_timestamp result;
    result.resize(n);
    return result;
  }

  static auto make_vt(const path_type& p) {
    return make_vt(p.size());
  }

  template <class Table, class OnNewNode>
  static void fill_tbl(Table& tbl, id_generator& g, const path_type& p,
                       size_t level, size_t max_level, OnNewNode on_new_node) {
    auto next_path = [](const auto& src, auto id) {
      auto result = src;
      result.emplace_back(id);
      return result;
    };
    auto add_entry = [&](const auto& id, const path_type& new_path) {
      add_or_update_path(tbl, id, new_path, make_vt(new_path));
    };
    // Add first leaf node.
    auto leaf1_id = g.next();
    auto leaf1_path = next_path(p, leaf1_id);
    on_new_node(level, leaf1_id);
    add_entry(leaf1_id, leaf1_path);
    // Add second leaf node.
    auto leaf2_id = g.next();
    auto leaf2_path = next_path(p, leaf2_id);
    on_new_node(level, leaf2_id);
    add_entry(leaf2_id, leaf2_path);
    // Add paths between leaf1 and leaf2.
    add_entry(leaf2_id, next_path(leaf1_path, leaf2_id));
    add_entry(leaf1_id, next_path(leaf2_path, leaf1_id));
    // Enter next level.
    if (level < max_level) {
      fill_tbl(tbl, g, leaf1_path, level + 1, max_level, on_new_node);
      fill_tbl(tbl, g, leaf2_path, level + 1, max_level, on_new_node);
    }
  }
};

} // namespace

// -- adding entries to a routing table ----------------------------------------

BENCHMARK_DEFINE_F(routing_table, add_or_update_path)(benchmark::State& state) {
  auto max_level = static_cast<size_t>(state.range(0));
  for (auto _ : state) {
    id_generator g;
    table_type tbl;
    fill_tbl(tbl, g, {}, 0, max_level, [](auto&&...) {});
    benchmark::ClobberMemory();
    benchmark::DoNotOptimize(tbl);
  }
}

BENCHMARK_REGISTER_F(routing_table, add_or_update_path)->DenseRange(0, 9, 1);

// -- shortest path to a "far away node" ---------------------------------------

BENCHMARK_DEFINE_F(routing_table, shortest_path)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& tbl = topologies[index];
  const auto& id = ids[index].front();
  for (auto _ : state) {
    auto sp = broker::alm::shortest_path(tbl, id);
    benchmark::DoNotOptimize(sp);
    assert(sp != nullptr);
    assert(sp->size() == index + 1);
  }
}

BENCHMARK_REGISTER_F(routing_table, shortest_path)->DenseRange(0, 9, 1);

// -- generate path for a "far away node" --------------------------------------

BENCHMARK_DEFINE_F(routing_table, generate_paths_1)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& tbl = topologies[index];
  const auto& id = ids[index].front();
  std::vector<endpoint_id> receivers{id};
  for (auto _ : state) {
    std::vector<broker::alm::multipath> routes;
    std::vector<endpoint_id> unreachables;
    broker::alm::multipath::generate(receivers, tbl, routes, unreachables);
    benchmark::ClobberMemory();
    benchmark::DoNotOptimize(routes);
    assert(unreachables.empty());
  }
}

BENCHMARK_REGISTER_F(routing_table, generate_paths_1)->DenseRange(0, 9, 1);

// -- generate paths to span 10% of all nodes ----------------------------------

BENCHMARK_DEFINE_F(routing_table, generate_paths_10p)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& tbl = topologies[index];
  const auto& receivers = receivers_10p[index];
  for (auto _ : state) {
    std::vector<broker::alm::multipath> routes;
    std::vector<endpoint_id> unreachables;
    broker::alm::multipath::generate(receivers, tbl, routes, unreachables);
    benchmark::ClobberMemory();
    benchmark::DoNotOptimize(routes);
    assert(unreachables.empty());
  }
}

BENCHMARK_REGISTER_F(routing_table, generate_paths_10p)->DenseRange(0, 9, 1);

// -- erase from the head of the routing table (direct connection) -------------

BENCHMARK_DEFINE_F(routing_table, erase_front)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& id = ids[0].front();
  for (auto _ : state) {
    state.PauseTiming();
    auto cpy = topologies[index];
    state.ResumeTiming();
    broker::alm::erase(cpy, id, [](auto&&...) {});
    benchmark::ClobberMemory();
    benchmark::DoNotOptimize(cpy);
  }
}

BENCHMARK_REGISTER_F(routing_table, erase_front)->DenseRange(0, 9, 1);

// -- erase from the tail of the routing table (a far away node) ---------------

BENCHMARK_DEFINE_F(routing_table, erase_back)(benchmark::State& state) {
  auto index = static_cast<size_t>(state.range(0));
  const auto& id = ids[index].front();
  for (auto _ : state) {
    state.PauseTiming();
    auto cpy = topologies[index];
    state.ResumeTiming();
    broker::alm::erase(cpy, id, [](auto&&...) {});
    benchmark::ClobberMemory();
    benchmark::DoNotOptimize(cpy);
  }
}

BENCHMARK_REGISTER_F(routing_table, erase_back)->DenseRange(0, 9, 1);
