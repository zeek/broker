#include "broker/alm/multipath.hh"
#include "broker/alm/routing_table.hh"
#include "broker/endpoint.hh"

#include <benchmark/benchmark.h>

#include <atomic>
#include <limits>
#include <random>

using broker::endpoint_id;
using broker::vector_timestamp;

namespace {

struct linear_routing_table_row {
  using path_type = std::vector<endpoint_id>;

  using versioned_path_type = std::pair<path_type, vector_timestamp>;

  endpoint_id id;

  caf::actor hdl;

  std::vector<versioned_path_type> versioned_paths;

  linear_routing_table_row() = default;
  linear_routing_table_row(linear_routing_table_row&&) = default;
  linear_routing_table_row(const linear_routing_table_row&) = default;
  linear_routing_table_row& operator=(linear_routing_table_row&&) = default;
  linear_routing_table_row&
  operator=(const linear_routing_table_row&) = default;

  explicit linear_routing_table_row(endpoint_id id) : id(std::move(id)) {
    versioned_paths.reserve(32);
  }

  linear_routing_table_row(endpoint_id id, caf::actor hdl)
    : id(std::move(id)), hdl(std::move(hdl)) {
    versioned_paths.reserve(32);
  }
};

struct linear_routing_table {
  std::vector<linear_routing_table_row> rows;

  const std::vector<endpoint_id>* shortest_path(const endpoint_id& peer) const {
    auto pred = [&peer](const auto& row) { return row.id == peer; };
    if (auto i = std::find_if(rows.begin(), rows.end(), pred); i != rows.end())
      return std::addressof(i->versioned_paths.front().first);
    else
      return nullptr;
  }

  linear_routing_table_row* find_row(const endpoint_id& peer) {
    auto pred = [&peer](const auto& row) { return row.id == peer; };
    if (auto i = std::find_if(rows.begin(), rows.end(), pred); i != rows.end())
      return std::addressof(*i);
    else
      return nullptr;
  }

  linear_routing_table_row& operator[](const endpoint_id& peer) {
    auto pred = [&peer](const auto& row) { return row.id == peer; };
    if (auto i = std::find_if(rows.begin(), rows.end(), pred);
        i != rows.end()) {
      return *i;
    } else {
      rows.emplace_back(peer);
      return rows.back();
    }
  }
};

auto shortest_path(const linear_routing_table& tbl, const endpoint_id& peer) {
  return tbl.shortest_path(peer);
}

struct sorted_linear_routing_table {
  std::vector<linear_routing_table_row> rows;

  struct row_less_t {
    bool operator()(const endpoint_id& x,
                    const linear_routing_table_row& y) const noexcept {
      return x < y.id;
    }

    bool operator()(const linear_routing_table_row& x,
                    const endpoint_id& y) const noexcept {
      return x.id < y;
    }
  };

  static constexpr auto row_less = row_less_t{};

  const std::vector<endpoint_id>* shortest_path(const endpoint_id& peer) const {
    if (auto i = std::lower_bound(rows.begin(), rows.end(), peer, row_less);
        i != rows.end() && i->id == peer)
      return std::addressof(i->versioned_paths.front().first);
    else
      return nullptr;
  }

  linear_routing_table_row* find_row(const endpoint_id& peer) {
    if (auto i = std::lower_bound(rows.begin(), rows.end(), peer, row_less);
        i != rows.end() && i->id == peer)
      return std::addressof(*i);
    else
      return nullptr;
  }

  linear_routing_table_row& operator[](const endpoint_id& peer) {
    if (auto i = std::lower_bound(rows.begin(), rows.end(), peer, row_less);
        i != rows.end()) {
      if (i->id == peer) {
        return *i;
      } else {
        return *rows.emplace(i, peer);
      }
    } else {
      rows.emplace_back(peer);
      return rows.back();
    }
  }
};

auto shortest_path(const sorted_linear_routing_table& tbl,
                   const endpoint_id& peer) {
  return tbl.shortest_path(peer);
}

template <class TableType>
bool add_or_update_path_impl(TableType& tbl, const endpoint_id& peer,
                             std::vector<endpoint_id>&& path,
                             vector_timestamp&& ts) {
  auto& row = tbl[peer];
  auto& paths = row.versioned_paths;
  auto i = std::lower_bound(paths.begin(), paths.end(), path,
                            broker::alm::path_less);
  if (i == paths.end()) {
    paths.emplace_back(std::move(path), std::move(ts));
    return true;
  } else if (i->first != path) {
    paths.insert(i, std::make_pair(std::move(path), std::move(ts)));
    return true;
  } else {
    if (i->second < ts)
      i->second = std::move(ts);
    return false;
  }
}

bool add_or_update_path(linear_routing_table& tbl, const endpoint_id& peer,
                        std::vector<endpoint_id> path, vector_timestamp ts) {
  return add_or_update_path_impl(tbl, peer, std::move(path), std::move(ts));
}

bool add_or_update_path(sorted_linear_routing_table& tbl,
                        const endpoint_id& peer, std::vector<endpoint_id> path,
                        vector_timestamp ts) {
  return add_or_update_path_impl(tbl, peer, std::move(path), std::move(ts));
}

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

template <class TableType>
class routing_table : public benchmark::Fixture {
public:
  using table_type = TableType;

  routing_table() {
    topologies.resize(10);
    // v2_topologies.resize(10);
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
    broker::vector_timestamp result;
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

  void add_or_update_path_bench(benchmark::State& state) {
    auto max_level = static_cast<size_t>(state.range(0));
    for (auto _ : state) {
      id_generator g;
      table_type tbl;
      fill_tbl(tbl, g, {}, 0, max_level, [](auto&&...) {});
      benchmark::ClobberMemory();
      benchmark::DoNotOptimize(tbl);
    }
  }

  void shortest_path_bench(benchmark::State& state) {
    auto index = static_cast<size_t>(state.range(0));
    const auto& tbl = topologies[index];
    const auto& id = ids[index].front();
    for (auto _ : state) {
      auto sp = shortest_path(tbl, id);
      benchmark::DoNotOptimize(sp);
      assert(sp != nullptr);
      assert(sp->size() == index + 1);
    }
  }

  void generate_paths_1_bench(benchmark::State& state) {
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

  void generate_paths_10_bench(benchmark::State& state) {
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

  void erase_front_bench(benchmark::State& state) {
    auto index = static_cast<size_t>(state.range(0));
    const auto& id = ids[0].front();
    for (auto _ : state) {
      state.PauseTiming();
      auto cpy = topologies[index];
      state.ResumeTiming();
      erase(cpy, id, [](auto&&...) {});
      benchmark::ClobberMemory();
      benchmark::DoNotOptimize(cpy);
    }
  }

  void erase_back_bench(benchmark::State& state) {
    auto index = static_cast<size_t>(state.range(0));
    const auto& id = ids[index].front();
    for (auto _ : state) {
      state.PauseTiming();
      auto cpy = topologies[index];
      state.ResumeTiming();
      erase(cpy, id, [](auto&&...) {});
      benchmark::ClobberMemory();
      benchmark::DoNotOptimize(cpy);
    }
  }
};

} // namespace

using default_routing_table = broker::alm::routing_table;

#define BENCH_SETUP(TableType, Algorithm)                                      \
  using TableType##_impl = routing_table<TableType>;                           \
  BENCHMARK_DEFINE_F(TableType##_impl, Algorithm)                              \
  (benchmark::State & state) {                                                 \
    Algorithm##_bench(state);                                                  \
  }                                                                            \
  BENCHMARK_REGISTER_F(TableType##_impl, Algorithm)->DenseRange(0, 9, 1);

// -- adding entries to a routing table ----------------------------------------

BENCH_SETUP(default_routing_table, add_or_update_path)
BENCH_SETUP(default_routing_table, shortest_path)
BENCH_SETUP(default_routing_table, generate_paths_1)
BENCH_SETUP(default_routing_table, generate_paths_10)
BENCH_SETUP(default_routing_table, erase_front)
BENCH_SETUP(default_routing_table, erase_back)

BENCH_SETUP(linear_routing_table, add_or_update_path)
BENCH_SETUP(linear_routing_table, shortest_path)
// BENCH_SETUP(linear_routing_table, generate_paths_1)
// BENCH_SETUP(linear_routing_table, generate_paths_10)
// BENCH_SETUP(linear_routing_table, erase_front)
// BENCH_SETUP(linear_routing_table, erase_back)

BENCH_SETUP(sorted_linear_routing_table, add_or_update_path)
BENCH_SETUP(sorted_linear_routing_table, shortest_path)
// BENCH_SETUP(sorted_linear_routing_table, generate_paths_1)
// BENCH_SETUP(sorted_linear_routing_table, generate_paths_10)
// BENCH_SETUP(sorted_linear_routing_table, erase_front)
// BENCH_SETUP(sorted_linear_routing_table, erase_back)
