#define SUITE alm.routing_table

#include "broker/alm/routing_table.hh"

#include "test.hh"

using namespace broker;
using namespace broker::literals;

namespace {

struct fixture : base_fixture {
  endpoint_id A;

  endpoint_id B;

  endpoint_id C;

  endpoint_id D;

  endpoint_id E;

  endpoint_id I;

  endpoint_id J;

  fixture() {
    A = ids['A'];
    B = ids['B'];
    C = ids['C'];
    D = ids['D'];
    E = ids['E'];
    I = ids['I'];
    J = ids['J'];
    // We use the subset of the topology that we use in the peer unit test:
    //
    //                                     +---+
    //                               +-----+ D +-----+
    //                               |     +---+     |
    //                               |               |
    //                             +---+           +---+
    //                       +-----+ B |           | I +-+
    //                       |     +---+           +---+ |
    //                       |       |               |   |
    //                       |       |     +---+     |   |
    //                       |       +-----+ E +-----+   |
    //                       |             +---+         |
    //                     +---+                       +---+
    //                     | A +-----------------------+ J |
    //                     +---+                       +---+
    //
    auto add = [&](endpoint_id id,
                   std::vector<std::vector<endpoint_id>> paths) {
      auto& entry = tbl.emplace(id, alm::routing_table_row{}).first->second;
      for (auto& path : paths)
        add_or_update_path(tbl, id, path, vector_timestamp(path.size()));
    };
    add(B, {{B}, {J, I, D, B}, {J, I, E, B}});
    add(D, {{B, D}, {J, I, D}});
    add(E, {{B, E}, {J, I, E}});
    add(I, {{B, E, I}, {B, D, I}, {J, I}});
    add(J, {{J}, {B, D, I, J}, {B, E, I, J}});
  }

  // Creates a list of IDs (endpoint IDs).
  template <class... Ts>
  auto ls(Ts... xs) {
    return std::vector<endpoint_id>{std::move(xs)...};
  }

  alm::routing_table tbl;
};

void nop(const endpoint_id&) {
  // nop
}

} // namespace

FIXTURE_SCOPE(routing_table_tests, fixture)

TEST(erase removes all paths that to and from a peer) {
  MESSAGE("before removing J, the shortest path to I is: J -> I");
  {
    auto path = shortest_path(tbl, I);
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls(J, I));
  }
  MESSAGE("after removing J, the shortest path to I is: B -> D -> I");
  {
    erase(tbl, J, nop);
    auto path = shortest_path(tbl, I);
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls(B, D, I));
  }
}

TEST(erase_direct drops the direct path but peers can remain reachable) {
  MESSAGE("before calling erase_direct(B), we reach B in one hop");
  {
    auto path = shortest_path(tbl, B);
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls(B));
  }
  MESSAGE("after calling erase_direct(B), we need four hops to reach B");
  {
    erase_direct(tbl, B, nop);
    auto path = shortest_path(tbl, B);
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls(J, I, D, B));
  }
}

TEST(peers may revoke paths) {
  using alm::revoked;
  auto path = ls(A, B, C, D);
  auto ts = vector_timestamp{{2_lt, 2_lt, 2_lt, 2_lt}};
  MESSAGE("revocations entries for X -> Y with timestamp 3 (newer) hit");
  {
    CHECK(revoked(path, ts, A, 3_lt, B));
    CHECK(revoked(path, ts, B, 3_lt, A));
    CHECK(revoked(path, ts, B, 3_lt, C));
    CHECK(revoked(path, ts, C, 3_lt, B));
    CHECK(revoked(path, ts, C, 3_lt, D));
    CHECK(revoked(path, ts, D, 3_lt, C));
  }
  MESSAGE("revocations entries for X -> Y with timestamp 2 (same) hit");
  {
    CHECK(revoked(path, ts, A, 2_lt, B));
    CHECK(revoked(path, ts, B, 2_lt, A));
    CHECK(revoked(path, ts, B, 2_lt, C));
    CHECK(revoked(path, ts, C, 2_lt, B));
    CHECK(revoked(path, ts, C, 2_lt, D));
    CHECK(revoked(path, ts, D, 2_lt, C));
  }
  MESSAGE("revocations entries for X -> Y with timestamp 1 (oder) do not hit");
  {
    CHECK(not revoked(path, ts, A, 1_lt, B));
    CHECK(not revoked(path, ts, B, 1_lt, A));
    CHECK(not revoked(path, ts, B, 1_lt, C));
    CHECK(not revoked(path, ts, C, 1_lt, B));
    CHECK(not revoked(path, ts, C, 1_lt, D));
    CHECK(not revoked(path, ts, D, 1_lt, C));
  }
}

TEST(revocationsing removes revokes paths) {
  MESSAGE("before revoking B -> D, we reach D in two hops");
  {
    auto path = shortest_path(tbl, D);
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls(B, D));
  }
  MESSAGE("after revoking B -> D, we reach D in three hops");
  {
    auto callback = [](const auto&) { FAIL("OnRemovePeer callback called"); };
    revoke(tbl, B, lamport_timestamp{2}, D, callback);
    auto path = shortest_path(tbl, D);
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls(J, I, D));
  }
  MESSAGE("after revoking J -> I, we no longer reach D");
  {
    std::vector<endpoint_id> unreachables;
    auto callback = [&](const auto& x) { unreachables.emplace_back(x); };
    revoke(tbl, J, lamport_timestamp{2}, I, callback);
    CHECK_EQUAL(unreachables, ls(D));
    CHECK_EQUAL(shortest_path(tbl, D), nullptr);
  }
}

TEST(revocationsing does not affect newer paths) {
  MESSAGE("set all timestamps to 3");
  {
    for (auto& row : tbl)
      for (auto& path : row.second.versioned_paths)
        for (auto& t : path.second)
          t = 3_lt;
  }
  MESSAGE("revoking B -> D with timestamp 2 has no effect");
  {
    auto callback = [](const auto&) { FAIL("OnRemovePeer callback called"); };
    revoke(tbl, B, 2_lt, D, callback);
    auto path = shortest_path(tbl, D);
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls(B, D));
  }
}

TEST(inseting into revocationss creates a sorted list) {
  using revocations = alm::revocations<endpoint_id>;
  revocations lst;
  struct dummy_self {
    auto clock() {
      struct dummy_clock {
        auto now() {
          return caf::actor_clock::clock_type::now();
        }
      };
      return dummy_clock{};
    }
  };
  auto emplace = [&](endpoint_id revoker, lamport_timestamp rtime,
                     endpoint_id hop) {
    dummy_self self;
    return alm::emplace(lst, &self, revoker, rtime, hop);
  };
  auto to_revocations = [](auto range) {
    return revocations(range.first, range.second);
  };
  MESSAGE("filling the list with new entries inserts");
  CHECK(emplace(A, 1_lt, B).second);
  CHECK(emplace(C, 2_lt, A).second);
  CHECK(emplace(A, 3_lt, B).second);
  CHECK(emplace(C, 1_lt, A).second);
  CHECK(emplace(B, 7_lt, A).second);
  CHECK(emplace(A, 2_lt, C).second);
  MESSAGE("inserting twice is a no-op");
  CHECK(not emplace(A, 1_lt, B).second);
  CHECK(not emplace(B, 7_lt, A).second);
  MESSAGE("the final list is sorted on revoker, ts, hop");
  CHECK_EQUAL(lst, revocations({{A, 1_lt, B},
                                {A, 2_lt, C},
                                {A, 3_lt, B},
                                {B, 7_lt, A},
                                {C, 1_lt, A},
                                {C, 2_lt, A}}));
  MESSAGE("equal_range allows access to subranges by revoker");
  CHECK_EQUAL(to_revocations(equal_range(lst, A)),
              revocations({{A, 1_lt, B}, {A, 2_lt, C}, {A, 3_lt, B}}));
  CHECK_EQUAL(to_revocations(equal_range(lst, B)), revocations({{B, 7_lt, A}}));
  CHECK_EQUAL(to_revocations(equal_range(lst, C)),
              revocations({{C, 1_lt, A}, {C, 2_lt, A}}));
}

FIXTURE_SCOPE_END()
