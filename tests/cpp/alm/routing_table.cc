#define SUITE alm.routing_table

#include "broker/alm/routing_table.hh"

#include "test.hh"

using namespace broker;
using namespace broker::literals;

namespace {

struct fixture {
  using table_type = alm::routing_table<std::string, int>;

  std::string A = "A";

  std::string B = "B";

  std::string C = "C";

  std::string D = "D";

  std::string E = "E";

  std::string I = "I";

  std::string J = "J";

  fixture() {
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
    auto add = [&](std::string id,
                   std::vector<std::vector<std::string>> paths) {
      auto& entry = tbl.emplace(id, table_type::mapped_type{0}).first->second;
      for (auto& path : paths)
        entry.versioned_paths.emplace(std::move(path),
                                      alm::vector_timestamp(path.size()));
    };
    add(B, {{B}, {J, I, D, B}, {J, I, E, B}});
    add(D, {{B, D}, {J, I, D}});
    add(E, {{B, E}, {J, I, E}});
    add(I, {{B, E, I}, {B, D, I}, {J, I}});
    add(J, {{J}, {B, D, I, J}, {B, E, I, J}});
  }

  // Creates a list of IDs (strings).
  template <class... Ts>
  auto ls(Ts... xs) {
    return std::vector<std::string>{std::move(xs)...};
  }

  alm::routing_table<std::string, int> tbl;
};

void nop(const std::string&) {
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

TEST(blacklisted scans pahts for revoked paths){
  using alm::blacklisted;
  auto path = ls(A, B, C, D);
  auto ts = alm::vector_timestamp{{2_lt, 2_lt, 2_lt, 2_lt}};
  MESSAGE("blacklist entries for X -> Y with timestamp 3 (newer) hit");
  {
    CHECK(blacklisted(path, ts, A, 3_lt, B));
    CHECK(blacklisted(path, ts, B, 3_lt, A));
    CHECK(blacklisted(path, ts, B, 3_lt, C));
    CHECK(blacklisted(path, ts, C, 3_lt, B));
    CHECK(blacklisted(path, ts, C, 3_lt, D));
    CHECK(blacklisted(path, ts, D, 3_lt, C));
  }
  MESSAGE("blacklist entries for X -> Y with timestamp 2 (same) hit");
  {
    CHECK(blacklisted(path, ts, A, 2_lt, B));
    CHECK(blacklisted(path, ts, B, 2_lt, A));
    CHECK(blacklisted(path, ts, B, 2_lt, C));
    CHECK(blacklisted(path, ts, C, 2_lt, B));
    CHECK(blacklisted(path, ts, C, 2_lt, D));
    CHECK(blacklisted(path, ts, D, 2_lt, C));
  }
  MESSAGE("blacklist entries for X -> Y with timestamp 1 (oder) do not hit");
  {
    CHECK(not blacklisted(path, ts, A, 1_lt, B));
    CHECK(not blacklisted(path, ts, B, 1_lt, A));
    CHECK(not blacklisted(path, ts, B, 1_lt, C));
    CHECK(not blacklisted(path, ts, C, 1_lt, B));
    CHECK(not blacklisted(path, ts, C, 1_lt, D));
    CHECK(not blacklisted(path, ts, D, 1_lt, C));
  }
}

TEST(blacklisting removes revokes paths) {
  MESSAGE("before revoking B -> D, we reach D in two hops");
  {
    auto path = shortest_path(tbl, D);
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls(B, D));
  }
  MESSAGE("after revoking B -> D, we reach D in three hops");
  {
    auto callback = [](const auto&) { FAIL("OnRemovePeer callback called"); };
    revoke(tbl, B, alm::lamport_timestamp{2}, D, callback);
    auto path = shortest_path(tbl, D);
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls(J, I, D));
  }
  MESSAGE("after revoking J -> I, we no longer reach D");
  {
    std::vector<std::string> unreachables;
    auto callback = [&](const auto& x) { unreachables.emplace_back(x); };
    revoke(tbl, J, alm::lamport_timestamp{2}, I, callback);
    CHECK_EQUAL(unreachables, ls(D));
    CHECK_EQUAL(shortest_path(tbl, D), nullptr);
  }
}

TEST(blacklisting does not affect newer paths) {
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

TEST(inseting into blacklists creates a sorted list) {
  using blacklist = alm::blacklist<std::string>;
  blacklist lst;
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
  auto emplace = [&](std::string revoker, alm::lamport_timestamp rtime,
                     std::string hop) {
    dummy_self self;
    return alm::emplace(lst, &self, revoker, rtime, hop);
  };
  auto to_blacklist = [](auto range) {
    return blacklist(range.first, range.second);
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
  CHECK_EQUAL(lst, blacklist({{A, 1_lt, B},
                              {A, 2_lt, C},
                              {A, 3_lt, B},
                              {B, 7_lt, A},
                              {C, 1_lt, A},
                              {C, 2_lt, A}}));
  MESSAGE("equal_range allows access to subranges by revoker");
  CHECK_EQUAL(to_blacklist(equal_range(lst, A)),
              blacklist({{A, 1_lt, B}, {A, 2_lt, C}, {A, 3_lt, B}}));
  CHECK_EQUAL(to_blacklist(equal_range(lst, B)), blacklist({{B, 7_lt, A}}));
  CHECK_EQUAL(to_blacklist(equal_range(lst, C)),
              blacklist({{C, 1_lt, A}, {C, 2_lt, A}}));
}

FIXTURE_SCOPE_END()
