#define SUITE alm.multipath

#include "broker/alm/multipath.hh"

#include "test.hh"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include "broker/alm/routing_table.hh"
#include "broker/detail/inspect_objects.hh"

using namespace broker;

namespace {

using linear_path = std::vector<std::string>;

using multipath = alm::multipath<std::string>;

} // namespace

FIXTURE_SCOPE(multipath_tests, base_fixture)

TEST(multipaths are default constructible) {
  multipath p;
  CHECK_EQUAL(p.id(), "");
  CHECK_EQUAL(p.nodes().size(), 0u);
  CHECK_EQUAL(caf::deep_to_string(p), "()");
}

TEST(users can fill multipaths with emplace_node) {
  multipath p{"a"};
  auto ac = p.emplace_node("ac").first;
  ac->emplace_node("acb");
  ac->emplace_node("aca");
  auto ab = p.emplace_node("ab").first;
  ab->emplace_node("abb");
  ab->emplace_node("aba");
  CHECK_EQUAL(caf::deep_to_string(p),
              "(a, [(ab, [(aba), (abb)]), (ac, [(aca), (acb)])])");
}

TEST(multipaths are constructible from linear paths) {
  linear_path abc{"a", "b", "c"};
  multipath path{abc.begin(), abc.end()};
  CHECK_EQUAL(caf::deep_to_string(path), "(a, [(b, [(c)])])");
}

TEST(multipaths are copy constructible and comparable) {
  linear_path abc{"a", "b", "c"};
  multipath path1{abc.begin(), abc.end()};
  auto path2 = path1;
  CHECK_EQUAL(caf::deep_to_string(path1), caf::deep_to_string(path2));
  CHECK_EQUAL(path1, path2);
}

TEST(splicing an empty or equal linear path is a nop) {
  linear_path abc{"a", "b", "c"};
  multipath path1{abc.begin(), abc.end()};
  auto path2 = path1;
  linear_path empty_path;
  CHECK(path2.splice(empty_path));
  CHECK_EQUAL(path1, path2);
  CHECK(path2.splice(abc));
  CHECK_EQUAL(path1, path2);
}

TEST(splicing merges linear paths into multipaths) {
  linear_path abc{"a", "b", "c"};
  linear_path abd{"a", "b", "d"};
  linear_path aef{"a", "e", "f"};
  linear_path aefg{"a", "e", "f", "g"};
  multipath path{"a"};
  for (const auto& lp : {abc, abd, aef, aefg})
    CHECK(path.splice(lp));
  CHECK_EQUAL(caf::deep_to_string(path),
              "(a, [(b, [(c), (d)]), (e, [(f, [(g)])])])");
}

TEST(multipaths are serializable) {
  multipath path{"a"};
  MESSAGE("fill the path with nodes");
  {
    auto ac = path.emplace_node("ac").first;
    ac->emplace_node("acb");
    ac->emplace_node("aca");
    auto ab = path.emplace_node("ab").first;
    ab->emplace_node("abb");
    ab->emplace_node("aba");
  }
  caf::binary_serializer::container_type buf;
  MESSAGE("serializer the path into a buffer");
  {
    caf::binary_serializer sink{sys, buf};
    CHECK_EQUAL(detail::inspect_objects(sink, path), caf::none);
  }
  multipath copy{"a"};
  MESSAGE("deserializers a copy from the path from the buffer");
  {
    caf::binary_deserializer source{sys,buf};
    CHECK_EQUAL(detail::inspect_objects(source, copy), caf::none);
  }
  MESSAGE("after a serialization roundtrip, the path is equal to its copy");
  CHECK_EQUAL(path, copy);
  CHECK_EQUAL(caf::deep_to_string(path), caf::deep_to_string(copy));
}

TEST(source routing extracts multipaths from routing tables) {
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
  using table_type = alm::routing_table<std::string, int>;
  alm::routing_table<std::string, int> tbl;
  auto add = [&](std::string id, std::vector<std::vector<std::string>> paths) {
    auto& entry = tbl.emplace(id, table_type::mapped_type{0}).first->second;
    for (auto& path : paths)
      entry.versioned_paths.emplace(std::move(path),
                                    alm::vector_timestamp(path.size()));
  };
  // Creates a list of IDs (strings).
  auto ls = [](auto... xs) {
    return std::vector<std::string>{std::move(xs)...};
  };
  // Creates a multipath from a linear path.
  auto mp = [](auto... xs) {
    linear_path lp{std::move(xs)...};
    return multipath{lp.begin(), lp.end()};
  };
  add("B", {{"B"}, {"J", "I", "D", "B"}, {"J", "I", "E", "B"}});
  add("D", {{"B", "D"}, {"J", "I", "D"}});
  add("E", {{"B", "E"}, {"J", "I", "E"}});
  add("I", {{"B", "E", "I"}, {"B", "D", "I"}, {"J", "I"}});
  add("J", {{"J"}, {"B", "D", "I", "J"}, {"B", "E", "I", "J"}});
  MESSAGE("Sending to B and D creates a single multipath");
  {
    std::vector<multipath> paths;
    std::vector<std::string> unreachables;
    alm::generate_paths(ls("B", "D"), tbl, paths, unreachables);
    REQUIRE_EQUAL(paths.size(), 1u);
    REQUIRE_EQUAL(unreachables.size(), 0u);
    CHECK_EQUAL(paths[0], mp("B", "D"));
  }
  MESSAGE("Sending to E and I creates two multipaths");
  {
    std::vector<multipath> paths;
    std::vector<std::string> unreachables;
    alm::generate_paths(ls("E", "I"), tbl, paths, unreachables);
    REQUIRE_EQUAL(paths.size(), 2u);
    REQUIRE_EQUAL(unreachables.size(), 0u);
    CHECK_EQUAL(paths[0], mp("B", "E"));
    CHECK_EQUAL(paths[1], mp("J", "I"));
  }
  MESSAGE("Sending to D, E and I creates two multipaths");
  {
    std::vector<multipath> paths;
    std::vector<std::string> unreachables;
    alm::generate_paths(ls("D", "E", "I"), tbl, paths, unreachables);
    REQUIRE_EQUAL(paths.size(), 2u);
    REQUIRE_EQUAL(unreachables.size(), 0u);
    multipath d_and_e{"B"};
    d_and_e.emplace_node("D");
    d_and_e.emplace_node("E");
    CHECK_EQUAL(paths[0], d_and_e);
    CHECK_EQUAL(paths[1], mp("J", "I"));
  }
  MESSAGE("Sending to B and G creates one path and one unreachable");
  {
    std::vector<multipath> paths;
    std::vector<std::string> unreachables;
    alm::generate_paths(ls("B", "G"), tbl, paths, unreachables);
    REQUIRE_EQUAL(paths.size(), 1u);
    REQUIRE_EQUAL(unreachables.size(), 1u);
    CHECK_EQUAL(paths[0], mp("B"));
    CHECK_EQUAL(unreachables, ls("G"));
  }
}

FIXTURE_SCOPE_END()
