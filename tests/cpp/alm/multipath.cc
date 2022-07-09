#define SUITE alm.multipath

#include "broker/alm/multipath.hh"

#include "test.hh"

#include <random>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include "broker/alm/routing_table.hh"

using broker::alm::multipath;

using namespace broker;

namespace {

struct fixture : base_fixture {
  void stringify(const multipath& path, std::string& result) {
    if (!path.head().id()) {
      result += "()";
    } else {
      result += '(';
      result += id_by_value(path.head().id());
      if (path.num_nodes() > 0) {
        result += ", [";
        path.for_each_node([this, &result, first{true}](const auto& n) mutable {
          if (first)
            first = false;
          else
            result += ", ";
          stringify(n, result);
        });
        result += ']';
      }
      result += ')';
    }
  }

  // Creates a list of endpoint IDs from their char key.
  template <class... Ts>
  auto ls(Ts... xs) {
    return std::vector<endpoint_id>{ids[xs]...};
  }

  std::string stringify(const multipath& path) {
    std::string result;
    stringify(path, result);
    return result;
  }

  std::shared_ptr<alm::multipath_tree> tptr;

  void make_tree(char id) {
    tptr = std::make_shared<alm::multipath_tree>(ids[id]);
  }

  using emplace_result = std::pair<alm::multipath_node*, bool>;

  emplace_result emplace(alm::multipath_node* pos, char id) {
    return pos->nodes().emplace(tptr->mem, ids[id]);
  }

  emplace_result emplace(char id) {
    return emplace(tptr->root, id);
  }
};

} // namespace

FIXTURE_SCOPE(multipath_tests, fixture)

TEST(multipaths are default constructible) {
  multipath p;
  CHECK(!p.head().id());
  CHECK_EQUAL(p.num_nodes(), 0u);
  CHECK_EQUAL(stringify(p), "()");
}

TEST(multipath trees grow with emplace) {
  make_tree('A');
  auto ac = emplace('C').first;
  CHECK_EQUAL(ac->head().id(), ids['C']);
  emplace(ac, 'D');
  emplace(ac, 'E');
  auto ab = emplace('B').first;
  CHECK_EQUAL(ab->head().id(), ids['B']);
  emplace(ab, 'F');
  emplace(ab, 'G');
  std::string buf;
  auto path = multipath{tptr};
  CHECK_EQUAL(stringify(path), "(A, [(B, [(F), (G)]), (C, [(D), (E)])])");
}

TEST(multipath nodes sort their children) {
  std::vector<char> children{'B', 'C', 'D', 'E'};
  do {
    make_tree('A');
    for (auto child : children)
      emplace(child);
    CHECK_EQUAL(stringify(multipath{tptr}), "(A, [(B), (C), (D), (E)])");
  } while (std::next_permutation(children.begin(), children.end()));
}

TEST(multipaths are constructible from linear paths) {
  auto abc = ls('A', 'B', 'C');
  multipath path{abc.begin(), abc.end()};
  CHECK_EQUAL(stringify(path), "(A, [(B, [(C)])])");
}

TEST(multipaths are copy constructible and comparable) {
  auto abc = ls('A', 'B', 'C');
  multipath path1{abc.begin(), abc.end()};
  auto path2 = path1;
  CHECK_EQUAL(stringify(path1), stringify(path2));
  CHECK_EQUAL(path1, path2);
}

TEST(multipaths are serializable) {
  make_tree('A');
  MESSAGE("fill the tree with nodes");
  {
    auto ac = emplace('C').first;
    emplace(ac, 'D');
    emplace(ac, 'E');
    auto ab = emplace('B').first;
    emplace(ab, 'F');
    emplace(ab, 'G');
  }
  auto path = multipath{tptr};
  caf::binary_serializer::container_type buf;
  MESSAGE("serializer the path into a buffer");
  {
    caf::binary_serializer sink{sys, buf};
    CHECK(sink.apply(path));
  }
  multipath copy;
  MESSAGE("deserializers a copy from the path from the buffer");
  {
    caf::binary_deserializer source{sys, buf};
    CHECK(source.apply(copy));
  }
  MESSAGE("after a serialization roundtrip, the path is equal to its copy");
  CHECK_EQUAL(stringify(path), stringify(copy));
  CHECK_EQUAL(to_string(path), to_string(copy));
  CHECK_EQUAL(path, copy);
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
  alm::routing_table tbl;
  auto add = [&](char id, std::vector<std::vector<char>> paths) {
    auto& entry = tbl.emplace(ids[id], alm::routing_table_row{}).first->second;
    for (auto& path : paths) {
      std::vector<endpoint_id> xs;
      for (auto c : path)
        xs.emplace_back(ids[c]);
      alm::add_or_update_path(tbl, ids[id], std::move(xs),
                              vector_timestamp(path.size()));
    }
  };
  add('B', {{'B'}, {'J', 'I', 'D', 'B'}, {'J', 'I', 'E', 'B'}});
  add('D', {{'B', 'D'}, {'J', 'I', 'D'}});
  add('E', {{'B', 'E'}, {'J', 'I', 'E'}});
  add('I', {{'B', 'E', 'I'}, {'B', 'D', 'I'}, {'J', 'I'}});
  add('J', {{'J'}, {'B', 'D', 'I', 'J'}, {'B', 'E', 'I', 'J'}});
  MESSAGE("Sending to B and D creates a single multipath");
  {
    std::vector<multipath> paths;
    std::vector<endpoint_id> unreachables;
    alm::multipath::generate(ls('B', 'D'), tbl, paths, unreachables);
    REQUIRE_EQUAL(paths.size(), 1u);
    REQUIRE_EQUAL(unreachables.size(), 0u);
    CHECK_EQUAL(stringify(paths[0]), "(B, [(D)])");
  }
  MESSAGE("Sending to E and I creates two multipaths");
  {
    std::vector<multipath> paths;
    std::vector<endpoint_id> unreachables;
    alm::multipath::generate(ls('E', 'I'), tbl, paths, unreachables);
    REQUIRE_EQUAL(paths.size(), 2u);
    REQUIRE_EQUAL(unreachables.size(), 0u);
    CHECK_EQUAL(stringify(paths[0]), "(B, [(E)])");
    CHECK_EQUAL(stringify(paths[1]), "(J, [(I)])");
  }
  MESSAGE("Sending to D, E and I creates two multipaths");
  {
    std::vector<multipath> paths;
    std::vector<endpoint_id> unreachables;
    alm::multipath::generate(ls('D', 'E', 'I'), tbl, paths, unreachables);
    REQUIRE_EQUAL(paths.size(), 2u);
    REQUIRE_EQUAL(unreachables.size(), 0u);
    CHECK_EQUAL(stringify(paths[0]), "(B, [(D), (E)])");
    CHECK_EQUAL(stringify(paths[1]), "(J, [(I)])");
  }
  MESSAGE("Sending to B and G creates one path and one unreachable");
  {
    std::vector<multipath> paths;
    std::vector<endpoint_id> unreachables;
    alm::multipath::generate(ls('B', 'G'), tbl, paths, unreachables);
    REQUIRE_EQUAL(paths.size(), 1u);
    REQUIRE_EQUAL(unreachables.size(), 1u);
    CHECK_EQUAL(stringify(paths[0]), "(B)");
    CHECK_EQUAL(unreachables, ls('G'));
  }
}

FIXTURE_SCOPE_END()
