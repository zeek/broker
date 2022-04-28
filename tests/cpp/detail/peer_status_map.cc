#define SUITE detail.peer_status_map

#include "broker/detail/peer_status_map.hh"

#include "test.hh"

using namespace broker;

namespace {

struct fixture : base_fixture {
  detail::peer_status_map uut;
  endpoint_id pid;

  fixture() {
    pid = ids['B'];
  }
};

} // namespace

FIXTURE_SCOPE(peer_status_map_tests, fixture)

TEST(inserting already known peers fails) {
  CHECK(uut.insert(pid));
  auto st = peer_status::connecting;
  CHECK(!uut.insert(pid, st));
  CHECK_EQUAL(st, peer_status::initialized);
}

TEST(update only succeeds if the expected value matches) {
  CHECK(uut.insert(pid));
  MESSAGE("initialized -> connecting (ok)");
  auto expected = peer_status::initialized;
  CHECK(uut.update(pid, expected, peer_status::connecting));
  MESSAGE("initialized -> connecting (fails, no longer in initialized state)");
  CHECK(!uut.update(pid, expected, peer_status::connecting));
  CHECK_EQUAL(expected, peer_status::connecting);
}

TEST(remove only succeeds if the expected value matches) {
  auto expected = peer_status::connected;
  CHECK(!uut.remove(pid, expected));
  CHECK_EQUAL(expected, peer_status::unknown);
  CHECK(uut.insert(pid));
  CHECK(!uut.remove(pid, expected));
  CHECK_EQUAL(expected, peer_status::initialized);
  CHECK(uut.remove(pid, expected));
}

FIXTURE_SCOPE_END()
