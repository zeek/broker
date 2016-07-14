#include <poll.h>

#include "broker/broker.hh"

#define SUITE endpoint
#include "test.hpp"

using namespace broker;

using std::chrono::milliseconds;
using std::chrono::seconds;

namespace {

bool is_ready(blocking_endpoint& e, seconds secs = seconds::zero()) {
  auto fd = e.mailbox().descriptor();
  pollfd p = {fd, POLLIN, {}};
  auto n = ::poll(&p, 1, secs.count() * 1000);
  if (n < 0)
    std::terminate();
  return n == 1 && p.revents & POLLIN;
}

} // namespace <anonymous>

TEST(blocking construction) {
  context ctx;
  auto b = ctx.spawn<blocking>();
  endpoint e = b; // upcast
}

TEST(nonblocking construction) {
  context ctx;
  auto n = ctx.spawn<nonblocking>(
    [](const topic&, const message&) {
      // nop
    }
  );
  endpoint e = n; // upcast
}

TEST(subscription management) {
  context ctx;
  auto e = ctx.spawn<blocking>();
  e.subscribe("/foo");
  e.subscribe("/foo/bar");
  e.unsubscribe("/foo");
  e.publish("/foo/baz", 42); // Does not match.
  CHECK(!is_ready(e));
  CHECK(e.mailbox().empty());
}

TEST(nonblocking subscription) {
  auto counter = std::make_shared<int>(0);
  // The destructor of the context object blocks until all actors have
  // terminated. In this case, once our endpoint "n" goes out of scope, the
  // underlying actor will receive an exit message. Prior to that, it will
  // process the three published messages.
  {
    context ctx;
    auto n = ctx.spawn<nonblocking>(
      [=](const topic&, const message&) {
        ++*counter;
      }
    );
    n.subscribe("/foo");
    n.publish("/foo/bar", 42);
    n.publish("/foo/baz", 4.2);
    n.publish("/foo/qux", "broker");
  }
  CHECK_EQUAL(*counter, 3);
}

TEST(blocking lambda receive) {
  context ctx;
  auto e = ctx.spawn<blocking>();
  e.subscribe("/foo");
  e.publish("/foo/bar", 42);
  e.publish("/foo/baz", -1);
  CHECK(is_ready(e, seconds{1}));
  e.receive([](const topic& t, const message& msg) {
    CHECK_EQUAL(t, "/foo/bar"_t);
    CHECK_EQUAL(*msg.get_as<data>(0).get<integer>(), 42);
  });
  CHECK(is_ready(e));
  CHECK(!e.mailbox().empty());
  e.receive([](const topic& t, const message& msg) {
    CHECK_EQUAL(t, "/foo/baz"_t);
    CHECK_EQUAL(msg.get_as<data>(0), data{-1});
  });
  CHECK(e.mailbox().empty());
  CHECK(!is_ready(e));
}

TEST(blocking non-lambda receive) {
  context ctx;
  auto e = ctx.spawn<blocking>();
  e.subscribe("/foo");
  auto m0 = make_data_message("broker");
  e.publish("/foo", m0);
  CHECK(is_ready(e, seconds{1}));
  auto msg = e.receive();
  REQUIRE_EQUAL(msg.size(), 2u); // <topic, message>
  auto t = msg.get_as<topic>(0);
  CHECK_EQUAL(t, "/foo"_t);
  auto m1 = msg.get_as<message>(1);
  REQUIRE_EQUAL(m1.size(), 1u);
  auto str = m1.get_as<data>(0).get<std::string>();
  REQUIRE(str);
  CHECK_EQUAL(*str, "broker");
  CHECK(e.mailbox().empty());
  CHECK(!is_ready(e));
}

TEST(multi-topic subscription) {
  context ctx;
  auto e = ctx.spawn<blocking>();
  e.subscribe("/foo");
  e.subscribe("/foo/bar");
  e.subscribe("/foo/bar/baz");
  e.publish("/foo/bar/baz", 4.2);
  e.receive(); // Block and wait until the next message, then discard it.
  CHECK(e.mailbox().empty());
}

TEST(local peering and unpeering) {
  context ctx;
  auto x = ctx.spawn<blocking>();
  auto y = ctx.spawn<blocking>();
  MESSAGE("peer endpoints");
  x.peer(y);
  x.receive([](const status& s) { CHECK(s == peer_added); });
  y.receive([](const status& s) { CHECK(s == peer_added); });
  CHECK(x.mailbox().empty());
  CHECK(y.mailbox().empty());
  MESSAGE("verifying peer info of originator");
  auto peers = x.peers();
  REQUIRE_EQUAL(peers.size(), 1u);
  CHECK(is_outbound(peers[0].flags));
  CHECK(!is_inbound(peers[0].flags));
  CHECK_EQUAL(peers[0].peer.id, y.info().id);
  CHECK(!peers[0].peer.network);
  MESSAGE("verifying peer info of responder");
  peers = y.peers();
  REQUIRE_EQUAL(peers.size(), 1u);
  CHECK(!is_outbound(peers[0].flags));
  CHECK(is_inbound(peers[0].flags));
  CHECK_EQUAL(peers[0].peer.id, x.info().id);
  CHECK(!peers[0].peer.network);
  MESSAGE("unpeer endpoints");
  x.unpeer(y);
  x.receive([](const status& s) { CHECK(s == peer_removed); });
  y.receive([](const status& s) { CHECK(s == peer_removed); });
  CHECK_EQUAL(x.peers().size(), 0u);
  CHECK_EQUAL(y.peers().size(), 0u);
  MESSAGE("attempt to unpeer from already unpeered endpoint");
  y.unpeer(x);
  y.receive([](const status& s) { CHECK(s == peer_invalid); });
}

TEST(remote peering) {
  context ctx;
  auto x = ctx.spawn<blocking>();
  auto y = ctx.spawn<blocking>();
  auto bound_port = y.listen("127.0.0.1");
  REQUIRE(bound_port > 0);
  x.peer("127.0.0.1", bound_port);
  x.receive([](const status& s) { CHECK(s == peer_added); });
  y.receive([](const status& s) { CHECK(s == peer_added); });
  CHECK_EQUAL(x.peers().size(), 1u);
  CHECK_EQUAL(y.peers().size(), 1u);
}

TEST(local peering termination) {
  context ctx;
  auto x = ctx.spawn<blocking>();
  {
    auto y = ctx.spawn<blocking>();
    x.peer(y);
    x.receive([](const status& s) { CHECK(s == peer_added); });
    y.receive([](const status& s) { CHECK(s == peer_added); });
    CHECK_EQUAL(x.peers().size(), 1u);
    CHECK_EQUAL(y.peers().size(), 1u);
  }
  // We cannot re-establish a connection to a local endpoint. If it terminates,
  // the peering is gone.
  x.receive([](const status& s) { CHECK(s == peer_removed); });
  CHECK_EQUAL(x.peers().size(), 0u);
}

TEST(remote peering termination) {
  context ctx;
  auto bound_port = uint16_t{0};
  auto x = ctx.spawn<blocking>();
  {
    auto y = ctx.spawn<blocking>();
    bound_port = y.listen("127.0.0.1");
    REQUIRE(bound_port > 0);
    x.peer("127.0.0.1", bound_port);
    x.receive([](const status& s) { CHECK(s == peer_added); });
    y.receive([](const status& s) { CHECK(s == peer_added); });
  }
  // When losing a connection to remote endpoint, we still keep that peer
  // around until the peering originator removes it.
  x.receive([&](const status& s) {
    CHECK(s == peer_lost);
    REQUIRE(s.peer.network);
    CHECK_EQUAL(s.peer.network->address, "127.0.0.1");
    CHECK_EQUAL(s.peer.network->port, bound_port);
  });
  CHECK_EQUAL(x.peers().size(), 1u);
  // Now remove the peer.
  x.unpeer("127.0.0.1", bound_port);
  x.receive([&](const status& s) { CHECK(s == peer_removed); });
  CHECK_EQUAL(x.peers().size(), 0u);
}

TEST(subscribe after peering) {
  MESSAGE("spawning endpoints");
  context ctx;
  auto a = ctx.spawn<blocking>();
  auto b = ctx.spawn<blocking>();
  auto c = ctx.spawn<blocking>();
  auto d = ctx.spawn<blocking>();
  // A <-> B <-> C <-> D
  MESSAGE("chaining peers");
  a.peer(b);
  a.receive([](const status& s) { CHECK(s == peer_added); });
  b.receive([](const status& s) { CHECK(s == peer_added); });
  b.peer(c);
  b.receive([](const status& s) { CHECK(s == peer_added); });
  c.receive([](const status& s) { CHECK(s == peer_added); });
  c.peer(d);
  c.receive([](const status& s) { CHECK(s == peer_added); });
  d.receive([](const status& s) { CHECK(s == peer_added); });
  CHECK_EQUAL(a.peers().size(), 1u);
  CHECK_EQUAL(b.peers().size(), 2u);
  CHECK_EQUAL(c.peers().size(), 2u);
  CHECK_EQUAL(d.peers().size(), 1u);
  CHECK(a.mailbox().empty());
  CHECK(b.mailbox().empty());
  CHECK(c.mailbox().empty());
  CHECK(d.mailbox().empty());
  MESSAGE("propagating subscriptions");
  a.subscribe("/foo");
  c.subscribe("/bar");
  // Wait until subscriptions propagated along the chain.
  std::this_thread::sleep_for(milliseconds{300});
  MESSAGE("D -> C -> B -> A");
  d.publish("/foo/d", 42);
  a.receive([](const topic& t, const message&) { CHECK_EQUAL(t, "/foo/d"_t); });
  CHECK(a.mailbox().empty());
  CHECK(b.mailbox().empty());
  CHECK(c.mailbox().empty());
  CHECK(d.mailbox().empty());
  MESSAGE("A -> B -> C");
  a.publish("/bar/a", 42);
  c.receive([](const topic& t, const message&) { CHECK_EQUAL(t, "/bar/a"_t); });
  CHECK(a.mailbox().empty());
  CHECK(b.mailbox().empty());
  CHECK(c.mailbox().empty());
  CHECK(d.mailbox().empty());
}

TEST(subscribe before peering) {
  MESSAGE("spawning endpoints");
  context ctx;
  auto a = ctx.spawn<blocking>();
  auto b = ctx.spawn<blocking>();
  auto c = ctx.spawn<blocking>();
  auto d = ctx.spawn<blocking>();
  a.subscribe("/foo");
  c.subscribe("/bar");
  // A <-> B <-> C <-> D
  MESSAGE("chaining peers");
  a.peer(b);
  a.receive([](const status& s) { CHECK(s == peer_added); });
  b.receive([](const status& s) { CHECK(s == peer_added); });
  b.peer(c);
  b.receive([](const status& s) { CHECK(s == peer_added); });
  c.receive([](const status& s) { CHECK(s == peer_added); });
  c.peer(d);
  c.receive([](const status& s) { CHECK(s == peer_added); });
  d.receive([](const status& s) { CHECK(s == peer_added); });
  CHECK_EQUAL(a.peers().size(), 1u);
  CHECK_EQUAL(b.peers().size(), 2u);
  CHECK_EQUAL(c.peers().size(), 2u);
  CHECK_EQUAL(d.peers().size(), 1u);
  CHECK(a.mailbox().empty());
  CHECK(b.mailbox().empty());
  CHECK(c.mailbox().empty());
  CHECK(d.mailbox().empty());
  MESSAGE("D -> C -> B -> A");
  d.publish("/foo/d", 42);
  a.receive([](const topic& t, const message&) { CHECK_EQUAL(t, "/foo/d"_t); });
  CHECK(a.mailbox().empty());
  CHECK(b.mailbox().empty());
  CHECK(c.mailbox().empty());
  CHECK(d.mailbox().empty());
  MESSAGE("A -> B -> C");
  a.publish("/bar/a", 42);
  c.receive([](const topic& t, const message&) { CHECK_EQUAL(t, "/bar/a"_t); });
  CHECK(a.mailbox().empty());
  CHECK(b.mailbox().empty());
  CHECK(c.mailbox().empty());
  CHECK(d.mailbox().empty());
}
