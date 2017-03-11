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

TEST(default construction) {
  endpoint{};
  blocking_endpoint{};
  nonblocking_endpoint{};
}

TEST(blocking construction) {
  context ctx;
  auto b = ctx.spawn<blocking>();
  endpoint e = b; // upcast
}

TEST(nonblocking construction) {
  context ctx;
  auto n = ctx.spawn<nonblocking>();
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

TEST(nonblocking subscription managment) {
  auto counter = std::make_shared<int>(0);
  context ctx;
  auto ep = ctx.spawn<nonblocking>();
  auto callback = [=](const topic&, const data&) { ++*counter; };
  MESSAGE("subscribing");
  ep.subscribe("/foo", callback);
  // After subscribe() returns, calls to publish() are guaranteed to execute
  // the provided callback.
  ep.publish("/foo/bar", 42);
  ep.publish("/foo/baz", 4.2);
  ep.publish("/foo/qux", "broker");
  // Wait for messages to arrive at the callback actor.
  std::this_thread::sleep_for(milliseconds{100});
  CHECK_EQUAL(*counter, 3);
  *counter = 0;
  MESSAGE("replacing");
  ep.subscribe("/foo", callback);
  ep.publish("/foo", "");
  // Wait for messages to arrive at the callback actor.
  std::this_thread::sleep_for(milliseconds{100});
  CHECK_EQUAL(*counter, 1);
  *counter = 0;
  MESSAGE("unsubscribing");
  ep.unsubscribe("/foo");
  // After unsubscribe() returns, calls to publish() are guaranteed to
  // no longer execute the previously installed callback.
  ep.publish("/foo/bar", 42);
  ep.publish("/foo/baz", 4.2);
  ep.publish("/foo/qux", "broker");
  CHECK_EQUAL(*counter, 0);
}

TEST(blocking message receive) {
  context ctx;
  auto e = ctx.spawn<blocking>();
  e.subscribe("/foo");
  e.publish("/foo", "broker");
  CHECK(is_ready(e, seconds{1}));
  auto elem = e.receive();
  auto msg = get_if<message>(elem);
  REQUIRE(msg);
  CHECK_EQUAL(msg->topic(), "/foo"_t);
  auto str = get_if<std::string>(msg->data());
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

TEST(blocking local peering and unpeering) {
  context ctx;
  auto x = ctx.spawn<blocking>();
  auto y = ctx.spawn<blocking>();
  MESSAGE("peer endpoints");
  x.peer(y);
  CHECK(get<status>(x.receive()) == sc::peer_added);
  CHECK(get<status>(y.receive()) == sc::peer_added);
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
  CHECK(get<status>(x.receive()) == sc::peer_removed);
  CHECK(get<status>(y.receive()) == sc::peer_removed);
  CHECK_EQUAL(x.peers().size(), 0u);
  CHECK_EQUAL(y.peers().size(), 0u);
  MESSAGE("attempt to unpeer from already unpeered endpoint");
  y.unpeer(x);
  CHECK(get<error>(y.receive()) == ec::peer_invalid);
}

TEST(nonblocking local peering) {
  context ctx;
  auto x = ctx.spawn<nonblocking>();
  auto y = ctx.spawn<nonblocking>();
  MESSAGE("adding status callback");
  x.subscribe([](const status& s) { CHECK(s == sc::peer_added); });
  y.subscribe([](const status& s) { CHECK(s == sc::peer_added); });
  x.peer(y);
  // Wait for status messages to trickle in.
  std::this_thread::sleep_for(milliseconds{100});
  MESSAGE("replacing status callback");
  x.subscribe([](const status& s) { CHECK(s == sc::peer_removed); });
  y.subscribe([](const status& s) { CHECK(s == sc::peer_removed); });
  x.unpeer(y);
  // Wait for status messages to trickle in.
  std::this_thread::sleep_for(milliseconds{100});
}

TEST(remote peering) {
  context ctx;
  auto x = ctx.spawn<blocking>();
  auto y = ctx.spawn<blocking>();
  auto bound_port = y.listen("127.0.0.1");
  REQUIRE(bound_port > 0);
  x.peer("127.0.0.1", bound_port);
  CHECK(get<status>(x.receive()) == sc::peer_added);
  CHECK(get<status>(y.receive()) == sc::peer_added);
  CHECK_EQUAL(x.peers().size(), 1u);
  CHECK_EQUAL(y.peers().size(), 1u);
}

TEST(local peering termination) {
  context ctx;
  auto x = ctx.spawn<blocking>();
  {
    auto y = ctx.spawn<blocking>();
    x.peer(y);
    CHECK(get<status>(x.receive()) == sc::peer_added);
    CHECK(get<status>(y.receive()) == sc::peer_added);
    CHECK_EQUAL(x.peers().size(), 1u);
    CHECK_EQUAL(y.peers().size(), 1u);
  }
  // We cannot re-establish a connection to a local endpoint. If it terminates,
  // the peering is gone.
  CHECK(get<status>(x.receive()) == sc::peer_removed);
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
    CHECK(get<status>(x.receive()) == sc::peer_added);
    CHECK(get<status>(y.receive()) == sc::peer_added);
  }
  // When losing a connection to remote endpoint, we still keep that peer
  // around until the peering originator removes it.
  auto e = x.receive();
  auto& s = get<status>(e);
  CHECK(s == sc::peer_lost);
  auto ep = s.context<endpoint_info>();
  REQUIRE(ep);
  REQUIRE(ep->network);
  CHECK_EQUAL(ep->network->address, "127.0.0.1");
  CHECK_EQUAL(ep->network->port, bound_port);
  CHECK_EQUAL(x.peers().size(), 1u);
  // Now remove the peer.
  x.unpeer("127.0.0.1", bound_port);
  CHECK(get<status>(x.receive()) == sc::peer_removed);
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
  CHECK(get<status>(a.receive()) == sc::peer_added);
  CHECK(get<status>(b.receive()) == sc::peer_added);
  b.peer(c);
  CHECK(get<status>(b.receive()) == sc::peer_added);
  CHECK(get<status>(c.receive()) == sc::peer_added);
  c.peer(d);
  CHECK(get<status>(c.receive()) == sc::peer_added);
  CHECK(get<status>(d.receive()) == sc::peer_added);
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
  CHECK_EQUAL(get<message>(a.receive()).topic(), "/foo/d"_t);
  CHECK(a.mailbox().empty());
  CHECK(b.mailbox().empty());
  CHECK(c.mailbox().empty());
  CHECK(d.mailbox().empty());
  MESSAGE("A -> B -> C");
  a.publish("/bar/a", 42);
  CHECK_EQUAL(get<message>(c.receive()).topic(), "/bar/a"_t);
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
  CHECK(get<status>(a.receive()) == sc::peer_added);
  CHECK(get<status>(b.receive()) == sc::peer_added);
  b.peer(c);
  CHECK(get<status>(b.receive()) == sc::peer_added);
  CHECK(get<status>(c.receive()) == sc::peer_added);
  c.peer(d);
  CHECK(get<status>(c.receive()) == sc::peer_added);
  CHECK(get<status>(d.receive()) == sc::peer_added);
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
  CHECK_EQUAL(get<message>(a.receive()).topic(), "/foo/d"_t);
  CHECK(a.mailbox().empty());
  CHECK(b.mailbox().empty());
  CHECK(c.mailbox().empty());
  CHECK(d.mailbox().empty());
  MESSAGE("A -> B -> C");
  a.publish("/bar/a", 42);
  CHECK_EQUAL(get<message>(c.receive()).topic(), "/bar/a"_t);
  CHECK(a.mailbox().empty());
  CHECK(b.mailbox().empty());
  CHECK(c.mailbox().empty());
  CHECK(d.mailbox().empty());
}
