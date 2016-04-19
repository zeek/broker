#include "broker/broker.hh"

#define SUITE endpoint
#include "test.hpp"

using namespace broker;

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
  CHECK(e.empty()); // No message in the mailbox.
}

TEST(blocking subscription) {
  context ctx;
  auto e = ctx.spawn<blocking>();
  e.subscribe("/foo");
  e.publish("/foo/data", 42);
  e.receive([](const topic& t, const message& msg) {
    CHECK_EQUAL(t, "/foo/data"_t);
    CHECK_EQUAL(msg.get_as<int>(0), 42);
  });
}

TEST(nonblocking subscription) {
  auto counter = std::make_shared<int>(0);
  // The destructor of the context object blocks until all actors have
  // terminated. Hence the new scope.
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

TEST(non-lambda blocking receive) {
  context ctx;
  auto e = ctx.spawn<blocking>();
  e.subscribe("/foo");
  auto m0 = make_message("broker");
  e.publish("/foo", m0);
  auto msg = e.receive();
  REQUIRE_EQUAL(msg.size(), 2u); // <topic, message>
  auto t = msg.get_as<topic>(0);
  CHECK_EQUAL(t, "/foo"_t);
  auto m1 = msg.get_as<message>(1);
  REQUIRE_EQUAL(m1.size(), 1u);
  CHECK_EQUAL(m1.get_as<std::string>(0), "broker");
  CHECK(e.empty());
}

TEST(multi-topic subscription) {
  context ctx;
  auto e = ctx.spawn<blocking>();
  e.subscribe("/foo");
  e.subscribe("/foo/bar");
  e.subscribe("/foo/bar/baz");
  e.publish("/foo/bar/baz", 4.2);
  e.receive(); // Block and wait until the next message, then discard it.
  CHECK(e.empty()); // We should have received exactly one message.
}

TEST(local peering) {
  context ctx;
  auto x = ctx.spawn<blocking>();
  auto y = ctx.spawn<blocking>();
  CHECK_EQUAL(x.peers().size(), 0u);
  x.peer(y);
  CHECK_EQUAL(x.peers().size(), 1u);
  // TODO: messaging
}

TEST(remote peering) {
  // TODO
}
