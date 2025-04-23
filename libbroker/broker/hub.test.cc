#include "broker/hub.hh"

#include "broker/broker-test.test.hh"
#include "broker/builder.hh"
#include "broker/publisher.hh"

#include <chrono>

using namespace std::literals;

TEST("hubs do not receive their own messages") {
  broker::endpoint ep;
  auto uut = ep.make_hub({"/foo/bar"});
  uut.publish("/foo/bar", broker::list_builder{}.add(1).add(2).add(3));
  auto msg = uut.get(150ms);
  CHECK_EQ(msg, nullptr);
}

TEST("hubs receive messages from other hubs") {
  broker::endpoint ep;
  auto uut1 = ep.make_hub({"/foo/bar"});
  auto uut2 = ep.make_hub({"/foo/bar"});
  uut1.publish("/foo/bar", broker::list_builder{}.add(1).add(2).add(3));
  auto msg = uut2.get(150ms);
  if (CHECK_NE(msg, nullptr)) {
    CHECK_EQ(msg->topic(), "/foo/bar");
    if (auto val = msg->value(); CHECK(val.is_list())) {
      CHECK_EQ(broker::to_string(val), "(1, 2, 3)");
    }
  }
}

TEST("hubs can change their subscriptions") {
  broker::endpoint ep;
  auto uut1 = ep.make_hub({"/foo/baz"});
  auto uut2 = ep.make_hub({"/foo/bar"});
  // Send to /foo/baz: should not be received by uut2.
  uut1.publish("/foo/baz", broker::list_builder{}.add(1).add(2).add(3));
  if (auto msg = uut2.get(150ms); !CHECK_EQ(msg, nullptr)) {
    return;
  }
  // Subscribe to /foo/baz on uut2: should receive the message.
  uut2.subscribe("/foo/baz", true);
  uut1.publish("/foo/baz", broker::list_builder{}.add(4).add(5).add(6));
  if (auto msg = uut2.get(150ms); CHECK_NE(msg, nullptr)) {
    CHECK_EQ(msg->topic(), "/foo/baz");
    if (auto val = msg->value(); CHECK(val.is_list())) {
      CHECK_EQ(broker::to_string(val), "(4, 5, 6)");
    }
  }
  // Unsubscribe from /foo/baz on uut2: should not receive the message anymore.
  uut2.unsubscribe("/foo/baz", true);
  uut1.publish("/foo/baz", broker::list_builder{}.add(7).add(8).add(9));
  if (auto msg = uut2.get(150ms); !CHECK_EQ(msg, nullptr)) {
    return;
  }
}

TEST("hubs receive messages from publishers") {
  broker::endpoint ep;
  auto uut = ep.make_hub({"/foo/bar"});
  auto pub = ep.make_publisher(broker::topic{"/foo/bar"});
  pub.publish(broker::list_builder{}.add(1).add(2).add(3));
  auto msg = uut.get(150ms);
  if (CHECK_NE(msg, nullptr)) {
    CHECK_EQ(msg->topic(), "/foo/bar");
    if (auto val = msg->value(); CHECK(val.is_list())) {
      CHECK_EQ(broker::to_string(val), "(1, 2, 3)");
    }
  }
}

TEST("hubs receive messages that are published on the endpoint") {
  broker::endpoint ep;
  auto uut = ep.make_hub({"/foo/bar"});
  ep.publish(broker::topic{"/foo/bar"},
             broker::list_builder{}.add(1).add(2).add(3).build());
  if (auto msg = uut.get(150ms); CHECK_NE(msg, nullptr)) {
    CHECK_EQ(msg->topic(), "/foo/bar");
    if (auto val = msg->value(); CHECK(val.is_list())) {
      CHECK_EQ(broker::to_string(val), "(1, 2, 3)");
    }
  }
}

TEST("subscribers receive messages from hubs but not from publishers") {
  broker::endpoint ep;
  auto uut = ep.make_hub({"/foo/bar"});
  auto sub = ep.make_subscriber({"/foo/bar"});
  uut.publish("/foo/bar", broker::list_builder{}.add(1).add(2).add(3));
  if (auto msg = sub.get(150ms); CHECK(msg.has_value())) {
    CHECK_EQ((*msg)->topic(), "/foo/bar");
    if (auto val = (*msg)->value(); CHECK(val.is_list())) {
      CHECK_EQ(broker::to_string(val), "(1, 2, 3)");
    }
  }
  auto pub = ep.make_publisher(broker::topic{"/foo/bar"});
  pub.publish(broker::list_builder{}.add(4).add(5).add(6));
  if (auto msg = sub.get(150ms); !CHECK(!msg.has_value())) {
    MESSAGE("unexpected message: " << broker::to_string((*msg)->value()));
  }
}
