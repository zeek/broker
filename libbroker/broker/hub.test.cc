#include "broker/hub.hh"

#include "broker/broker-test.test.hh"
#include "broker/builder.hh"

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
