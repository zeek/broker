#include "broker/internal/radix_filter.hh"

#include "broker/broker-test.test.hh"

using namespace broker::internal;

namespace {

template <class... Ts>
broker::filter_type ls(Ts&&... xs) {
  broker::filter_type result;
  (result.emplace_back(xs), ...);
  std::sort(result.begin(), result.end());
  return result;
}

} // namespace

TEST(a radix filter counts references to topics) {
  radix_filter tree;
  auto my_topic = "/my/topic";
  MESSAGE("when adding an item four times");
  CHECK(filter_add_ref(tree, my_topic));
  CHECK(!filter_add_ref(tree, my_topic));
  CHECK(!filter_add_ref(tree, my_topic));
  CHECK(!filter_add_ref(tree, my_topic));
  CHECK_EQUAL(tree.size(), 1);
  CHECK_EQUAL(to_filter(tree), ls("/my/topic"));
  MESSAGE("then it's deleted after calling filter_release_ref four times");
  CHECK(!filter_release_ref(tree, my_topic));
  CHECK(!filter_release_ref(tree, my_topic));
  CHECK(!filter_release_ref(tree, my_topic));
  CHECK(filter_release_ref(tree, my_topic));
  CHECK(tree.empty());
}

TEST(trees track nested topics individually) {
  radix_filter tree;
  auto my_topic = "/my/topic";
  auto my_topic_part1 = "/my/topic/part1";
  auto my_topic_part1_detail = "/my/topic/part1/detail";
  auto my_other_topic = "/my/other/topic";
  MESSAGE("when adding nested topics");
  CHECK(filter_add_ref(tree, my_topic));
  CHECK(filter_add_ref(tree, my_topic_part1));
  CHECK(filter_add_ref(tree, my_topic_part1_detail));
  CHECK(filter_add_ref(tree, my_other_topic));
  CHECK_EQUAL(tree.size(), 4);
  CHECK_EQUAL(to_filter(tree), ls("/my/topic", "/my/topic/part1",
                                  "/my/topic/part1/detail", "/my/other/topic"));
  MESSAGE("then removing parent nodes does not delete children");
  CHECK(filter_release_ref(tree, my_topic));
  CHECK_EQUAL(tree.size(), 3);
  CHECK_EQUAL(to_filter(tree), ls("/my/topic/part1", "/my/topic/part1/detail",
                                  "/my/other/topic"));
}
