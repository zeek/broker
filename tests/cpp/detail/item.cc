#define SUITE detail.item

#include "broker/detail/item.hh"

#include "test.hh"

#include <exception>

namespace broker::detail {

} // namespace broker::detail

using namespace broker;

namespace {

struct fixture {
  detail::item_allocator_ptr alloc;

  fixture() {
    alloc = detail::item_allocator::make();
  }
};

} // namespace

FIXTURE_SCOPE(item_tests, fixture)

TEST(items return to their stash when ref count reaches 0) {
  MESSAGE("given a stash with up to 8 items");
  auto stash = detail::item_stash::make(alloc, 8);
  auto available_after_stash_construction = alloc->available_items();
  CHECK_EQUAL(stash->available(), 8u);
  MESSAGE("when taking five items out of the stash");
  auto next_item = [&stash](auto&& t, auto&& d) {
    return stash->next_item(make_data_message(t, d), 1, nullptr);
  };
  std::vector<detail::item_ptr> items;
  for (int i = 0; i < 5; ++i)
    items.emplace_back(next_item(topic{"foo/bar"}, data{i}));
  CHECK_EQUAL(stash->available(), 3u);
  MESSAGE("then all items return to the stash once their ref count drops to 0");
  items.clear();
  CHECK_EQUAL(stash->available(), 8u);
  MESSAGE("and all items return to the allocator when destroying the stash");
  stash = nullptr;
  CHECK_EQUAL(alloc->available_items(), available_after_stash_construction + 8);
}

TEST(a stash throws when running out of available items) {
  auto stash = detail::item_stash::make(alloc, 8);
  auto next_item = [&stash](auto&& t, auto&& d) {
    return stash->next_item(make_data_message(t, d), 1, nullptr);
  };
  std::vector<detail::item_ptr> items;
  for (int i = 0; i < 8; ++i)
    items.emplace_back(next_item(topic{"foo/bar"}, data{i}));
  CHECK_EQUAL(stash->available(), 0u);
  std::exception_ptr eptr;
  try {
    items.emplace_back(next_item(topic{"you/cannot/pass"}, data{666}));
  } catch (...) {
    eptr = std::current_exception();
  }
  CHECK(items.size(), 8u);
  CHECK(eptr != nullptr);
}

FIXTURE_SCOPE_END()
