#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/detail/radix_tree.hh"
#include "broker/detail/trie.hh"
#include "broker/filter_type.hh"

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <limits>
#include <random>
#include <thread>

using vector_filter = std::vector<broker::topic>;

namespace broker::internal {

/// A filter based on a RADIX tree. The key is the always the string (i.e, the
/// topic) and the value is a "reference count" for the subscription.
using radix_filter = detail::radix_tree<int>;

/// Adds a reference to `x` to the filter.
/// @return `true` if we have added a new node, `false` otherwise.
bool filter_add_ref(radix_filter& tree, const topic& x) {
  auto [i, is_new] = tree.insert({x.string(), 1});
  if (!is_new)
    ++i->second;
  return is_new;
}

/// Convenience function for calling `filter_add_ref` with each topic in `xs`.
/// @return the number of added nodes.
size_t filter_add_ref(radix_filter& tree, const filter_type& xs) {
  size_t added = 0;
  for (auto& x : xs)
    if (filter_add_ref(tree, x))
      ++added;
  return added;
}

/// Convenience function for calling `filter_extend` with each topic in `other`
/// that matches `predicate`.
/// @return the number of added nodes.
template <class Predicate>
size_t filter_add_ref(radix_filter& tree, const filter_type& other,
                      Predicate predicate) {
  size_t added = 0;
  for (auto& x : other)
    if (predicate(x) && filter_add_ref(tree, x))
      ++added;
  return added > 0;
}

/// Removes a reference for `x` from the filter.
/// @return `true` if the filter has removed the topic entirely, `false`
/// otherwise.
bool filter_release_ref(radix_filter& tree, const topic& x) {
  auto i = tree.find(x.string());
  if (i == tree.end() || --i->second > 0)
    return false;
  tree.erase(x.string());
  return true;
}

/// Convenience function for calling `filter_release_ref` with each topic in
/// `xs`.
/// @return the number of removed nodes.
bool filter_release_ref(radix_filter& tree, const filter_type& xs) {
  size_t removed = 0;
  for (auto& x : xs)
    if (filter_release_ref(tree, x))
      ++removed;
  return removed;
}

/// Converts the radix-filter to a regular filter.
filter_type to_filter(const radix_filter& tree) {
  filter_type result;
  result.reserve(tree.size());
  for (auto& kvp : tree)
    result.emplace_back(kvp.first);
  std::sort(result.begin(), result.end());
  return result;
}

} // namespace broker::internal

std::string_view common_prefix(std::string_view lhs, std::string_view rhs) {
  auto len = std::min(lhs.size(), rhs.size());
  auto i = size_t{0};
  while (i < len && lhs[i] == rhs[i])
    ++i;
  return lhs.substr(0, i);
}

enum class extend_mode { nop, append, truncate };

extend_mode mode(vector_filter& f, const broker::topic& x) {
  for (auto& t : f) {
    if (t == x || t.prefix_of(x)) {
      // Filter already contains x or a less specific subscription.
      return extend_mode::nop;
    }
    if (x.prefix_of(t)) {
      // New topic is less specific than existing entries.
      return extend_mode::truncate;
    }
  }
  return extend_mode::append;
}

bool filter_extend_v1(vector_filter& f, const broker::topic& x) {
  switch (mode(f, x)) {
    case extend_mode::append: {
      f.emplace_back(x);
      std::sort(f.begin(), f.end());
      return true;
    }
    case extend_mode::truncate: {
      auto predicate = [&](const broker::topic& y) { return x.prefix_of(y); };
      f.erase(std::remove_if(f.begin(), f.end(), predicate), f.end());
      f.emplace_back(x);
      std::sort(f.begin(), f.end());
      return true;
    }
    default:
      return false;
  }
}

bool filter_extend_v2(vector_filter& f, const broker::topic& x) {
  switch (mode(f, x)) {
    case extend_mode::append: {
      auto i = std::lower_bound(f.begin(), f.end(), x);
      f.insert(i, x);
      return true;
    }
    case extend_mode::truncate: {
      auto predicate = [&](const broker::topic& y) { return x.prefix_of(y); };
      f.erase(std::remove_if(f.begin(), f.end(), predicate), f.end());
      auto i = std::lower_bound(f.begin(), f.end(), x);
      f.insert(i, x);
      return true;
    }
    default:
      return false;
  }
}

bool filter_match_v1(const vector_filter& filter, std::string_view t) noexcept {
  for (auto& prefix : filter)
    if (broker::topic::is_prefix(t, prefix.string()))
      return true;
  return false;
}

struct v2_pred {
  bool cmp(std::string_view lhs, std::string_view rhs) const noexcept {
    auto len = std::min(lhs.size(), rhs.size());
    return std::lexicographical_compare(lhs.begin(), lhs.begin() + len,
                                        rhs.begin(), rhs.begin() + len);
  }
  bool operator()(const broker::topic& lhs,
                  std::string_view rhs) const noexcept {
    return cmp(lhs.string(), rhs);
  }
  bool operator()(std::string_view lhs,
                  const broker::topic& rhs) const noexcept {
    return cmp(lhs, rhs.string());
  }
};

bool filter_match_v2(const vector_filter& filter, std::string_view t) noexcept {
  return std::binary_search(filter.begin(), filter.end(), t, v2_pred{});
}

class generator {
public:
  generator(unsigned seed)
    : rng_(seed), len_(10, 80), char_(33, 125), index_(0, 99) {}

  std::string next_string() {
    std::string result;
    result.resize(len_(rng_));
    for (auto& c : result)
      c = static_cast<char>(char_(rng_));
    return result;
  }

  size_t next_index() {
    return index_(rng_);
  }

private:
  std::minstd_rand rng_;

  // Generates a random string length between 10 and 80 characters.
  std::uniform_int_distribution<size_t> len_;

  // Generates a random printable ASCII character.
  std::uniform_int_distribution<> char_;

  // Generates a random index in [0, 99].
  std::uniform_int_distribution<size_t> index_;
};

class filter : public benchmark::Fixture {
public:
  filter() {
    auto gen = generator{0x3eec};
    for (size_t i = 0; i < 100; ++i)
      topics.emplace_back(gen.next_string());
    // Have 10 topics that we query later on, half of them are in the filter and
    // the other half are not.
    for (size_t i = 0; i < 5; ++i) {
      needles.emplace_back(topics[gen.next_index()].string());
      needles.emplace_back(gen.next_string());
    }
    for (const auto& topic : topics) {
      filter_extend_v1(vec_filter, topic);
      broker::internal::filter_add_ref(rx_filter, topic);
      trie_filter.insert(topic.string());
    }
    // Sanity check: filters must yield the same results.
    for (const auto& needle : needles) {
      auto res1 = filter_match_v1(vec_filter, needle);
      auto res2 = filter_match_v2(vec_filter, needle);
      auto res3 = rx_filter.is_prefix(needle);
      auto res4 = trie_filter.has_prefix(needle);
      if (res1 != res2 || res1 != res3 || res1 != res4)
        fprintf(stderr, "mismatch for %s\n", needle.c_str());
    }
  }

  std::vector<broker::topic> topics;
  std::vector<std::string> needles;
  vector_filter vec_filter;
  broker::internal::radix_filter rx_filter;
  broker::detail::trie trie_filter;
};

// -- insertion ----------------------------------------------------------------

BENCHMARK_DEFINE_F(filter, add_entries_vector_v1)(benchmark::State& state) {
  for (auto _ : state) {
    vector_filter filter;
    filter.reserve(topics.size());
    for (const auto& topic : topics)
      filter_extend_v1(filter, topic);
    benchmark::DoNotOptimize(filter);
  }
}

BENCHMARK_REGISTER_F(filter, add_entries_vector_v1);

BENCHMARK_DEFINE_F(filter, add_entries_vector_v2)(benchmark::State& state) {
  for (auto _ : state) {
    vector_filter filter;
    filter.reserve(topics.size());
    for (const auto& topic : topics)
      filter_extend_v2(filter, topic);
    benchmark::DoNotOptimize(filter);
  }
}

BENCHMARK_REGISTER_F(filter, add_entries_vector_v2);

BENCHMARK_DEFINE_F(filter, add_entries_radix_tree)(benchmark::State& state) {
  for (auto _ : state) {
    broker::internal::radix_filter filter;
    for (const auto& topic : topics)
      broker::internal::filter_add_ref(filter, topic);
    benchmark::DoNotOptimize(filter);
  }
}

BENCHMARK_REGISTER_F(filter, add_entries_radix_tree);

BENCHMARK_DEFINE_F(filter,
                   add_entries_segmented_trie)(benchmark::State& state) {
  for (auto _ : state) {
    broker::detail::trie filter;
    for (const auto& topic : topics)
      filter.insert(topic.string());
    benchmark::DoNotOptimize(filter);
  }
}

BENCHMARK_REGISTER_F(filter, add_entries_segmented_trie);

// -- lookup -------------------------------------------------------------------

BENCHMARK_DEFINE_F(filter, match_vector_v1)(benchmark::State& state) {
  for (auto _ : state) {
    for (const auto& needle : needles) {
      auto matches = filter_match_v1(vec_filter, needle);
      benchmark::DoNotOptimize(matches);
    }
  }
}

BENCHMARK_REGISTER_F(filter, match_vector_v1);

BENCHMARK_DEFINE_F(filter, match_vector_v2)(benchmark::State& state) {
  for (auto _ : state) {
    for (const auto& needle : needles) {
      auto matches = filter_match_v2(vec_filter, needle);
      benchmark::DoNotOptimize(matches);
    }
  }
}

BENCHMARK_REGISTER_F(filter, match_vector_v2);

BENCHMARK_DEFINE_F(filter, match_radix_tree)(benchmark::State& state) {
  for (auto _ : state) {
    for (const auto& needle : needles) {
      auto matches = rx_filter.is_prefix(needle);
      benchmark::DoNotOptimize(matches);
    }
  }
}

BENCHMARK_REGISTER_F(filter, match_radix_tree);

BENCHMARK_DEFINE_F(filter, match_segmented_trie)(benchmark::State& state) {
  for (auto _ : state) {
    for (const auto& needle : needles) {
      auto matches = trie_filter.has_prefix(needle);
      benchmark::DoNotOptimize(matches);
    }
  }
}

BENCHMARK_REGISTER_F(filter, match_segmented_trie);
