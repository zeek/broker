#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/detail/trie.hh"
#include "broker/filter_type.hh"
#include "broker/internal/radix_filter.hh"

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <limits>
#include <random>
#include <thread>

using vector_filter = std::vector<broker::topic>;

std::string_view common_prefix(std::string_view lhs, std::string_view rhs) {
  auto len = std::min(lhs.size(), rhs.size());
  auto i = size_t{0};
  while (i < len && lhs[i] == rhs[i])
    ++i;
  return lhs.substr(0, i);
}

class my_trie {
public:
  // -- member types -----------------------------------------------------------

  struct node_or_leaf {
    virtual ~node_or_leaf() noexcept {}
    virtual bool is_leaf() const noexcept = 0;
  };

  struct node : node_or_leaf {
    std::unique_ptr<node_or_leaf> children[256];

    bool is_leaf() const noexcept override {
      return false;
    }
    bool has_children() const noexcept;
  };

  struct leaf : node_or_leaf {
    std::string key;
    int val;
    bool is_leaf() const noexcept override {
      return true;
    }

    explicit leaf(std::string str) : key(std::move(str)), val(1) {}
  };

  /// Inserts a key into the trie.
  void insert(std::string_view str) {
    auto* ptr = &root_;
    auto pos = size_t{0};
    for (auto c : str) {
      auto index = static_cast<size_t>(c);
      if (!ptr->children[index]) {
        // std::cout<<"new leaf: "<<str<<std::endl;
        ptr->children[index].reset(new leaf(std::string{str}));
        return;
      }
      auto* next = ptr->children[index].get();
      if (next->is_leaf()) {
        // Increase count for existing leaf if it matches the key.
        auto* lptr = static_cast<leaf*>(next);
        if (lptr->key == str) {
          lptr->val += 1;
          return;
        }
        // Take ownership of the leaf.
        ptr->children[index].release();
        // Insert nodes for the common prefix.
        auto common = common_prefix(str.substr(pos),
                                    std::string_view{lptr->key}.substr(pos));
        for (auto ch : common) {
          auto* new_node = new node;
          auto index = static_cast<size_t>(ch);
          ptr->children[index].reset(new_node);
          ptr = new_node;
        }
        // std::cout<<"common: "<<common<<std::endl;
        //  Insert the rest of the new string.
        if (str.size() > pos + common.size()) {
          // std::cout<<"str: "<<str<<std::endl;
          auto* new_leaf = new leaf(std::string{str});
          auto i = static_cast<size_t>(str[pos + common.size()]);
          ptr->children[i].reset(new_leaf);
        }
        // Re-insert the old leaf unless it was a prefix of the new string.
        if (lptr->key.size() <= pos + common.size()) {
          delete lptr;
        } else {
          // std::cout<<"old: "<<lptr->key<<std::endl;
          auto i = static_cast<size_t>(lptr->key[pos + common.size()]);
          ptr->children[i].reset(lptr);
        }
        return;
      }
      ptr = static_cast<node*>(next);
      ++pos;
    }
  }

  /// Erases a key from the trie.
  bool erase(std::string_view key) {
    std::vector<std::pair<node*, char>> path;
    auto* n = &root_;
    for (auto c : key) {
      auto index = static_cast<size_t>(c);
      if (!n->children[index])
        return false;
      auto* child = n->children[index].get();
      if (child->is_leaf()) {
        auto* lptr = static_cast<leaf*>(child);
        if (lptr->key != key)
          return false;
        if (lptr->val > 1) {
          --lptr->val;
          return false;
        }
        n->children[index].reset();
        while (!n->has_children() && !path.empty()) {
          auto [parent, ch] = path.back();
          path.pop_back();
          parent->children[static_cast<size_t>(ch)].reset();
          n = parent;
        }
        return true;
      }
      path.emplace_back(n, c);
      n = static_cast<node*>(child);
    }
    return false;
  }

  /// Returns whether the trie contains a key.
  bool contains(std::string_view str) const {
    auto* n = &root_;
    for (auto c : str) {
      auto i = static_cast<size_t>(c);
      if (!n->children[i])
        return false;
      auto* child = n->children[i].get();
      if (child->is_leaf()) {
        auto* lptr = static_cast<leaf*>(child);
        return lptr->key == str;
      }
      n = static_cast<node*>(child);
    }
    return false;
  }

  /// Returns whether the trie contains at least one key for the given prefix.
  bool has_prefix(std::string_view str) const {
    auto* n = &root_;
    for (auto c : str) {
      auto i = static_cast<size_t>(c);
      if (!n->children[i])
        return false;
      auto* child = n->children[i].get();
      if (child->is_leaf()) {
        auto* lptr = static_cast<leaf*>(child);
        return lptr->key.compare(0, str.size(), str) == 0;
      }
      n = static_cast<node*>(child);
    }
    return true;
  }

private:
  node root_;
};

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
      trie2_filter.insert(topic.string());
    }
    // Sanity check: filters must yield the same results.
    for (const auto& needle : needles) {
      auto res1 = filter_match_v1(vec_filter, needle);
      auto res2 = filter_match_v2(vec_filter, needle);
      auto res3 = rx_filter.is_prefix(needle);
      auto res4 = trie_filter.has_prefix(needle);
      auto res5 = trie2_filter.has_prefix(needle);
      if (res1 != res2 || res1 != res3 || res1 != res4 || res1 != res5)
        fprintf(stderr, "mismatch for %s\n", needle.c_str());
    }
  }

  std::vector<broker::topic> topics;
  std::vector<std::string> needles;
  vector_filter vec_filter;
  broker::internal::radix_filter rx_filter;
  broker::detail::trie trie_filter;
  my_trie trie2_filter;
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

BENCHMARK_DEFINE_F(filter, add_entries_trie96)(benchmark::State& state) {
  for (auto _ : state) {
    broker::detail::trie filter;
    for (const auto& topic : topics)
      filter.insert(topic.string());
    benchmark::DoNotOptimize(filter);
  }
}

BENCHMARK_REGISTER_F(filter, add_entries_trie96);

BENCHMARK_DEFINE_F(filter, add_entries_trie256)(benchmark::State& state) {
  for (auto _ : state) {
    my_trie filter;
    for (const auto& topic : topics)
      filter.insert(topic.string());
    benchmark::DoNotOptimize(filter);
  }
}

BENCHMARK_REGISTER_F(filter, add_entries_trie256);

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

BENCHMARK_DEFINE_F(filter, match_trie96)(benchmark::State& state) {
  for (auto _ : state) {
    for (const auto& needle : needles) {
      auto matches = trie_filter.has_prefix(needle);
      benchmark::DoNotOptimize(matches);
    }
  }
}

BENCHMARK_REGISTER_F(filter, match_trie96);

BENCHMARK_DEFINE_F(filter, match_trie256)(benchmark::State& state) {
  for (auto _ : state) {
    for (const auto& needle : needles) {
      auto matches = trie2_filter.has_prefix(needle);
      benchmark::DoNotOptimize(matches);
    }
  }
}

BENCHMARK_REGISTER_F(filter, match_trie256);
