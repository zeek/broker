#pragma once

#include "broker/fwd.hh"

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace broker::detail {

/// A custom trie implementation optimized for use as a filter in Broker. The
/// trie uses segments to avoid wasting memory. Since keys are typically ASCII
/// data, only a few segments are usually in use.
class trie {
public:
  // -- constants --------------------------------------------------------------

  /// The number of nodes in each segment. Segments are allocated lazily in
  /// order to avoid wasting memory.
  static constexpr size_t segment_size = 32;

  static_assert(256 % segment_size == 0,
                "segment_size must be a divisor of 256");

  /// The number of segments in a node.
  static constexpr size_t num_segments = 256 / segment_size;

  // -- member types -----------------------------------------------------------

  struct node;

  /// An array of nodes.
  struct segment {
    std::unique_ptr<node> children[segment_size];

    bool has_children() const noexcept;
  };

  /// A node in the trie.
  struct node {
    node() = default;

    node(std::string_view str, bool is_compressed)
      : val(1), compressed(is_compressed) {
      key(str);
    }

    node(const node&) = delete;

    node& operator=(const node&) = delete;

    /// An array of segments. Allocated lazily.
    std::unique_ptr<segment> segments[num_segments];

    /// The key data. Allocated lazily.
    std::unique_ptr<char[]> key_data;

    /// The length of the key, if available.
    size_t key_len;

    /// The value of the node, i.e., the number of times the key has been
    /// inserted.
    int val = 0;

    /// Stores whether this node contains the full key even if their position in
    /// the trie is not the end of the key. A compressed node cannot have
    /// children.
    bool compressed = false;

    /// Sets the key data and the length of the key.
    void key(std::string_view str) {
      key_data = std::make_unique<char[]>(str.size() + 1);
      std::copy(str.begin(), str.end(), key_data.get());
      key_data[str.size()] = '\0';
      key_len = str.size();
    }

    /// Returns the key as a string view.
    std::string_view key() const noexcept {
      return {key_data.get(), key_len};
    }

    /// Returns whether this node has any children.
    bool has_children() const noexcept;

    /// Returns a reference to the node at the given key. If the segment
    /// containing that key is not yet allocated, it will be allocated.
    std::unique_ptr<node>& ref(char key);

    /// Returns a pointer to the node at the given key.
    const node* at(char key) const noexcept;

    /// Returns a pointer to the node at the given key.
    node* at(char key) noexcept;

    /// Erases the node at the given key.
    void erase(char key);

    /// Splits the key into the segment index and the child index.
    static constexpr auto indexes(char key) noexcept {
      auto index = static_cast<size_t>(static_cast<unsigned char>(key));
      return std::pair{index / segment_size, index % segment_size};
    }
  };

  trie() = default;

  trie(const trie&) = delete;

  trie& operator=(const trie&) = delete;

  trie(trie&& other) noexcept;

  trie& operator=(trie&& other) noexcept;

  /// Inserts a key into the trie.
  int insert(std::string_view key);

  /// Inserts all keys from `other` into this trie.
  void insert_from(const trie& other);

  /// Erases a key from the trie.
  bool erase(std::string_view key);

  /// Returns whether the trie contains a key.
  bool contains(std::string_view key) const;

  /// Returns whether the trie contains at least one key for the given prefix.
  bool has_prefix(std::string_view prefix) const;

  /// Calls the given function for each key in the trie.
  template <class F>
  void for_each(F&& f) const {
    do_for_each(&root_, f);
  }

  /// Returns all keys in the trie.
  std::vector<std::string> keys() const;

  /// Returns the number of keys in the trie.
  size_t size() const noexcept;

  /// Checks whether the trie is empty, i.e., has no keys.
  size_t empty() const noexcept {
    return size() == 0;
  }

  /// Returns the value of the key, i.e., the number of times the key has been
  /// inserted.
  int value(std::string_view key) const noexcept;

  /// Returns a pointer to the root node.
  const node* root() const noexcept {
    return &root_;
  }

  /// Copies the contents of `other` into this trie.
  void copy_from(const trie& other);

  /// Clears the trie.
  void clear() noexcept;

  /// Overrides the contents of the trie with the given topics.
  void assign(const filter_type& from);

private:
  template <class F>
  void do_for_each(const node* pos, F& f) const {
    if (pos->val > 0 && pos->key_data) {
      f(pos->key());
    }
    // Traverse all children
    for (auto& seg : pos->segments) {
      if (seg) {
        for (auto& child : seg->children) {
          if (child) {
            do_for_each(child.get(), f);
          }
        }
      }
    }
  }

  node* emplace(node* start, std::string_view path);

  node root_;
};

} // namespace broker::detail
