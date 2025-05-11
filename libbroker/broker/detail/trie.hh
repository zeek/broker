#pragma once

#include <memory>
#include <string>
#include <string_view>

namespace broker::detail {

class trie {
public:
  // -- member types -----------------------------------------------------------

  struct node_or_leaf {
    virtual ~node_or_leaf() noexcept;
    virtual bool is_leaf() const noexcept = 0;
  };

  struct node : node_or_leaf {
    std::unique_ptr<node_or_leaf> children[96];

    bool is_leaf() const noexcept override;
    bool has_children() const noexcept;
  };

  struct leaf : node_or_leaf {
    std::string key;
    int val;
    bool is_leaf() const noexcept override;

    explicit leaf(std::string str) : key(std::move(str)), val(1) {}
  };

  /// Inserts a key into the trie.
  void insert(std::string_view key);

  /// Erases a key from the trie.
  bool erase(std::string_view key);

  /// Returns whether the trie contains a key.
  bool contains(std::string_view key) const;

  /// Returns whether the trie contains at least one key for the given prefix.
  bool has_prefix(std::string_view prefix) const;

  /// Returns whether the trie is empty.
  bool empty() const noexcept {
    return !root_.has_children();
  }

private:
  node root_;
};

} // namespace broker::detail
