#include "broker/detail/trie.hh"

#include "broker/detail/assert.hh"

#include <stdexcept>
#include <vector>

namespace broker::detail {

namespace {

std::string_view common_prefix(std::string_view lhs, std::string_view rhs) {
  auto len = std::min(lhs.size(), rhs.size());
  auto index = size_t{0};
  while (index < len && lhs[index] == rhs[index]) {
    ++index;
  }
  return lhs.substr(0, index);
}

} // namespace

bool trie::segment::has_children() const noexcept {
  return std::any_of(std::begin(children), std::end(children),
                     [](const auto& x) { return x != nullptr; });
}

bool trie::node::has_children() const noexcept {
  return std::any_of(std::begin(segments), std::end(segments),
                     [](const auto& ptr) {
                       return ptr && ptr->has_children();
                     });
}

std::unique_ptr<trie::node>& trie::node::ref(char key) {
  assert(!compressed);
  auto [segment_index, child_index] = indexes(key);
  auto& ptr = segments[segment_index];
  if (!ptr) {
    ptr = std::make_unique<segment>();
  }
  return ptr->children[child_index];
}

const trie::node* trie::node::at(char key) const noexcept {
  auto [segment_index, child_index] = indexes(key);
  auto& ptr = segments[segment_index];
  if (!ptr) {
    return nullptr;
  }
  return ptr->children[child_index].get();
}

trie::node* trie::node::at(char key) noexcept {
  auto [segment_index, child_index] = indexes(key);
  auto& ptr = segments[segment_index];
  if (!ptr) {
    return nullptr;
  }
  return ptr->children[child_index].get();
}

void trie::node::erase(char key) {
  auto [segment_index, child_index] = indexes(key);
  auto& ptr = segments[segment_index];
  if (!ptr) {
    return;
  }
  ptr->children[child_index].reset();
  if (!ptr->has_children()) {
    ptr.reset();
  }
}

trie::trie(trie&& other) noexcept {
  for (size_t index = 0; index < num_segments; ++index) {
    root_.segments[index].swap(other.root_.segments[index]);
  }
}

trie& trie::operator=(trie&& other) noexcept {
  if (this != &other) {
    for (size_t index = 0; index < num_segments; ++index) {
      root_.segments[index].swap(other.root_.segments[index]);
    }
  }
  return *this;
}

int trie::insert(std::string_view str) {
  if (str.empty()) {
    return 0;
  }
  auto* ptr = &root_;
  auto depth = size_t{0};
  for (auto c : str) {
    auto& child = ptr->ref(c);
    if (!child) {
      auto at_end = depth + 1 == str.size();
      child = std::make_unique<node>(str, !at_end);
      return 1;
    }
    if (child->compressed) {
      // Bump the value of the compressed node on a match.
      if (child->key() == str) {
        child->val += 1;
        return child->val;
      }
      // Otherwise, we need to split the node.
      auto old_key = child->key();
      auto new_key = str;
      assert(old_key != new_key);
      auto prefix = common_prefix(old_key, new_key); // Full prefix.
      assert(old_key != prefix || new_key != prefix);
      auto common = prefix.substr(depth); // Prefix starting at `depth`.
      assert(common.size() > 0);
      // Store the old node and then create new nodes for the common prefix.
      std::unique_ptr<node> old_node;
      child.swap(old_node);
      auto* junction = emplace(ptr, common);
      assert(!junction->compressed);
      // The `junction` node holds the key up to the common prefix now.
      if (new_key.size() == prefix.size()) {
        // Case 1: New key is a prefix of old key (e.g., "test" vs "tested").
        // The old node becomes a child.
        junction->val = 1;
        junction->key(new_key);
        auto ch_old = old_key[depth + common.size()];
        old_node->compressed = old_key.size() > prefix.size() + 1;
        junction->ref(ch_old) = std::move(old_node);
      } else if (old_key.size() == prefix.size()) {
        // Case 2: Old key is a prefix of new key (e.g., "tested" vs "test").
        // The new node becomes a child.
        junction->val = old_node->val;
        junction->key_data.swap(old_node->key_data);
        junction->key_len = old_node->key_len;
        auto ch_new = new_key[depth + common.size()];
        auto compressed = new_key.size() > prefix.size() + 1;
        junction->ref(ch_new) = std::make_unique<node>(str, compressed);
      } else {
        // Case 3: Neither is a prefix of the other (e.g., "test" vs "temp").
        // We have an actual split and the junction will have two children.
        assert(old_key.size() > prefix.size());
        assert(new_key.size() > prefix.size());
        auto ch_old = old_key[depth + common.size()];
        old_node->compressed = old_key.size() > prefix.size() + 1;
        junction->ref(ch_old) = std::move(old_node);
        auto ch_new = new_key[depth + common.size()];
        auto compressed = new_key.size() > prefix.size() + 1;
        junction->ref(ch_new) = std::make_unique<node>(str, compressed);
      }
      return 1;
    }
    ptr = child.get();
    ++depth;
  }
  // We have reached the end of the key. This means the current node is at the
  // right path. Bump the value of the node and set the key if necessary.
  if (ptr->val == 0) {
    ptr->key(str);
  }
  ptr->val += 1;
  return ptr->val;
}

trie::node* trie::emplace(node* pos, std::string_view path) {
  for (auto c : path) {
    auto& child = pos->ref(c);
    if (!child) {
      child = std::make_unique<node>();
    }
    pos = child.get();
  }
  return pos;
}

namespace {

/// Erases a key from the trie.
/// @param pos The current node.
/// @param key The key to erase.
/// @param depth The current depth in the trie.
/// @param result Output parameter that stores whether the key was erased.
/// @returns `true` if `pos` can be removed.
bool erase_recursive(trie::node* pos, std::string_view key, size_t depth,
                     bool& result) {
  // Check if we've reached the end of the key.
  if (depth == key.size()) {
    // Cannot be a match if this node has no value or if it is compressed. If
    // this node is compressed, `key` is only a prefix of the node's key.
    if (pos->val == 0 || pos->compressed) {
      return false;
    }
    if (pos->val > 1) {
      --pos->val;
      return false;
    }
    result = true;
    if (pos->has_children()) {
      pos->val = 0;
      pos->key_data.reset();
      return false;
    }
    assert(pos->val == 1);
    return true;
  }
  if (pos->compressed) {
    assert(!pos->has_children());
    if (pos->key() == key) {
      if (pos->val > 1) {
        --pos->val;
        return false;
      }
      result = true;
      return true;
    }
    return false;
  }
  auto ch = key[depth];
  auto child = pos->at(ch);
  if (!child) {
    return false;
  }
  if (erase_recursive(child, key, depth + 1, result)) {
    pos->erase(ch);
    return !pos->has_children() && pos->val == 0;
  }
  return false;
}

} // namespace

bool trie::erase(std::string_view key) {
  bool result = false;
  std::ignore = erase_recursive(&root_, key, 0, result);
  return result;
}

bool trie::contains(std::string_view str) const {
  auto* pos = &root_;
  for (auto c : str) {
    auto* child = pos->at(c);
    if (!child) {
      return false;
    }
    if (child->compressed) {
      return child->key() == str;
    }
    pos = child;
  }
  return pos->val > 0;
}

bool trie::has_prefix(std::string_view str) const {
  auto* pos = &root_;
  for (auto c : str) {
    auto* child = pos->at(c);
    if (!child) {
      return false;
    }
    if (child->compressed) {
      return child->key().compare(0, str.size(), str) == 0;
    }
    pos = child;
  }
  return true; // We've reached the end of the prefix. This is always a match.
}

std::vector<std::string> trie::keys() const {
  std::vector<std::string> result;
  for_each([&result](std::string_view key) { result.emplace_back(key); });
  return result;
}

size_t trie::size() const noexcept {
  size_t result = 0;
  for_each([&result](std::string_view) { ++result; });
  return result;
}

int trie::value(std::string_view key) const noexcept {
  auto* pos = &root_;
  for (auto c : key) {
    auto* child = pos->at(c);
    if (!child) {
      return 0;
    }
    if (child->compressed) {
      return child->key() == key ? child->val : 0;
    }
    pos = child;
  }
  return pos->val;
}

namespace {

void do_deep_copy(trie::node* dst, const trie::node* src) {
  dst->val = src->val;
  if (src->key_data) {
    dst->key_data = std::make_unique<char[]>(src->key_len + 1);
    std::memcpy(dst->key_data.get(), src->key_data.get(), src->key_len);
    dst->key_data[src->key_len] = '\0';
    dst->key_len = src->key_len;
  }
  dst->compressed = src->compressed;
  for (size_t index = 0; index < trie::num_segments; ++index) {
    auto& src_seg = src->segments[index];
    if (!src_seg) {
      continue;
    }
    auto& dst_seg = dst->segments[index];
    dst_seg = std::make_unique<trie::segment>();
    for (size_t child = 0; child < trie::segment_size; ++child) {
      auto& src_child = src_seg->children[child];
      if (!src_child) {
        continue;
      }
      auto& dst_child = dst_seg->children[child];
      dst_child = std::make_unique<trie::node>();
      do_deep_copy(dst_child.get(), src_child.get());
    }
  }
}

} // namespace

void trie::copy_from(const trie& other) {
  clear();
  do_deep_copy(&root_, &other.root_);
}

void trie::clear() noexcept {
  for (auto& seg : root_.segments) {
    seg.reset();
  }
}

} // namespace broker::detail
