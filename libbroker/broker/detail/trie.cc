#include "broker/detail/trie.hh"

#include <stdexcept>
#include <vector>

namespace broker::detail {

namespace {

std::string_view common_prefix(std::string_view lhs, std::string_view rhs) {
  auto len = std::min(lhs.size(), rhs.size());
  auto i = size_t{0};
  while (i < len && lhs[i] == rhs[i])
    ++i;
  return lhs.substr(0, i);
}

constexpr bool printable(char c) {
  return c >= 32 && c <= 127;
}

} // namespace

trie::node_or_leaf::~node_or_leaf() noexcept {}

bool trie::node::is_leaf() const noexcept {
  return false;
}

bool trie::node::has_children() const noexcept {
  return std::any_of(std::begin(children), std::end(children),
                     [](const auto& x) { return x != nullptr; });
}

bool trie::leaf::is_leaf() const noexcept {
  return true;
}

void trie::insert(std::string_view str) {
  auto* ptr = &root_;
  auto pos = size_t{0};
  for (auto c : str) {
    if (!printable(c))
      throw std::invalid_argument("trie: non-printable character in key");
    auto index = static_cast<size_t>(c - 32);
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
        auto index = static_cast<size_t>(ch - 32);
        ptr->children[index].reset(new_node);
        ptr = new_node;
      }
      // std::cout<<"common: "<<common<<std::endl;
      //  Insert the rest of the new string.
      if (str.size() > pos + common.size()) {
        // std::cout<<"str: "<<str<<std::endl;
        auto* new_leaf = new leaf(std::string{str});
        auto i = static_cast<size_t>(str[pos + common.size()] - 32);
        ptr->children[i].reset(new_leaf);
      }
      // Re-insert the old leaf unless it was a prefix of the new string.
      if (lptr->key.size() <= pos + common.size()) {
        delete lptr;
      } else {
        // std::cout<<"old: "<<lptr->key<<std::endl;
        auto i = static_cast<size_t>(lptr->key[pos + common.size()] - 32);
        ptr->children[i].reset(lptr);
      }
      return;
    }
    ptr = static_cast<node*>(next);
    ++pos;
  }
}

bool trie::erase(std::string_view key) {
  std::vector<std::pair<node*, char>> path;
  auto* n = &root_;
  for (auto c : key) {
    if (!printable(c))
      throw std::invalid_argument("trie: non-printable character in key");
    auto index = static_cast<size_t>(c - 32);
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
        parent->children[static_cast<size_t>(ch - 32)].reset();
        n = parent;
      }
      return true;
    }
    path.emplace_back(n, c);
    n = static_cast<node*>(child);
  }
  return false;
}

bool trie::contains(std::string_view str) const {
  auto* n = &root_;
  for (auto c : str) {
    if (!printable(c))
      throw std::logic_error("non-printable character in string");
    auto i = static_cast<size_t>(c - 32);
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

bool trie::has_prefix(std::string_view str) const {
  auto* n = &root_;
  for (auto c : str) {
    if (!printable(c))
      throw std::logic_error("non-printable character in string");
    auto i = static_cast<size_t>(c - 32);
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

} // namespace broker::detail
