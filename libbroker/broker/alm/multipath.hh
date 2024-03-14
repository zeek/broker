#pragma once

#include <algorithm>
#include <memory>
#include <type_traits>
#include <utility>

#include "broker/detail/assert.hh"
#include "broker/detail/monotonic_buffer_resource.hh"
#include "broker/endpoint_id.hh"
#include "broker/fwd.hh"

namespace broker::alm {

struct multipath_tree {
  explicit multipath_tree(endpoint_id id);
  ~multipath_tree();
  multipath_node* root;
  detail::monotonic_buffer_resource mem;
};

template <class T>
class node_iterator {
public:
  using iterator_category = std::forward_iterator_tag;

  using difference_type = ptrdiff_t;

  using value_type = T;

  using pointer = value_type*;

  using reference = value_type&;

  explicit node_iterator(pointer ptr) noexcept : ptr_(ptr) {
    // nop
  }

  node_iterator(const node_iterator&) noexcept = default;

  node_iterator& operator=(const node_iterator&) noexcept = default;

  node_iterator operator++(int) {
    node_iterator cpy{ptr_};
    ptr_ = ptr_->right_;
    return cpy;
  }

  node_iterator& operator++() {
    ptr_ = ptr_->right_;
    return *this;
  }

  reference operator*() {
    return *ptr_;
  }

  pointer operator->() {
    return ptr_;
  }

  pointer get() {
    return ptr_;
  }

private:
  pointer ptr_;
};

template <class T, class U>
auto operator==(node_iterator<T> x, node_iterator<U> y)
  -> decltype(x.get() == y.get()) {
  return x.get() == y.get();
}

template <class T, class U>
auto operator!=(node_iterator<T> x, node_iterator<U> y)
  -> decltype(x.get() != y.get()) {
  return x.get() != y.get();
}

class multipath_group {
public:
  friend class multipath;
  friend class multipath_node;

  using iterator = node_iterator<multipath_node>;

  using const_iterator = node_iterator<const multipath_node>;

  multipath_group() noexcept = default;

  multipath_group(const multipath_group&) = delete;

  multipath_group& operator=(const multipath_group&) = delete;

  ~multipath_group();

  size_t size() const noexcept {
    return size_;
  }

  bool empty() const noexcept {
    return size_ == 0;
  }

  iterator begin() noexcept {
    return iterator{first_};
  }

  const_iterator begin() const noexcept {
    return const_iterator{first_};
  }

  iterator end() noexcept {
    return iterator{nullptr};
  }

  const_iterator end() const noexcept {
    return const_iterator{nullptr};
  }

  bool equals(const multipath_group& other) const noexcept;

  bool contains(const endpoint_id& id) const noexcept;

  std::pair<multipath_node*, bool>
  emplace(detail::monotonic_buffer_resource& mem, const endpoint_id& id);

  bool emplace(multipath_node* node);

private:
  template <class MakeNewNode>
  std::pair<multipath_node*, bool> emplace_impl(const endpoint_id& id,
                                                MakeNewNode make_new_node);

  void shallow_delete() noexcept;

  size_t size_ = 0;
  multipath_node* first_ = nullptr;
};

class multipath_node {
public:
  friend class multipath;
  friend class multipath_group;
  friend class node_iterator<const multipath_node>;
  friend class node_iterator<multipath_node>;
  friend struct multipath_tree;

  explicit multipath_node(const endpoint_id& id) noexcept : id_(id) {
    // nop
  }

  multipath_node() = delete;

  multipath_node(const multipath_node&) = delete;

  multipath_node& operator=(const multipath_node&) = delete;

  ~multipath_node();

  auto& head() noexcept {
    return *this;
  }

  const auto& head() const noexcept {
    return *this;
  }

  const auto& id() const noexcept {
    return id_;
  }

  bool is_receiver() const noexcept {
    return is_receiver_;
  }

  auto& nodes() noexcept {
    return down_;
  }

  const auto& nodes() const noexcept {
    return down_;
  }

  bool equals(const multipath_node& other) const noexcept;

  bool contains(const endpoint_id& id) const noexcept;

  void stringify(std::string& buf) const;

private:
  template <class Inspector>
  bool save_children(Inspector& f) {
    if (f.begin_sequence(down_.size()))
      for (auto& child : down_)
        if (!child.save(f))
          return false;
    return f.end_sequence();
  }

  template <class Inspector>
  bool save(Inspector& f) {
    // We are lying to the inspector about the type, because multipath_node and
    // multipath_group are internal implementation details.
    return f.template begin_object_t<multipath>() //
           && f.begin_field("id")                 // <id>
           && f.apply(id_)                        //   <value>
           && f.end_field()                       // </id>
           && f.begin_field("is_receiver")        // <is_receiver>
           && f.apply(is_receiver_)               //   <value>
           && f.end_field()                       // </is_receiver>
           && f.begin_field("nodes")              // <nodes>
           && save_children(f)                    //   [...]
           && f.end_field()                       // </nodes>
           && f.end_object();
  }

  template <class Inspector>
  bool load_children(detail::monotonic_buffer_resource& mem, Inspector& f) {
    size_t n = 0;
    if (f.begin_sequence(n)) {
      for (size_t i = 0; i < n; ++i) {
        auto child = detail::new_instance<multipath_node>(mem, endpoint_id{});
        if (!child->load(mem, f)) {
          child->shallow_delete();
          return false;
        }
        if (!down_.emplace(child)) {
          child->shallow_delete();
          f.field_invariant_check_failed(
            "a multipath may not contain duplicates");
          return false;
        }
      }
    }
    return f.end_sequence();
  }

  template <class Inspector>
  bool load(detail::monotonic_buffer_resource& mem, Inspector& f) {
    return f.template begin_object_t<multipath>() //
           && f.begin_field("id")                 // <id>
           && f.apply(id_)                        //   <value>
           && f.end_field()                       // </id>
           && f.begin_field("is_receiver")        // <is_receiver>
           && f.apply(is_receiver_)               //   <value>
           && f.end_field()                       // </is_receiver>
           && f.begin_field("nodes")              // <nodes>
           && load_children(mem, f)               //   [...]
           && f.end_field()                       // </nodes>
           && f.end_object();
  }

  void shallow_delete() noexcept;

  template <class Iterator, class Sentinel>
  void splice_cont(detail::monotonic_buffer_resource& mem, Iterator first,
                   Sentinel last) {
    if (first != last) {
      auto child = down_.emplace(mem, *first).first;
      child->splice_cont(mem, ++first, last);
    }
  }

  endpoint_id id_;
  bool is_receiver_ = false;
  multipath_node* right_ = nullptr;
  multipath_group down_;
};

/// A recursive data structure for encoding branching paths for source routing.
/// For example:
///
/// ~~~
/// A ────> B ─┬──> C
///            └──> D ────> E
/// ~~~
///
/// In this topology, the sender A sends a message to B that B then has to
/// forward to C and D. After that, C is the final destination on that branch,
/// but D has to forward the message also to E.
class multipath {
public:
  using tree_ptr = std::shared_ptr<multipath_tree>;

  multipath();

  explicit multipath(const endpoint_id& id);

  multipath(const endpoint_id& id, bool is_receiver);

  /// Constructs a multipath from the linear path `[first, last)`.
  /// @pre `first != last`
  template <class Iterator, class Sentinel>
  explicit multipath(Iterator first, Sentinel last) : multipath() {
    if (first != last) {
      head_->id_ = *first;
      auto pos = head_;
      for (++first; first != last; ++first)
        pos = pos->down_.emplace(tree_->mem, *first).first;
      pos->is_receiver_ = true;
    }
  }

  multipath(tree_ptr, multipath_node*);

  explicit multipath(const tree_ptr& tptr) : multipath(tptr, tptr->root) {
    // nop
  }

  multipath(multipath&& other) noexcept = default;

  multipath(const multipath& other) = default;

  multipath& operator=(multipath&& other) noexcept = default;

  multipath& operator=(const multipath& other) = default;

  const auto& head() const noexcept {
    return *head_;
  }

  bool equals(const multipath& other) const noexcept {
    return head_->equals(*other.head_);
  }

  bool contains(const endpoint_id& id) const noexcept {
    return head_->contains(id);
  }

  size_t num_nodes() const noexcept {
    return head_->down_.size();
  }

  template <class F>
  void for_each_node(F fun) const {
    for (auto i = head_->down_.begin(); i != head_->down_.end(); ++i)
      fun(multipath{tree_, i.get()});
  }

  template <class F>
  bool for_each_node_while(F fun) const {
    for (auto i = head_->down_.begin(); i != head_->down_.end(); ++i)
      if (!fun(multipath{tree_, i.get()}))
        return false;
    return true;
  }

  template <class Inspector>
  friend bool inspect(Inspector& f, multipath& x) {
    if constexpr (Inspector::is_loading)
      return x.load(f);
    else
      return x.save(f);
  }

  friend std::string to_string(const alm::multipath& x) {
    std::string result;
    x.head_->stringify(result);
    return result;
  }

  /// Fills the `routes` list such that all reachable receivers are included.
  /// @param receivers List of nodes that should receive a certain message.
  /// @param tbl The routing table for shorest path lookups.
  /// @param routes Stores the source routing paths.
  /// @param unreachables Stores receivers without routing table entries.
  static void generate(const std::vector<endpoint_id>& receivers,
                       const routing_table& tbl, std::vector<multipath>& routes,
                       std::vector<endpoint_id>& unreachables);

private:
  std::pair<multipath_node*, bool> emplace(const endpoint_id& id) {
    return head_->down_.emplace(tree_->mem, id);
  }

  template <class Inspector>
  bool load(Inspector& f) {
    return head_->load(tree_->mem, f);
  }

  template <class Inspector>
  bool save(Inspector& f) {
    return head_->save(f);
  }

  // Splices a linear path into this multipath. The last entry in the path is
  // automatically tagged as a receiver.
  // @pre `!path.empty() && path[0] == id()`
  void splice(const std::vector<endpoint_id>& path);

  /// Provides storage for children.
  std::shared_ptr<multipath_tree> tree_;

  /// Stores information for this object.
  multipath_node* head_;
};

/// @relates multipath
inline bool operator==(const multipath& x, const multipath& y) {
  return x.equals(y);
}

/// @relates multipath
inline bool operator!=(const multipath& x, const multipath& y) {
  return !(x == y);
}

/// @relates multipath
std::string to_string(const alm::multipath& x);

} // namespace broker::alm
