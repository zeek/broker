#pragma once

#include <algorithm>
#include <memory>
#include <type_traits>
#include <utility>

#include "caf/meta/omittable_if_empty.hpp"
#include "caf/meta/type_name.hpp"
#include "caf/sec.hpp"

#include "broker/detail/assert.hh"

namespace broker::alm {

/// Helper class for giving multipath children an STL-like interface.
template <class Pointer>
class multipath_node_set {
public:
  using mutable_pointer = std::remove_const_t<Pointer>;

  using multipath_type = std::remove_pointer_t<mutable_pointer>;

  using value_type = typename multipath_type::value_type;

  explicit multipath_node_set(Pointer this_ptr) : this_(this_ptr) {
    //nop
  }

  auto begin() const {
    return this_->nodes_begin();
  }

  auto end() const {
    return this_->nodes_end();
  }

  auto size() const {
    return this_->nodes_count();
  }

  auto empty() const {
    return size() == 0;
  }

  template <class... Ts>
  decltype(auto) emplace(Ts&&... xs) {
    return this_->emplace_node(std::forward<Ts>(xs)...);
  }

private:
  Pointer this_;
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
template <class PeerId>
class multipath {
public:
  using pointer = multipath*;

  using const_pointer = const multipath*;

  using iterator = pointer;

  using value_type = PeerId;

  static constexpr size_t block_size = 16;

  static constexpr bool nothrow_move
    = std::is_nothrow_move_constructible<PeerId>::value;

  static constexpr bool nothrow_assign
    = std::is_nothrow_move_assignable<PeerId>::value;

  struct node_less {
    bool operator()(const multipath& x, const PeerId& y) const noexcept {
      return x.id() < y;
    }

    bool operator()(const PeerId& x, const multipath& y) const noexcept {
      return x < y.id();
    }
  };

  multipath() noexcept(std::is_nothrow_constructible<PeerId>::value) = default;

  explicit multipath(PeerId id) noexcept(nothrow_move) : id_(std::move(id)) {
    // nop
  }

  /// Constructs a multipath from the linear path `[first, last)`.
  /// @pre `first != last`
  template <class Iterator>
  explicit multipath(Iterator first, Iterator last) : id_(*first) {
    auto pos = this;
    for (++first; first != last; ++first)
      pos = pos->emplace_node(*first).first;
  }

  multipath(multipath&& other) noexcept(nothrow_move)
    : id_(std::move(other.id_)),
      nodes_(nullptr),
      size_(other.size_),
      reserved_(other.reserved_) {
    using std::swap;
    swap(nodes_, other.nodes_);
  }

  multipath(const multipath& other) {
    *this = other;
  }

  multipath& operator=(multipath&& other) noexcept(nothrow_assign) {
    using std::swap;
    swap(id_, other.id_);
    swap(nodes_, other.nodes_);
    size_ = other.size_;
    reserved_ = other.reserved_;
    return *this;
  }

  multipath& operator=(const multipath& other) {
    size_ = 0;
    id_ = other.id_;
    for (const auto& node : other.nodes()) {
      auto& child = *emplace_node(node.id()).first;
      child = node;
    }
    return *this;
  }

  ~multipath() noexcept {
    delete[] nodes_;
  }

  const auto& id() const noexcept {
    return id_;
  }

  /// Returns the root ID of the path.
  const auto& head() const noexcept {
    return id_;
  }

  auto nodes() noexcept {
    return multipath_node_set<pointer>{this};
  }

  auto nodes() const noexcept {
    return multipath_node_set<const_pointer>{this};
  }

  auto nodes_begin() noexcept {
    return nodes_;
  }

  auto nodes_end() noexcept {
    return nodes_ + size_;
  }

  auto nodes_begin() const noexcept {
    return nodes_;
  }

  auto nodes_end() const noexcept {
    return nodes_ + size_;
  }

  auto nodes_count() const noexcept {
    return size_;
  }

  std::pair<iterator, bool> emplace_node(PeerId id) {
    node_less pred;
    auto insertion_point = std::lower_bound(nodes_begin(), nodes_end(), id,
                                            pred);
    if (insertion_point == nodes_end()) {
      return {append(std::move(id)), true};
    } else if (insertion_point->id() == id) {
      return {insertion_point, false};
    } else {
      return {insert_at(insertion_point, std::move(id)), true};
    }
  }

  /// Tries to merge `x` into this multipath.
  /// @returns `false` if `x.id()` is already in ::nodes, `true` otherwise.
  bool merge(multipath&& x) {
    auto [iter, inserted] = emplace_node(std::move(x.id_));
    if (!inserted)
      return false;
    for (size_t i = 0; i < x.size_; ++i)
      if (!iter->merge(std::move(x.nodes_[i])))
        return false;
    return true;
  }

  template <class Iterator>
  bool splice(Iterator first, Iterator last) {
    if (first == last)
      return true;
    if (*first != id_)
      return false;
    if (++first == last)
      return true;
    auto child = emplace_node(*first).first;
    if (++first == last)
      return true;
    child->splice_cont(first, last);
    return true;
  }

  template <class LinearPath>
  bool splice(const LinearPath& path) {
    return splice(path.begin(), path.end());
  }

  bool equals(const multipath& other) const noexcept {
    auto is_equal = [](const multipath& x, const multipath& y) {
      return x.equals(y);
    };
    return id_ == other.id_
           && std::equal(nodes_begin(), nodes_end(), other.nodes_begin(),
                         other.nodes_end(), is_equal);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, multipath& x) {
    if constexpr (Inspector::reads_state) {
      auto tag = caf::meta::omittable_if_empty();
      auto nodes = x.nodes();
      return f(std::tie(x.id_, tag, nodes));
    } else {
      if (auto err = f(x.id_))
        return err;
      size_t num_children = 0;
      if (auto err = f.begin_sequence(num_children))
        return err;
      x.size_ = 0;
      for (size_t i = 0; i < num_children; ++i) {
        multipath tmp;
        if (auto err = inspect(f, tmp))
          return err;
        // TODO: this assumes Inspector::result_type is constructible from
        //       caf::sec.
        if (!x.merge(std::move(tmp)))
          return typename Inspector::result_type{caf::sec::runtime_error};
      }
      return f.end_sequence();
    }
  }

private:
  // Recursively splices a linear path into a multipath.
  template <class Iterator>
  void splice_cont(Iterator first, Iterator last) {
    BROKER_ASSERT(first != last);
    auto child = emplace_node(*first).first;
    if (++first != last)
      child->splice_cont(first, last);
  }


  iterator append(PeerId&& id) {
    grow_if_needed();
    auto ptr = nodes_ + size_;
    ++size_;
    ptr->id_ = std::move(id);
    return ptr;
  }

  iterator insert_at(iterator pos, PeerId&& id) {
    grow_if_needed();
    auto last = nodes_end();
    std::move_backward(pos, last, last + 1);
    *pos = multipath{std::move(id)};
    ++size_;
    return pos;
  }

  void grow_if_needed() {
    if (size_ == reserved_) {
      auto tmp = std::make_unique<multipath[]>(reserved_ + block_size);
      if constexpr (nothrow_assign)
        std::move(nodes_begin(), nodes_end(), tmp.get());
      else
        std::copy(nodes_begin(), nodes_end(), tmp.get());
      auto ptr = nodes_;
      nodes_ = tmp.release();
      tmp.reset(ptr);
      reserved_ += block_size;
    }
  }

  /// Unique identifier of this node.
  PeerId id_;

  // Unfortunately, `std::vector` is not guaranteed to work with incomplete
  // types, so we have to roll our own dynamic memory management here.

  /// Stores the childen of this node, if any.
  multipath* nodes_ = nullptr;

  /// Current size of `nodes_`.
  size_t size_ = 0;

  /// Reserved capacity for `nodes_`.
  size_t reserved_ = 0;
};

/// @relates multipath
template <class PeerId>
bool operator==(const multipath<PeerId>& x, const multipath<PeerId>& y) {
  return x.equals(y);
}

/// @relates multipath
template <class PeerId>
bool operator!=(const multipath<PeerId>& x, const multipath<PeerId>& y) {
  return !(x == y);
}

/// Fills the `routes` list such that all reachable receivers are included.
/// @param receivers List of nodes that should receive a certain message.
/// @param tbl The routing table for shorest path lookups.
/// @param routes Stores the source routing paths.
/// @param unreachables Stores receivers where the routing table lookup failed.
/// @relates multipath
/// @relatesalso routing_table
template <class PeerId, class RoutingTable>
void generate_paths(const std::vector<PeerId>& receivers,
                    const RoutingTable& tbl,
                    std::vector<multipath<PeerId>>& routes,
                    std::vector<PeerId>& unreachables) {
  auto route = [&](const PeerId& id) -> auto& {
    for (auto& mpath : routes)
      if (mpath.id() == id)
        return mpath;
    routes.emplace_back(id);
    return routes.back();
  };
  for (auto& receiver : receivers) {
    if (auto ptr = shortest_path(tbl, receiver)) {
      auto& sp = *ptr;
      BROKER_ASSERT(!sp.empty());
      route(sp[0]).splice(sp);
    } else {
      unreachables.emplace_back(receiver);
    }
  }
}

} // namespace broker::alm
