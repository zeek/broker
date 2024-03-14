#include <utility>

#include "broker/alm/multipath.hh"

#include "broker/alm/routing_table.hh"

namespace broker::alm {

multipath_tree::multipath_tree(endpoint_id id) {
  root = detail::new_instance<multipath_node>(mem, id);
}

multipath_tree::~multipath_tree() {
  // We can simply "wink out" the tree structure, but we still need to release
  // all references to node IDs.
  root->shallow_delete();
}

multipath_group::~multipath_group() {
  delete first_;
}

bool multipath_group::equals(const multipath_group& other) const noexcept {
  auto eq = [](const auto& lhs, const auto& rhs) { return lhs.equals(rhs); };
  return std::equal(begin(), end(), other.begin(), other.end(), eq);
}

bool multipath_group::contains(const endpoint_id& id) const noexcept {
  auto pred = [&id](const multipath_node& node) { return node.contains(id); };
  return std::any_of(begin(), end(), pred);
}

template <class MakeNewNode>
std::pair<multipath_node*, bool>
multipath_group::emplace_impl(const endpoint_id& id,
                              MakeNewNode make_new_node) {
  if (size_ == 0) {
    first_ = make_new_node();
    size_ = 1;
    return {first_, true};
  } else {
    // Insertion sorts by ID.
    BROKER_ASSERT(first_ != nullptr);
    if (first_->id_ == id) {
      return {first_, false};
    } else if (first_->id_ > id) {
      ++size_;
      auto new_node = make_new_node();
      new_node->right_ = first_;
      first_ = new_node;
      return {new_node, true};
    }
    auto pos = first_;
    auto next = pos->right_;
    while (next != nullptr) {
      if (next->id_ == id) {
        return {next, false};
      } else if (next->id_ > id) {
        ++size_;
        auto new_node = make_new_node();
        pos->right_ = new_node;
        new_node->right_ = next;
        return {new_node, true};
      } else {
        pos = next;
        next = next->right_;
      }
    }
    ++size_;
    auto new_node = make_new_node();
    BROKER_ASSERT(pos->right_ == nullptr);
    pos->right_ = new_node;
    return {new_node, true};
  }
}

std::pair<multipath_node*, bool>
multipath_group::emplace(detail::monotonic_buffer_resource& mem,
                         const endpoint_id& id) {
  auto make_new_node = [&mem, &id] {
    return detail::new_instance<multipath_node>(mem, id);
  };
  return emplace_impl(id, make_new_node);
}

bool multipath_group::emplace(multipath_node* new_node) {
  auto make_new_node = [new_node] { return new_node; };
  return emplace_impl(new_node->id_, make_new_node).second;
}

void multipath_group::shallow_delete() noexcept {
  for (auto& child : *this)
    child.shallow_delete();
}

multipath_node::~multipath_node() {
  delete right_;
}

bool multipath_node::equals(const multipath_node& other) const noexcept {
  return id_ == other.id_ && is_receiver_ == other.is_receiver_
         && down_.equals(other.down_);
}

bool multipath_node::contains(const endpoint_id& id) const noexcept {
  return id_ == id || down_.contains(id);
}

void multipath_node::stringify(std::string& buf) const {
  buf += '(';
  buf += to_string(id_);
  if (!down_.empty()) {
    buf += ", [";
    auto i = down_.begin();
    i->stringify(buf);
    while (++i != down_.end()) {
      buf += ", ";
      i->stringify(buf);
    }
    buf += ']';
  }
  buf += ')';
}

void multipath_node::shallow_delete() noexcept {
  id_ = endpoint_id{};
  down_.shallow_delete();
}

multipath::multipath() {
  tree_ = std::make_shared<multipath_tree>(endpoint_id{});
  head_ = tree_->root;
}

multipath::multipath(const endpoint_id& id) {
  tree_ = std::make_shared<multipath_tree>(id);
  head_ = tree_->root;
}

multipath::multipath(const endpoint_id& id, bool is_receiver) : multipath(id) {
  head_->is_receiver_ = is_receiver;
}

multipath::multipath(tree_ptr t, multipath_node* h)
  : tree_(std::move(t)), head_(h) {
  // nop
}

void multipath::generate(const std::vector<endpoint_id>& receivers,
                         const routing_table& tbl,
                         std::vector<multipath>& routes,
                         std::vector<endpoint_id>& unreachables) {
  auto route = [&](const endpoint_id& id) -> auto& {
    for (auto& mpath : routes)
      if (mpath.head().id() == id)
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

void multipath::splice(const std::vector<endpoint_id>& path) {
  BROKER_ASSERT(path.empty() || path[0] == head().id());
  if (!path.empty()) {
    auto child = head_;
    for (auto i = path.begin() + 1; i != path.end(); ++i)
      child = child->down_.emplace(tree_->mem, *i).first;
    child->is_receiver_ = true;
  }
}

} // namespace broker::alm
