#pragma once

#include "broker/data.hh"
#include "broker/fwd.hh"
#include "broker/message.hh"
#include "broker/time.hh"

#include <caf/type_id.hpp>
#include <caf/uuid.hpp>

#include <random>

// -- forward declarations -----------------------------------------------------

struct legacy_node_message;

using uuid = caf::uuid;

struct uuid_multipath_tree;

class uuid_multipath;
class uuid_multipath_group;
class uuid_multipath_node;

using node_message_content =
  broker::variant<broker::data_message, broker::command_message>;

using uuid_node_message = caf::cow_tuple<node_message_content, uuid_multipath>;

// -- type IDs -----------------------------------------------------------------

#define MICRO_BENCH_ADD_TYPE(type) CAF_ADD_TYPE_ID(micro_benchmarks, type)

CAF_BEGIN_TYPE_ID_BLOCK(micro_benchmarks, caf::id_block::broker_internal::end)

  MICRO_BENCH_ADD_TYPE((caf::stream<legacy_node_message>) )
  MICRO_BENCH_ADD_TYPE((legacy_node_message))
  MICRO_BENCH_ADD_TYPE((std::vector<legacy_node_message>) )

  MICRO_BENCH_ADD_TYPE((caf::stream<uuid_node_message>) )
  MICRO_BENCH_ADD_TYPE((std::vector<uuid_node_message>) )
  MICRO_BENCH_ADD_TYPE((uuid_multipath))
  MICRO_BENCH_ADD_TYPE((uuid_node_message))

CAF_END_TYPE_ID_BLOCK(micro_benchmarks)

// -- custom types -------------------------------------------------------------

/// A `node_message` as it used to be pre-ALM.
struct legacy_node_message {
  /// Content of the message.
  node_message_content content;

  /// Time-to-life counter.
  uint16_t ttl;
};

template <class Inspector>
bool inspect(Inspector& f, legacy_node_message& x) {
  return f.object(x).fields(f.field("content", x.content),
                            f.field("ttl", x.ttl));
}

using uuid = caf::uuid;

class uuid_multipath_node;

struct uuid_multipath_tree {
  uuid_multipath_tree(uuid id, bool is_receiver);
  ~uuid_multipath_tree();
  uuid_multipath_node* root;
  broker::detail::monotonic_buffer_resource mem;
};

class uuid_multipath_group {
public:
  friend class uuid_multipath;
  friend class uuid_multipath_node;

  using iterator = broker::alm::node_iterator<uuid_multipath_node>;

  using const_iterator = broker::alm::node_iterator<const uuid_multipath_node>;

  uuid_multipath_group() noexcept = default;

  uuid_multipath_group(const uuid_multipath_group&) = delete;

  uuid_multipath_group& operator=(const uuid_multipath_group&) = delete;

  ~uuid_multipath_group();

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

  bool equals(const uuid_multipath_group& other) const noexcept;

  bool contains(uuid what) const noexcept;

  std::pair<uuid_multipath_node*, bool>
  emplace(broker::detail::monotonic_buffer_resource& mem, uuid id,
          bool is_receiver);

  bool emplace(uuid_multipath_node* node);

private:
  template <class MakeNewNode>
  std::pair<uuid_multipath_node*, bool> emplace_impl(uuid id,
                                                     MakeNewNode make_new_node);

  size_t size_ = 0;
  uuid_multipath_node* first_ = nullptr;
};

class uuid_multipath_node {
public:
  friend class uuid_multipath;
  friend class uuid_multipath_group;
  friend class broker::alm::node_iterator<const uuid_multipath_node>;
  friend class broker::alm::node_iterator<uuid_multipath_node>;
  friend struct uuid_multipath_tree;

  uuid_multipath_node(uuid id, bool is_receiver) noexcept
    : id_(id), is_receiver_(is_receiver) {
    // nop
  }

  uuid_multipath_node() = delete;

  uuid_multipath_node(const uuid_multipath_node&) = delete;

  uuid_multipath_node& operator=(const uuid_multipath_node&) = delete;

  ~uuid_multipath_node();

  const uuid& id() const noexcept {
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

  bool equals(const uuid_multipath_node& other) const noexcept;

  bool contains(uuid what) const noexcept;

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
    return f.begin_object(caf::type_id_v<uuid_multipath>,
                          caf::type_name_v<uuid_multipath>)
           && f.begin_field("id")          // <id>
           && f.apply(id_)                 //   [...]
           && f.end_field()                // </id>
           && f.begin_field("is_receiver") // <is_receiver>
           && f.apply(is_receiver_)        //   [...]
           && f.end_field()                // </is_receiver>
           && f.begin_field("nodes")       // <nodes>
           && save_children(f)             //   [...]
           && f.end_field()                // </nodes>
           && f.end_object();
  }

  template <class Inspector>
  bool load_children(broker::detail::monotonic_buffer_resource& mem,
                     Inspector& f) {
    size_t n = 0;
    if (f.begin_sequence(n)) {
      for (size_t i = 0; i < n; ++i) {
        auto child =
          broker::detail::new_instance<uuid_multipath_node>(mem, uuid{}, false);
        if (!child->load(mem, f)) {
          return false;
        } else if (!down_.emplace(child)) {
          f.emplace_error(caf::sec::field_invariant_check_failed,
                          "a multipath may not contain duplicates");
          return false;
        }
      }
    }
    return f.end_sequence();
  }

  template <class Inspector>
  bool load(broker::detail::monotonic_buffer_resource& mem, Inspector& f) {
    return f.begin_object(caf::type_id_v<uuid_multipath>,
                          caf::type_name_v<uuid_multipath>)
           && f.begin_field("id")          // <id>
           && f.apply(id_)                 //   [...]
           && f.end_field()                // </id>
           && f.begin_field("is_receiver") // <is_receiver>
           && f.apply(is_receiver_)        //   [...]
           && f.end_field()                // </is_receiver>
           && f.begin_field("nodes")       // <nodes>
           && load_children(mem, f)        //   [...]
           && f.end_field()                // </nodes>
           && f.end_object();
  }

  uuid id_;
  bool is_receiver_;
  uuid_multipath_node* right_ = nullptr;
  uuid_multipath_group down_;
};

class uuid_multipath {
public:
  using tree_ptr = std::shared_ptr<uuid_multipath_tree>;

  uuid_multipath();

  uuid_multipath(uuid id, bool is_receiver);

  uuid_multipath(const tree_ptr& t, uuid_multipath_node* h);

  explicit uuid_multipath(const tree_ptr& tptr)
    : uuid_multipath(tptr, tptr->root) {
    // nop
  }

  uuid_multipath(uuid_multipath&& other) noexcept = default;

  uuid_multipath(const uuid_multipath& other) = default;

  uuid_multipath& operator=(uuid_multipath&& other) noexcept = default;

  uuid_multipath& operator=(const uuid_multipath& other) = default;

  auto id() const noexcept {
    return head_->id_;
  }

  auto is_receiver() const noexcept {
    return head_->is_receiver_;
  }

  bool equals(const uuid_multipath& other) const noexcept;

  bool contains(uuid what) const noexcept;

  size_t num_nodes() const noexcept {
    return head_->down_.size();
  }

  template <class Inspector>
  friend bool inspect(Inspector& f, uuid_multipath& x) {
    if constexpr (Inspector::is_loading)
      return x.head_->load(x.tree_->mem, f);
    else
      return x.head_->save(f);
  }

private:
  auto emplace(uuid id, bool is_receiver) {
    return head_->down_.emplace(tree_->mem, id, is_receiver);
  }

  std::shared_ptr<uuid_multipath_tree> tree_;

  uuid_multipath_node* head_;
};

/// @relates multipath
inline bool operator==(const uuid_multipath& x, const uuid_multipath& y) {
  return x.equals(y);
}

/// @relates multipath
inline bool operator!=(const uuid_multipath& x, const uuid_multipath& y) {
  return !(x == y);
}

// -- benchmark utilities ------------------------------------------------------

class generator {
public:
  generator();

  broker::endpoint_id next_endpoint_id();

  static auto make_endpoint_id() {
    generator g;
    return g.next_endpoint_id();
  }

  broker::count next_count();

  caf::uuid next_uuid();

  std::string next_string(size_t length);

  broker::timestamp next_timestamp();

  // Generates events for one of three possible types:
  // 1. Trivial data consisting of a number and a string.
  // 2. More complex data that resembles a line in a conn.log.
  // 3. Large tables of size 100 by 10, filled with random strings.
  broker::data next_data(size_t event_type);

  static auto make_data(size_t event_type) {
    generator g;
    return g.next_data(event_type);
  }

private:
  std::minstd_rand rng_;
  broker::timestamp ts_;
};

void run_streaming_benchmark();
