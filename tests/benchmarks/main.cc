#include "main.hh"

#include "broker/configuration.hh"
#include "broker/data.hh"

#include <benchmark/benchmark.h>

#include <caf/init_global_meta_objects.hpp>

#include <cstdlib>

using namespace broker;
using namespace std::literals;

// -- custom types -------------------------------------------------------------

uuid_multipath_tree::uuid_multipath_tree(uuid id, bool is_receiver) {
  root = broker::detail::new_instance<uuid_multipath_node>(mem, id,
                                                           is_receiver);
}

uuid_multipath_tree::~uuid_multipath_tree() {
  // nop; we can simply "wink out" the tree structure.
}

uuid_multipath_group::~uuid_multipath_group() {
  delete first_;
}

bool uuid_multipath_group::equals(
  const uuid_multipath_group& other) const noexcept {
  auto eq = [](const auto& lhs, const auto& rhs) { return lhs.equals(rhs); };
  return std::equal(begin(), end(), other.begin(), other.end(), eq);
}

bool uuid_multipath_group::contains(uuid id) const noexcept {
  auto pred = [&id](const uuid_multipath_node& node) {
    return node.contains(id);
  };
  return std::any_of(begin(), end(), pred);
}

template <class MakeNewNode>
std::pair<uuid_multipath_node*, bool>
uuid_multipath_group::emplace_impl(uuid id, MakeNewNode make_new_node) {
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

std::pair<uuid_multipath_node*, bool>
uuid_multipath_group::emplace(detail::monotonic_buffer_resource& mem, uuid id,
                              bool is_receiver) {
  auto make_new_node = [&mem, id, is_receiver] {
    return broker::detail::new_instance<uuid_multipath_node>(mem, id,
                                                             is_receiver);
  };
  return emplace_impl(id, make_new_node);
}

bool uuid_multipath_group::emplace(uuid_multipath_node* new_node) {
  auto make_new_node = [new_node] { return new_node; };
  return emplace_impl(new_node->id_, make_new_node).second;
}

uuid_multipath_node::~uuid_multipath_node() {
  delete right_;
}

bool uuid_multipath_node::equals(
  const uuid_multipath_node& other) const noexcept {
  return id_ == other.id_ && down_.equals(other.down_);
}

bool uuid_multipath_node::contains(uuid what) const noexcept {
  return id_ == what || down_.contains(what);
}

uuid_multipath::uuid_multipath() {
  tree_ = std::make_shared<uuid_multipath_tree>(uuid{}, false);
  head_ = tree_->root;
}

uuid_multipath::uuid_multipath(uuid id, bool is_receiver) {
  tree_ = std::make_shared<uuid_multipath_tree>(id, is_receiver);
  head_ = tree_->root;
}

uuid_multipath::uuid_multipath(const tree_ptr& t, uuid_multipath_node* h)
  : tree_(t), head_(h) {
  // nop
}

bool uuid_multipath::equals(const uuid_multipath& other) const noexcept {
  return head_->equals(*other.head_);
}

bool uuid_multipath::contains(uuid what) const noexcept {
  return head_->contains(what);
}

// -- benchmark utilities ------------------------------------------------------

namespace {

struct vector_builder {
  vector* vec;
};

template <class T>
vector_builder&& operator<<(vector_builder&& builder, T&& value) {
  builder.vec->emplace_back(std::forward<T>(value));
  return std::move(builder);
}

template <class T>
vector_builder& operator<<(vector_builder& builder, T&& value) {
  builder.vec->emplace_back(std::forward<T>(value));
  return builder;
}

auto add_to(vector& vec) {
  return vector_builder{&vec};
}

timestamp brokergenesis() {
  // Broker started its life on Jul 9, 2014, 5:16 PM GMT+2 with the first commit
  // by Jon Siwek. This function returns a UNIX timestamp for that time.
  return clock::from_time_t(1404918960);
}

} // namespace

generator::generator() : rng_(0xB7E57), ts_(brokergenesis()) {
  // nop
}

endpoint_id generator::next_endpoint_id() {
  using array_type = caf::hashed_node_id::host_id_type;
  using value_type = array_type::value_type;
  std::uniform_int_distribution<> d{0, std::numeric_limits<value_type>::max()};
  array_type result;
  for (auto& x : result)
    x = static_cast<value_type>(d(rng_));
  return caf::make_node_id(d(rng_), result);
}

count generator::next_count() {
  std::uniform_int_distribution<count> d;
  return d(rng_);
}

caf::uuid generator::next_uuid() {
  return caf::uuid::random(rng_());
}

std::string generator::next_string(size_t length) {
  std::string_view charset = "0123456789"
                             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                             "abcdefghijklmnopqrstuvwxyz";
  std::uniform_int_distribution<size_t> d{0, charset.size() - 1};
  std::string result;
  result.resize(length);
  for (auto& c : result)
    c = charset[d(rng_)];
  return result;
}

timestamp generator::next_timestamp() {
  std::uniform_int_distribution<size_t> d{1, 100};
  ts_ += std::chrono::seconds(d(rng_));
  return ts_;
}

data generator::next_data(size_t event_type) {
  vector result;
  switch (event_type) {
    case 1: {
      add_to(result) << 42 << "test"s;
      break;
    }
    case 2: {
      address a1;
      address a2;
      convert("1.2.3.4", a1);
      convert("3.4.5.6", a2);
      add_to(result) << next_timestamp() << next_string(10)
                     << vector{a1, port(4567, port::protocol::tcp), a2,
                               port(80, port::protocol::tcp)}
                     << enum_value("tcp") << next_string(10)
                     << std::chrono::duration_cast<timespan>(3140ms)
                     << next_count() << next_count() << next_string(5) << true
                     << false << next_count() << next_string(10) << next_count()
                     << next_count() << next_count() << next_count()
                     << set({next_string(10), next_string(10)});
      break;
    }
    case 3: {
      table m;
      for (int i = 0; i < 100; ++i) {
        set s;
        for (int j = 0; j < 10; ++j)
          s.insert(next_string(5));
        m[next_string(15)] = std::move(s);
      }
      add_to(result) << next_timestamp() << std::move(m);
      break;
    }
    default: {
      std::cerr << "event type must be 1, 2, or 3; got " << event_type << '\n';
      throw std::logic_error("invalid event type");
    }
  }
  return data{std::move(result)};
}

int main(int argc, char** argv) {
  caf::init_global_meta_objects<caf::id_block::micro_benchmarks>();
  configuration::init_global_state();
  run_streaming_benchmark();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return EXIT_FAILURE;
  } else {
    benchmark::RunSpecifiedBenchmarks();
    return EXIT_SUCCESS;
  }
}
