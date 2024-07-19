#include "broker/data.hh"

#include <caf/hash/fnv.hpp>
#include <caf/node_id.hpp>

#include "broker/convert.hh"
#include "broker/expected.hh"
#include "broker/format/bin.hh"
#include "broker/format/txt.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"

using broker::internal::native;

namespace txt_v1 = broker::format::txt::v1;

namespace {

using namespace broker;

template <broker::data::type Type>
using data_variant_at_t =
  std::variant_alternative_t<static_cast<size_t>(Type), data_variant>;

// Make sure the static_cast in data::get_type is safe.
using std::is_same_v;
static_assert(is_same_v<none, data_variant_at_t<data::type::none>>);
static_assert(is_same_v<boolean, data_variant_at_t<data::type::boolean>>);
static_assert(is_same_v<count, data_variant_at_t<data::type::count>>);
static_assert(is_same_v<integer, data_variant_at_t<data::type::integer>>);
static_assert(is_same_v<real, data_variant_at_t<data::type::real>>);
static_assert(is_same_v<std::string, data_variant_at_t<data::type::string>>);
static_assert(is_same_v<address, data_variant_at_t<data::type::address>>);
static_assert(is_same_v<subnet, data_variant_at_t<data::type::subnet>>);
static_assert(is_same_v<port, data_variant_at_t<data::type::port>>);
static_assert(is_same_v<timestamp, data_variant_at_t<data::type::timestamp>>);
static_assert(is_same_v<timespan, data_variant_at_t<data::type::timespan>>);
static_assert(is_same_v<enum_value, data_variant_at_t<data::type::enum_value>>);
static_assert(is_same_v<set, data_variant_at_t<data::type::set>>);
static_assert(is_same_v<table, data_variant_at_t<data::type::table>>);
static_assert(is_same_v<vector, data_variant_at_t<data::type::vector>>);

} // namespace

namespace broker {

data::type data::get_type() const {
  return static_cast<data::type>(data_.index());
}

data data::from_type(data::type t) {
  switch (t) {
    case data::type::address:
      return broker::address{};
    case data::type::boolean:
      return broker::boolean{};
    case data::type::count:
      return broker::count{};
    case data::type::enum_value:
      return broker::enum_value{};
    case data::type::integer:
      return broker::integer{};
    case data::type::none:
      return broker::data{};
    case data::type::port:
      return broker::port{};
    case data::type::real:
      return broker::real{};
    case data::type::set:
      return broker::set{};
    case data::type::string:
      return std::string{};
    case data::type::subnet:
      return broker::subnet{};
    case data::type::table:
      return broker::table{};
    case data::type::timespan:
      return broker::timespan{};
    case data::type::timestamp:
      return broker::timestamp{};
    case data::type::vector:
      return broker::vector{};
    default:
      return data{};
  }
}

namespace {

vector empty_vector;

enum_value empty_enum_value;

} // namespace

const enum_value& data::to_enum_value() const noexcept {
  if (auto* val = std::get_if<enum_value>(&data_))
    return *val;
  return empty_enum_value;
}

const vector& data::to_list() const {
  if (auto ptr = std::get_if<vector>(&data_))
    return *ptr;
  return empty_vector;
}

namespace {

// Consumes events from a decoder and produces a data object.
struct decoder_handler {
  using kvp_t = std::pair<data, data>;

  // Marker that we expect the key for a table entry next.
  struct key_t {};

  // Marker that we expect the value for a table entry next.
  struct val_t {
    data key;
  };

  using frame = std::variant<data, vector, set, table, key_t, val_t>;

  std::vector<frame> stack;

  decoder_handler() {
    stack.emplace_back(data{});
  }

  // Assigns a value to a data object, converting as necessary.
  template <class T>
  static void assign(data& dst, const T& arg) {
    if constexpr (std::is_same_v<T, std::string_view>) {
      dst = std::string{arg};
    } else if constexpr (std::is_same_v<T, enum_value_view>) {
      dst = enum_value{std::string{arg.name}};
    } else {
      dst = arg;
    }
  }

  template <class T>
  bool value(const T& arg) {
    if (stack.empty()) {
      return false;
    }
    auto fn = [this, &arg](auto& dst) {
      using dst_t = std::decay_t<decltype(dst)>;
      if constexpr (std::is_same_v<dst_t, data>) {
        // Assign directly to a data object.
        assign(dst, arg);
        return true;
      } else if constexpr (std::is_same_v<dst_t, vector>) {
        auto& item = dst.emplace_back();
        assign(item, arg);
        return true;
      } else if constexpr (std::is_same_v<dst_t, set>) {
        data item;
        assign(item, arg);
        dst.insert(std::move(item));
        return true;
      } else if constexpr (std::is_same_v<dst_t, table>) {
        // We expect a key-value pair on the stack, not the table itself.
        return false;
      } else if constexpr (std::is_same_v<dst_t, key_t>) {
        // Assign to the key of a key-value pair.
        stack.pop_back();
        stack.emplace_back(val_t{});
        assign(std::get<val_t>(stack.back()).key, arg);
        return true;
      } else {
        static_assert(std::is_same_v<dst_t, val_t>);
        // Assign to the value of a key-value pair and add it to the table (the
        // frame below the current one).
        data key = std::move(std::get<val_t>(stack.back()).key);
        stack.pop_back();
        if (stack.empty() || !std::holds_alternative<table>(stack.back())) {
          return false;
        }
        data val;
        assign(val, arg);
        std::get<table>(stack.back()).emplace(std::move(key), std::move(val));
        return true;
      }
    };
    return std::visit(fn, stack.back());
  }

  // Pushes a list, set, or table onto the current frame.
  template <class T>
  bool push(T& arg) {
    auto fn = [this, &arg](auto& dst) {
      using dst_t = std::decay_t<decltype(dst)>;
      if constexpr (std::is_same_v<dst_t, data>) {
        // Assigning a container to a data object. The object must be empty,
        // otherwise it has been assigned to before.
        if (!dst.is_none()) {
          return false;
        }
        dst = std::move(arg);
        return true;
      } else if constexpr (std::is_same_v<dst_t, vector>) {
        dst.emplace_back(std::move(arg));
        return true;
      } else if constexpr (std::is_same_v<dst_t, set>) {
        dst.insert(data{std::move(arg)});
        return true;
      } else if constexpr (std::is_same_v<dst_t, table>) {
        return false;
      } else if constexpr (std::is_same_v<dst_t, key_t>) {
        // Assign to the key of a key-value pair.
        stack.pop_back();
        stack.emplace_back(val_t{});
        std::get<val_t>(stack.back()).key = std::move(arg);
        return true;
      } else {
        static_assert(std::is_same_v<dst_t, val_t>);
        // Assign to the value of a key-value pair and add it to the table (the
        // frame below the current one).
        data key = std::move(std::get<val_t>(stack.back()).key);
        stack.pop_back();
        if (stack.empty() || !std::holds_alternative<table>(stack.back())) {
          return false;
        }
        std::get<table>(stack.back())
          .emplace(std::move(key), data{std::move(arg)});
        return true;
      }
    };
    return std::visit(fn, stack.back());
  }

  bool begin_list() {
    stack.emplace_back(vector{});
    return true;
  }

  bool end_list() {
    // We must always have at least two frames on the stack: the list we have
    // just produced and the parent frame that the list belongs to.
    if (stack.size() < 2 || !std::holds_alternative<vector>(stack.back())) {
      return false;
    }
    vector result = std::move(std::get<vector>(stack.back()));
    stack.pop_back();
    return push(result);
  }

  bool begin_set() {
    stack.emplace_back(set{});
    return true;
  }

  bool end_set() {
    // Similar to end_list, but for sets.
    if (stack.size() < 2 || !std::holds_alternative<set>(stack.back())) {
      return false;
    }
    set result = std::move(std::get<set>(stack.back()));
    stack.pop_back();
    return push(result);
  }

  bool begin_table() {
    stack.emplace_back(table{});
    return true;
  }

  bool end_table() {
    // Similar to end_list, but for tables.
    if (stack.size() < 2 || !std::holds_alternative<table>(stack.back())) {
      return false;
    }
    table result = std::move(std::get<table>(stack.back()));
    stack.pop_back();
    return push(result);
  }

  bool begin_key_value_pair() {
    stack.emplace_back(key_t{});
    return true;
  }

  bool end_key_value_pair() {
    return true;
  }
};

} // namespace

bool data::deserialize(const std::byte* payload, size_t payload_size) {
  decoder_handler handler;
  auto payload_end = payload + payload_size;
  auto [ok, pos] = format::bin::v1::decode(payload, payload_end, handler);
  if (!ok || pos != payload_end || handler.stack.size() != 1) {
    return false;
  }
  *this = std::move(std::get<data>(handler.stack.back()));
  return true;
}

namespace {

template <class Container>
void container_convert(Container& c, std::string& str, char left, char right) {
  constexpr auto* delim = ", ";
  auto first = begin(c);
  auto last = end(c);
  str += left;
  if (first != last) {
    str += to_string(*first);
    while (++first != last)
      str += delim + to_string(*first);
  }
  str += right;
}

struct data_converter {
  template <class T>
  void operator()(const T& x) {
    using std::to_string;
    str += to_string(x);
  }

  void operator()(timespan ts) {
    convert(ts.count(), str);
    str += "ns";
  }

  void operator()(timestamp ts) {
    (*this)(ts.time_since_epoch());
  }

  void operator()(bool b) {
    str = b ? 'T' : 'F';
  }

  void operator()(const std::string& x) {
    str = x;
  }

  std::string& str;
};

} // namespace

void convert(const table::value_type& x, std::string& str) {
  str += to_string(x.first) + " -> " + to_string(x.second);
}

void convert(const vector& x, std::string& str) {
  txt_v1::encode(x, std::back_inserter(str));
}

void convert(const set& x, std::string& str) {
  txt_v1::encode(x, std::back_inserter(str));
}

void convert(const table& x, std::string& str) {
  txt_v1::encode(x, std::back_inserter(str));
}

void convert(const data& x, std::string& str) {
  txt_v1::encode(x, std::back_inserter(str));
}

bool convert(const data& x, endpoint_id& node) {
  return is<std::string>(x) && convert(get<std::string>(x), node);
}

bool convert(const endpoint_id& node, data& d) {
  if (node)
    d = to_string(node);
  else
    d = nil;
  return true;
}

std::string to_string(const expected<data>& x) {
  if (x)
    return to_string(*x);
  else
    return "!" + to_string(x.error());
}

} // namespace broker

namespace broker::detail {

size_t fnv_hash(const broker::data& x) {
  return caf::hash::fnv<size_t>::compute(x);
}

size_t fnv_hash(const broker::set& x) {
  return caf::hash::fnv<size_t>::compute(x);
}

size_t fnv_hash(const broker::vector& x) {
  return caf::hash::fnv<size_t>::compute(x);
}

size_t fnv_hash(const broker::table::value_type& x) {
  return caf::hash::fnv<size_t>::compute(x);
}

size_t fnv_hash(const broker::table& x) {
  return caf::hash::fnv<size_t>::compute(x);
}

} // namespace broker::detail
