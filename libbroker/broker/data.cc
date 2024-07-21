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

// Assigns a value to a data object, converting as necessary.
template <class T>
void do_assign(data& dst, const T& arg) {
  if constexpr (std::is_same_v<T, std::string_view>) {
    dst = std::string{arg};
  } else if constexpr (std::is_same_v<T, enum_value_view>) {
    dst = enum_value{std::string{arg.name}};
  } else {
    dst = arg;
  }
}

struct decoder_handler_value;

struct decoder_handler_list;

struct decoder_handler_set;

struct decoder_handler_table;

// Consumes events from a decoder and produces a data object.
struct decoder_handler_value {
  data result;

  template <class T>
  void value(const T& arg) {
    do_assign(result, arg);
  }

  decoder_handler_list begin_list();

  void end_list(decoder_handler_list&);

  decoder_handler_set begin_set();

  void end_set(decoder_handler_set&);

  decoder_handler_table begin_table();

  void end_table(decoder_handler_table&);
};

// Consumes events from a decoder and produces a list of data objects.
struct decoder_handler_list {
  vector result;

  template <class T>
  void value(const T& arg) {
    do_assign(result.emplace_back(), arg);
  }

  decoder_handler_list begin_list() {
    return {};
  }

  void end_list(decoder_handler_list& other) {
    result.emplace_back() = std::move(other.result);
  }

  decoder_handler_set begin_set();

  void end_set(decoder_handler_set&);

  decoder_handler_table begin_table();

  void end_table(decoder_handler_table&);
};

// Consumes events from a decoder and produces a set of data objects.
struct decoder_handler_set {
  set result;

  template <class T>
  void value(const T& arg) {
    data item;
    do_assign(item, arg);
    result.insert(std::move(item));
  }

  decoder_handler_list begin_list() {
    return {};
  }

  void end_list(decoder_handler_list& other) {
    result.insert(data{std::move(other.result)});
  }

  decoder_handler_set begin_set() {
    return {};
  }

  void end_set(decoder_handler_set& other) {
    result.insert(data{std::move(other.result)});
  }

  decoder_handler_table begin_table();

  void end_table(decoder_handler_table&);
};

struct decoder_handler_table {
  table result;
  std::optional<data> key;

  void add(data&& arg) {
    if (!key) {
      key.emplace(std::move(arg));
    } else {
      result.emplace(std::move(*key), std::move(arg));
      key.reset();
    }
  }

  template <class T>
  void value(const T& arg) {
    data val;
    do_assign(val, arg);
    add(std::move(val));
  }

  decoder_handler_list begin_list() {
    return {};
  }

  void end_list(decoder_handler_list& other) {
    add(data{std::move(other.result)});
  }

  decoder_handler_set begin_set() {
    return {};
  }

  void end_set(decoder_handler_set& other) {
    add(data{std::move(other.result)});
  }

  decoder_handler_table begin_table() {
    return {};
  }

  void end_table(decoder_handler_table& other) {
    add(data{std::move(other.result)});
  }

  void begin_key_value_pair() {
    // nop
  }

  void end_key_value_pair() {
    // nop
  }
};

decoder_handler_list decoder_handler_value::begin_list() {
  return {};
}

void decoder_handler_value::end_list(decoder_handler_list& other) {
  result = std::move(other.result);
}

decoder_handler_set decoder_handler_value::begin_set() {
  return {};
}

void decoder_handler_value::end_set(decoder_handler_set& other) {
  result = std::move(other.result);
}

decoder_handler_table decoder_handler_value::begin_table() {
  return {};
}

void decoder_handler_value::end_table(decoder_handler_table& other) {
  result = std::move(other.result);
}

decoder_handler_set decoder_handler_list::begin_set() {
  return {};
}

void decoder_handler_list::end_set(decoder_handler_set& other) {
  result.emplace_back(std::move(other.result));
}

decoder_handler_table decoder_handler_list::begin_table() {
  return {};
}

void decoder_handler_list::end_table(decoder_handler_table& other) {
  result.emplace_back(std::move(other.result));
}

decoder_handler_table decoder_handler_set::begin_table() {
  return {};
}

void decoder_handler_set::end_table(decoder_handler_table& other) {
  result.insert(data{std::move(other.result)});
}

} // namespace

bool data::deserialize(const std::byte* payload, size_t payload_size) {
  decoder_handler_value handler;
  auto payload_end = payload + payload_size;
  auto [ok, pos] = format::bin::v1::decode(payload, payload_end, handler);
  if (!ok || pos != payload_end) {
    return false;
  }
  *this = std::move(handler.result);
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
