#include "broker/data.hh"

#include <caf/hash/fnv.hpp>
#include <caf/node_id.hpp>

#include "broker/convert.hh"
#include "broker/expected.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"

using broker::internal::native;

namespace {

using namespace broker;

constexpr const char* data_type_names[] = {
  "none",     "boolean",    "count",  "integer", "real",
  "string",   "address",    "subnet", "port",    "timestamp",
  "timespan", "enum_value", "set",    "table",   "vector",
};

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

const char* data::get_type_name() const {
  return data_type_names[data_.index()];
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
  container_convert(x, str, '(', ')');
}

void convert(const set& x, std::string& str) {
  container_convert(x, str, '{', '}');
}

void convert(const table& x, std::string& str) {
  container_convert(x, str, '{', '}');
}

void convert(const data& x, std::string& str) {
  visit(data_converter{str}, x);
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
