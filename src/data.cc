#include "broker/data.hh"

#include <caf/hash/fnv.hpp>
#include <caf/node_id.hpp>

#include "broker/convert.hh"

namespace {

using namespace broker;

constexpr const char* data_type_names[] = {
  "none",     "boolean",    "count",  "integer", "real",
  "string",   "address",    "subnet", "port",    "timestamp",
  "timespan", "enum_value", "set",    "table",   "vector",
};

constexpr int ival_of(broker::data::type x) {
  return static_cast<int>(x);
}

template <class T>
constexpr int pos_of() {
  return caf::detail::tl_index_of<data::types, T>::value;
}

// Make sure the static_cast in data::get_type is safe.
static_assert(ival_of(data::type::none) == pos_of<none>());
static_assert(ival_of(data::type::boolean) == pos_of<boolean>());
static_assert(ival_of(data::type::count) == pos_of<count>());
static_assert(ival_of(data::type::integer) == pos_of<integer>());
static_assert(ival_of(data::type::real) == pos_of<real>());
static_assert(ival_of(data::type::string) == pos_of<std::string>());
static_assert(ival_of(data::type::address) == pos_of<address>());
static_assert(ival_of(data::type::subnet) == pos_of<subnet>());
static_assert(ival_of(data::type::port) == pos_of<port>());
static_assert(ival_of(data::type::timestamp) == pos_of<timestamp>());
static_assert(ival_of(data::type::timespan) == pos_of<timespan>());
static_assert(ival_of(data::type::enum_value) == pos_of<enum_value>());
static_assert(ival_of(data::type::set) == pos_of<set>());
static_assert(ival_of(data::type::table) == pos_of<table>());
static_assert(ival_of(data::type::vector) == pos_of<vector>());

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
void container_convert(Container& c, std::string& str,
                       const char* left, const char* right,
                       const char* delim = ", ") {
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
  using result_type = bool;

  template <class T>
  result_type operator()(const T& x) {
    return convert(x, str);
  }

  result_type operator()(timespan ts) {
    if (convert(ts.count(), str)) {
      str += "ns";
      return true;
    } else {
      return false;
    }
  }

  result_type operator()(timestamp ts) {
    return (*this)(ts.time_since_epoch());
  }

  result_type operator()(bool b) {
    str = b ? 'T' : 'F';
    return true;
  }

  result_type operator()(const std::string& x) {
    str = x;
    return true;
  }

  std::string& str;
};

} // namespace <anonymous>

bool convert(const table::value_type& e, std::string& str) {
  str += to_string(e.first) + " -> " + to_string(e.second);
  return true;
}

bool convert(const vector& v, std::string& str) {
  container_convert(v, str, "(", ")");
  return true;
}

bool convert(const set& s, std::string& str) {
  container_convert(s, str, "{", "}");
  return true;
}

bool convert(const table& t, std::string& str) {
  container_convert(t, str, "{", "}");
  return true;
}

bool convert(const data& d, std::string& str) {
  caf::visit(data_converter{str}, d);
  return true;
}

bool convert(const data& d, endpoint_id& node){
  if (is<std::string>(d))
    if (auto err = caf::parse(get<std::string>(d), node); !err)
      return true;
  return false;
}

bool convert(const endpoint_id& node, data& d) {
  if (node)
    d = to_string(node);
  else
    d = nil;
  return true;
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
