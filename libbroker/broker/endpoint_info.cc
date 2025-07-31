#include "broker/endpoint_info.hh"

#include <caf/expected.hpp>
#include <caf/node_id.hpp>
#include <caf/uri.hpp>

#include "broker/data.hh"
#include "broker/format.hh"
#include "broker/internal/native.hh"
#include "broker/variant.hh"
#include "broker/variant_list.hh"

#include <format>

namespace broker {

bool convertible_to_endpoint_info(const data& src) {
  if (auto vec = get_if<vector>(src)) {
    return convertible_to_endpoint_info(*vec);
  } else {
    return false;
  }
}

template <class List>
bool convertible_to_endpoint_info_impl(const List& src) {
  // Types: <string, string, port, count>.
  // - Fields 1 can be none.
  // - Field 2 - 4 are either *all* none or all defined.
  if (contains<any_type, none, none, none>(src)
      || contains<any_type, std::string, port, count>(src)) {
    return can_convert_to<endpoint_id>(src[0]);
  }
  return false;
}

bool convertible_to_endpoint_info(const std::vector<data>& src) {
  return convertible_to_endpoint_info_impl(src);
}

bool convertible_to_endpoint_info(const variant& src) {
  return convertible_to_endpoint_info_impl(src.to_list());
}

template <class DataOrVariant>
bool convert_impl(const DataOrVariant& src, endpoint_info& dst) {
  // Types: <string, string, port, count>. Fields 1 can be none.
  // Field 2 -4 are either all none or all defined.
  auto&& xs = src.to_list();
  if (xs.size() != 4)
    return false;
  // Parse the node (field 1).
  if (xs[0].is_string()) {
    if (!convert(std::string{xs[0].to_string()}, dst.node))
      return false;
  } else if (xs[0].is_none()) {
    dst.node = endpoint_id{};
  } else {
    // Type mismatch.
    return false;
  }
  // Parse the network (fields 2 - 4).
  if (xs[1].is_none() && xs[2].is_none() && xs[3].is_none()) {
    dst.network = std::nullopt;
    return true;
  }
  if (xs[1].is_string() && xs[2].is_port() && xs[3].is_count()) {
    dst.network = network_info{};
    auto& net = *dst.network;
    net.address = xs[1].to_string();
    net.port = xs[2].to_port().number();
    net.retry = timeout::seconds{xs[3].to_count()};
    return true;
  }
  // Type mismatch.
  return false;
}

bool convert(const data& src, endpoint_info& dst) {
  return convert_impl(src, dst);
}

bool convert(const variant& src, endpoint_info& dst) {
  return convert_impl(src, dst);
}

bool convert(const endpoint_info& src, data& dst) {
  vector result;
  result.resize(4);
  if (src.node)
    result[0] = to_string(src.node);
  if (src.network) {
    result[1] = src.network->address;
    result[2] = port{src.network->port, port::protocol::tcp};
    result[3] = static_cast<count>(src.network->retry.count());
  }
  dst = std::move(result);
  return true;
}

void convert(const endpoint_info& src, std::string& dst) {
  if (auto& net = src.network) {
    dst = std::format("endpoint_info({}, *{})", src.node, *net);
  } else {
    dst = std::format("endpoint_info({}, none)", src.node);
  }
}

} // namespace broker
