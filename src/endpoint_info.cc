#include "broker/endpoint_info.hh"

#include <regex>

#include <caf/expected.hpp>
#include <caf/node_id.hpp>
#include <caf/uri.hpp>

#include "broker/data.hh"
#include "broker/data_view.hh"
#include "broker/internal/native.hh"

namespace broker {

bool convertible_to_endpoint_info(const data& src) {
  if (auto vec = get_if<vector>(src))
    return convertible_to_endpoint_info(*vec);
  return false;
}

bool convertible_to_endpoint_info(const std::vector<data>& src) {
  // Types: <string, string, port, count>.
  // - Fields 1 can be none.
  // - Field 2 - 4 are either *all* none or all defined.
  if (contains<any_type, none, none, none>(src)
      || contains<any_type, std::string, port, count>(src)) {
    return can_convert_to<endpoint_id>(src[0]);
  } else {
    return false;
  }
}

bool convertible_to_endpoint_info(const data_view& src) {
  return convertible_to_endpoint_info(src.to_vector());
}

bool convertible_to_endpoint_info(const vector_view& src) {
  // Types: <string, string, port, count>.
  // - Fields 1 can be none.
  // - Field 2 - 4 are either *all* none or all defined.
  if (contains<any_type, none, none, none>(src)
      || contains<any_type, std::string, port, count>(src)) {
    return can_convert_to<endpoint_id>(src[0]);
  } else {
    return false;
  }
}

template<class DataOrView>
bool convert_impl(const DataOrView& src, endpoint_info& dst) {
  if (!src.is_vector())
    return false;
  // Types: <string, string, port, count>. Fields 1 can be none.
  // Field 2 -4 are either all none or all defined.
  auto&& xs = src.to_vector();
  if (xs.size() != 4)
    return false;
  // Parse the node (field 1).
  auto&& node = xs[0];
  if (node.is_string()) {
    if (!dst.node.parse(node.to_string()))
      return false;
  } else if (node.is_none()) {
    dst.node = endpoint_id{};
  } else {
    // Type mismatch.
    return false;
  }
  // Parse the network (fields 2 - 4).
  auto&& net = xs[1];
  auto&& port = xs[2];
  auto&& retry = xs[3];
  if (net.is_none() && port.is_none() && retry.is_none()) {
    dst.network = std::nullopt;
    return true;
  }
  if (!net.is_string() || !port.is_port() || !retry.is_count())
    return false;
  dst.network = network_info{};
  auto& dst_net = *dst.network;
  dst_net.address = std::string{net.to_string()};
  dst_net.port = port.to_port().number();
  dst_net.retry = timeout::seconds{retry.to_count()};
  return true;
}

bool convert(const data& src, endpoint_info& dst) {
  return convert_impl(src, dst);
}

bool convert(const data_view& src, endpoint_info& dst) {
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
  dst += "endpoint_info(";
  dst += to_string(src.node);
  dst += ", ";
  if (auto& net = src.network) {
    dst += '*';
    dst += to_string(*net);
  } else {
    dst += "none";
  }
  dst += ')';
}

} // namespace broker
