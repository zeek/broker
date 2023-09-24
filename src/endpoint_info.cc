#include "broker/endpoint_info.hh"

#include <regex>

#include <caf/expected.hpp>
#include <caf/node_id.hpp>
#include <caf/uri.hpp>

#include "broker/data.hh"
#include "broker/internal/native.hh"
#include "broker/variant.hh"
#include "broker/variant_list.hh"

namespace broker {

bool convertible_to_endpoint_info(const data& src) {
  if (auto vec = get_if<vector>(src)) {
    return convertible_to_endpoint_info(*vec);
  } else {
    return false;
  }
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

bool convertible_to_endpoint_info(const variant& src) {
  return convertible_to_endpoint_info(src.to_list());
}

bool convertible_to_endpoint_info(const variant_list& src) {
  // Types: <string, string, port, count>.
  // - Fields 1 can be none.
  // - Field 2 - 4 are either *all* none or all defined.
  if (contains<any_type, none, none, none>(src)
      || contains<any_type, std::string, port, count>(src)) {
    return can_convert_to<endpoint_id>(src.front());
  } else {
    return false;
  }
}

bool convert(const data& src, endpoint_info& dst) {
  if (!is<vector>(src))
    return false;
  // Types: <string, string, port, count>. Fields 1 can be none.
  // Field 2 -4 are either all none or all defined.
  auto& xs = get<vector>(src);
  if (xs.size() != 4)
    return false;
  // Parse the node (field 1).
  if (auto str = get_if<std::string>(xs[0])) {
    if (!convert(*str, dst.node))
      return false;
  } else if (is<none>(xs[0])) {
    dst.node = endpoint_id{};
  } else {
    // Type mismatch.
    return false;
  }
  // Parse the network (fields 2 - 4).
  if (is<none>(xs[1]) && is<none>(xs[2]) && is<none>(xs[3])) {
    dst.network = std::nullopt;
  } else if (is<std::string>(xs[1]) && is<port>(xs[2]) && is<count>(xs[3])) {
    dst.network = network_info{};
    auto& net = *dst.network;
    net.address = get<std::string>(xs[1]);
    net.port = get<port>(xs[2]).number();
    net.retry = timeout::seconds{get<count>(xs[3])};
  } else {
    // Type mismatch.
    return false;
  }
  return true;
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
