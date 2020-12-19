#pragma once

#include <string>
#include <string_view>

#include "broker/detail/blob.hh"

namespace broker::detail {

class base64 {
public:
  static void encode(const_blob_span in, std::string& out);
  static bool decode(std::string_view in, blob_buffer& out);
};

} // namespace broker::detail
