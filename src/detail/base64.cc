#include "broker/detail/base64.hh"

#include <cstdint>

namespace broker::detail {

// clang-format off

namespace {

constexpr uint8_t decoding_tbl[] = {
/*       ..0 ..1 ..2 ..3 ..4 ..5 ..6 ..7 ..8 ..9 ..A ..B ..C ..D ..E ..F  */
/* 0.. */  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
/* 1.. */  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
/* 2.. */  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 62,  0,  0,  0, 63,
/* 3.. */ 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,  0,  0,  0,  0,  0,  0,
/* 4.. */  0,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
/* 5.. */ 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,  0,  0,  0,  0,  0,
/* 6.. */  0, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
/* 7.. */ 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,  0,  0,  0,  0,  0};

constexpr const char encoding_tbl[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                      "abcdefghijklmnopqrstuvwxyz"
                                      "0123456789+/";

} // namespace

// clang-format on

void base64::encode(const_blob_span in, std::string& out) {
  using byte = blob_buffer::value_type;
  // Consumes three characters from input at once.
  auto consume = [&out](const byte* i) {
    auto at = [i](size_t index) { return static_cast<int>(i[index]); };
    int buf[] = {
      (at(0) & 0xfc) >> 2,
      ((at(0) & 0x03) << 4) + ((at(1) & 0xf0) >> 4),
      ((at(1) & 0x0f) << 2) + ((at(2) & 0xc0) >> 6),
      at(2) & 0x3f,
    };
    for (auto x : buf)
      out += encoding_tbl[x];
  };
  // Iterate the input in chunks of three bytes.
  auto i = in.begin();
  for (; std::distance(i, in.end()) >= 3; i += 3)
    consume(i);
  if (i != in.end()) {
    // Pad input with zeros.
    byte buf[] = {byte{0}, byte{0}, byte{0}};
    std::copy(i, in.end(), buf);
    consume(buf);
    // Override padded bytes (garbage) with '='.
    for (auto j = out.end() - (3 - (in.size() % 3)); j != out.end(); ++j)
      *j = '=';
  }
}

bool base64::decode(std::string_view in, blob_buffer& out) {
  using byte = blob_buffer::value_type;
  // Short-circuit empty buffers.
  if (in.empty())
    return true;
  // Base64 always produces character groups of size 4.
  if (in.size() % 4 != 0)
    return false;
  // Consume four characters from the input at once.
  auto val = [](char c) { return decoding_tbl[c & 0x7F]; };
  for (auto i = in.begin(); i != in.end(); i += 4) {
    // clang-format off
    auto bits =   (val(i[0]) << 18)
                | (val(i[1]) << 12)
                | (val(i[2]) << 6)
                | (val(i[3]));
    // clang-format on
    out.emplace_back(static_cast<byte>((bits & 0xFF0000) >> 16));
    out.emplace_back(static_cast<byte>((bits & 0x00FF00) >> 8));
    out.emplace_back(static_cast<byte>((bits & 0x0000FF)));
  }
  // Fix up the output buffer when the input contained padding.
  auto s = in.size();
  if (in[s - 2] == '=') {
    out.pop_back();
    out.pop_back();
  } else if (in[s - 1] == '=') {
    out.pop_back();
  }
  return true;
}

} // namespace broker::detail
