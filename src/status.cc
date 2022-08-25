#include "broker/status.hh"

#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/error.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"

using namespace std::literals;

namespace broker {

namespace {

std::string_view sc_strings[] = {
  "unspecified"sv,         //
  "peer_added"sv,          //
  "peer_removed"sv,        //
  "peer_lost"sv,           //
  "endpoint_discovered"sv, //
  "endpoint_unreachable"sv,

};

} // namespace

std::string to_string(sc code) {
  auto index = static_cast<uint8_t>(code);
  BROKER_ASSERT(index < std::size(sc_strings));
  return std::string{sc_strings[index]};
}

bool convert(std::string_view str, sc& code) noexcept {
  return default_enum_convert(sc_strings, str, code);
}

bool convert(const data& src, sc& code) noexcept {
  if (auto val = get_if<enum_value>(src)) {
    return convert(val->name, code);
  } else {
    return false;
  }
}

bool convertible_to_sc(const data& src) noexcept {
  sc dummy;
  return convert(src, dummy);
}

sc status::code() const {
  return code_;
}

error status::verify() const {
  switch (code_) {
    default:
      return make_error(ec::invalid_status, "invalid enum value");
    case sc::unspecified:
      if (!context_.node && !context_.network)
        return {};
      else
        return make_error(ec::invalid_status,
                          "an unspecified status may not have a context");
    case sc::peer_added:
    case sc::peer_removed:
    case sc::peer_lost:
    case sc::endpoint_discovered:
    case sc::endpoint_unreachable:
      if (context_.node)
        return {};
      else
        return make_error(ec::invalid_status,
                          "a non-default status must provide a node ID");
  }
}

bool operator==(const status& x, const status& y) {
  return x.code_ == y.code_;
}

bool operator==(const status& x, sc y) {
  return x.code() == y;
}

bool operator==(sc x, const status& y) {
  return y == x;
}

namespace {

template <class StatusOrView>
std::string status_to_string_impl(const StatusOrView& x) {
  std::string result = to_string(x.code());
  result += '(';
  if (auto ctx = x.context()) {
    result += to_string(ctx->node);
    if (ctx->network) {
      result += ", ";
      result += to_string(*ctx->network);
    }
    result += ", ";
  }
  result += '"';
  result += *x.message();
  result += "\")";
  return result;
}

} // namespace

std::string to_string(const status& x) {
  return status_to_string_impl(x);
}

std::string to_string(status_view x) {
  return status_to_string_impl(x);
}

bool convertible_to_status(const vector& xs) noexcept {
  if (xs.size() != 4)
    return false;
  if (auto str = get_if<std::string>(xs[0]); !str || *str != "status")
    return false;
  if (auto code = to<sc>(xs[1]))
    return *code != sc::unspecified
             ? contains<any_type, any_type, endpoint_info, std::string>(xs)
             : contains<any_type, any_type, none, none>(xs);
  return false;
}

bool convertible_to_status(const data& src) noexcept {
  if (auto xs = get_if<vector>(src))
    return convertible_to_status(*xs);
  return false;
}

bool convert(const data& src, status& dst) {
  if (!convertible_to_status(src))
    return false;
  auto& xs = get<vector>(src);
  if (!convert(get<enum_value>(xs[1]).name, dst.code_))
    return false;
  if (dst.code_ != sc::unspecified) {
    if (convert(get<vector>(xs[2]), dst.context_)) {
      dst.message_ = get<std::string>(xs[3]);
      return true;
    }
  } else {
    dst.context_ = endpoint_info{};
    dst.message_.clear();
    return true;
  }
  return false;
}

bool convert(const status& src, data& dst) {
  vector result;
  result.resize(4);
  result[0] = "status"s;
  result[1] = enum_value{to_string(src.code_)};
  if (src.code_ != sc::unspecified) {
    if (!convert(src.context_, result[2]))
      return false;
    result[3] = src.message_;
  }
  dst = std::move(result);
  return true;
}

sc status_view::code() const {
  BROKER_ASSERT(xs_ != nullptr);
  return get_as<sc>((*xs_)[1]);
}

const std::string* status_view::message() const {
  BROKER_ASSERT(xs_ != nullptr);
  if (is<none>((*xs_)[3]))
    return nullptr;
  else
    return get_if<std::string>((*xs_)[3]);
}

std::optional<endpoint_info> status_view::context() const {
  BROKER_ASSERT(xs_ != nullptr);
  endpoint_info ei;
  if (!convert((*xs_)[2], ei))
    return std::nullopt;
  else
    return {std::move(ei)};
}

status_view status_view::make(const data& src) {
  return status_view{convertible_to_status(src) ? &get<vector>(src) : nullptr};
}

} // namespace broker
