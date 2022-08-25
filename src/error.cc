#include "broker/error.hh"

#include "broker/data.hh"
#include "broker/detail/assert.hh"
#include "broker/endpoint_info.hh"
#include "broker/internal/native.hh"
#include "broker/internal/type_id.hh"

#include "caf/const_typed_message_view.hpp"

static_assert(sizeof(caf::error) == sizeof(broker::error::impl*));
static_assert(std::is_same_v<caf::type_id_t, uint16_t>);

using namespace std::string_literals;

namespace broker {

namespace {

using internal::native;
using native_t = caf::error;

constexpr std::string_view ec_names[] = {
  "none",
  "unspecified",
  "peer_incompatible",
  "peer_invalid",
  "peer_unavailable",
  "peer_disconnect_during_handshake",
  "peer_timeout",
  "master_exists",
  "no_such_master",
  "no_such_key",
  "request_timeout",
  "type_clash",
  "invalid_data",
  "backend_failure",
  "stale_data",
  "cannot_open_file",
  "cannot_write_file",
  "invalid_topic_key",
  "end_of_file",
  "invalid_tag",
  "invalid_message",
  "invalid_status",
  "conversion_failed",
  "consumer_exists",
  "connection_timeout",
  "bad_member_function_call",
  "repeated_request_id",
  "broken_clone",
  "shutting_down",
  "invalid_peering_request",
  "repeated_peering_handshake_request",
  "unexpected_handshake_message",
  "invalid_handshake_state",
  "no_path_to_peer",
  "no_connector_available",
  "cannot_open_resource",
  "serialization_failed",
  "deserialization_failed",
  "wrong_magic_number",
  "redundant_connection",
  "logic_error",
};

template <class T, size_t N>
constexpr size_t array_size(const T (&)[N]) {
  return N;
}

} // namespace

uint16_t ec_category() noexcept {
  return caf::type_id_v<ec>;
}

error::error() {
  new (obj_) native_t();
}

error::error(ec code) {
  new (obj_) native_t(code);
}

error::error(ec code, std::string description) {
  new (obj_) native_t(code, caf::make_message(std::move(description)));
}

error::error(ec code, endpoint_info info, std::string description) {
  new (obj_)
    native_t(code, caf::make_message(std::move(info), std::move(description)));
}

error::error(const impl* other) {
  new (obj_) native_t(native(other));
}

error::error(const error& other) {
  new (obj_) native_t(native(other));
}

error::error(error&& other) noexcept {
  new (obj_) native_t(std::move(native(other)));
}

error& error::operator=(const error& other) {
  if (this != &other)
    native(*this) = native(other);
  return *this;
}

error& error::operator=(error&& other) noexcept {
  native(*this) = std::move(native(other));
  return *this;
}

error::~error() {
  native(*this).~native_t();
}

bool error::valid() const noexcept {
  return static_cast<bool>(native(*this));
}

uint8_t error::code() const noexcept {
  return native(*this).code();
}

uint16_t error::category() const noexcept {
  return native(*this).category();
}

const std::string* error::message() const noexcept {
  auto& msg = native(*this).context();
  if (auto v1 =
        caf::make_const_typed_message_view<endpoint_info, std::string>(msg))
    return std::addressof(get<1>(v1));
  else if (auto v2 = caf::make_const_typed_message_view<std::string>(msg))
    return std::addressof(get<0>(v2));
  else
    return nullptr;
}

const endpoint_info* error::context() const noexcept {
  auto& msg = native(*this).context();
  if (auto v = caf::make_const_typed_message_view<endpoint_info>(msg))
    return std::addressof(get<0>(v));
  else
    return nullptr;
}

error::impl* error::native_ptr() noexcept {
  return reinterpret_cast<impl*>(obj_);
}

const error::impl* error::native_ptr() const noexcept {
  return reinterpret_cast<const impl*>(obj_);
}

int error::compare(const error& other) const noexcept {
  return native(*this).compare(native(other));
}

int error::compare(uint8_t code, uint16_t category) const noexcept {
  return native(*this).compare(code, category);
}

void convert(const error& in, std::string& out) {
  out = caf::to_string(native(in));
}

error make_error(ec code, endpoint_info info, std::string description) {
  return error{code, std::move(info), std::move(description)};
}

std::string to_string(ec code) {
  auto index = static_cast<uint8_t>(code);
  BROKER_ASSERT(index < array_size(ec_names));
  return std::string{ec_names[index]};
}

std::string_view enum_str(ec code) {
  auto index = static_cast<uint8_t>(code);
  BROKER_ASSERT(index < array_size(ec_names));
  return ec_names[index];
}

bool convert(std::string_view str, ec& code) noexcept {
  return default_enum_convert(ec_names, str, code);
}

bool convert(const data& src, ec& code) noexcept {
  if (auto val = get_if<enum_value>(src))
    return convert(val->name, code);
  return false;
}

bool convertible_to_ec(const data& src) noexcept {
  ec dummy;
  return convert(src, dummy);
}

bool convertible_to_ec(uint8_t src) noexcept {
  return src < std::size(ec_names);
}

bool convertible_to_error(const vector& xs) noexcept {
  if (!contains<std::string, ec, any_type>(xs)) {
    // There is one special case: default errors with enum value "none" fail to
    // convert to ec but are still legal.
    if (contains<std::string, enum_value, none>(xs))
      return get<std::string>(xs[0]) == "error"
             && get<enum_value>(xs[1]).name == "none";
    return false;
  }
  if (get<std::string>(xs[0]) != "error")
    return false;
  return is<none>(xs[2]) || contains<std::string>(xs[2])
         || contains<endpoint_info, std::string>(xs[2]);
}

bool convertible_to_error(const data& src) noexcept {
  if (auto xs = get_if<vector>(src))
    return convertible_to_error(*xs);
  return false;
}

bool convert(const error& src, data& dst) {
  if (!src) {
    vector result;
    result.resize(3);
    result[0] = "error"s;
    result[1] = enum_value{"none"};
    dst = std::move(result);
    return true;
  }
  if (src.category() != caf::type_id_v<broker::ec>)
    return false;
  vector result;
  result.resize(3);
  result[0] = "error"s;
  result[1] = enum_value{to_string(static_cast<ec>(src.code()))};
  auto& context = native(src).context();
  if (context.empty()) {
    dst = std::move(result);
    return true;
  }
  if (context.match_elements<std::string>()) {
    result[2] = vector{context.get_as<std::string>(0)};
    dst = std::move(result);
    return true;
  }
  if (context.match_elements<endpoint_info, std::string>()) {
    vector xs;
    xs.resize(2);
    if (!convert(context.get_as<endpoint_info>(0), xs[0]))
      return false;
    xs[1] = context.get_as<std::string>(1);
    result[2] = std::move(xs);
    dst = std::move(result);
    return true;
  }
  return false;
}

bool convert(const data& src, error& dst) {
  if (!convertible_to_error(src))
    return false;
  auto& xs = get<vector>(src);
  if (get<enum_value>(xs[1]).name == "none") {
    dst = error{};
    return true;
  }
  if (is<none>(xs[2])) {
    dst = make_error(get_as<ec>(xs[1]));
    return true;
  }
  auto& cxt = get<vector>(xs[2]);
  if (contains<std::string>(cxt)) {
    dst = make_error(get_as<ec>(xs[1]), get<std::string>(cxt[0]));
  } else {
    BROKER_ASSERT((contains<endpoint_info, std::string>(cxt)));
    dst = make_error(get_as<ec>(xs[1]), get_as<endpoint_info>(cxt[0]),
                     get<std::string>(cxt[1]));
  }
  return true;
}

ec error_view::code() const noexcept {
  auto result = ec::unspecified;
  std::ignore = convert((*xs_)[1], result);
  return result;
}

const std::string* error_view::message() const noexcept {
  if (is<none>((*xs_)[2]))
    return nullptr;
  auto try_get_str = [](const vector& vec, size_t index) {
    return vec.size() > index ? get_if<std::string>(vec[index]) : nullptr;
  };
  if (auto ctx = get_if<vector>((*xs_)[2]); !ctx) {
    return nullptr;
  } else {
    return try_get_str(*ctx, ctx->size() == 1 ? 0 : 1);
  }
}

std::optional<endpoint_info> error_view::context() const {
  if (is<none>((*xs_)[2])) {
    return std::nullopt;
  }
  if (auto& ctx = get<vector>((*xs_)[2]); ctx.size() == 2) {
    return get_as<endpoint_info>(ctx[0]);
  }
  return std::nullopt;
}

error_view error_view::make(const data& src) {
  return error_view{convertible_to_error(src) ? &get<vector>(src) : nullptr};
}

error error_factory::make_impl(ec code, endpoint_info node, std::string msg) {
  return make_error(code, std::move(node), std::move(msg));
}

} // namespace broker
