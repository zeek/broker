#pragma once

#include "broker/data.hh"
#include "broker/detail/type_traits.hh"

#include <format>
#include <type_traits>

namespace broker::detail {

template <class T>
struct default_formatter {
  using string_formatter = std::formatter<std::string, char>;

  constexpr auto parse(std::format_parse_context& ctx) {
    return string_formatter{}.parse(ctx);
  }

  template <typename FormatContext>
  auto format(const T& value, FormatContext& ctx) const {
    std::string out;
    broker::convert(value, out);
    return string_formatter{}.format(out, ctx);
  }
};

// Backported from C++23.
template <class... Args>
void println(std::format_string<Args...> fmt, Args&&... args) {
  auto str = std::format(fmt, std::forward<Args>(args)...);
  printf("%s\n", str.c_str());
}

// Backported from C++23.
template <class... Args>
void println(std::FILE* stream, std::format_string<Args...> fmt,
             Args&&... args) {
  auto str = std::format(fmt, std::forward<Args>(args)...);
  fprintf(stream, "%s\n", str.c_str());
}

} // namespace broker::detail

#define BROKER_STD_FORMATTER_IMPL(type_name)                                   \
  template <>                                                                  \
  struct formatter<broker::type_name, char>                                    \
    : broker::detail::default_formatter<broker::type_name> {}

namespace std {

BROKER_STD_FORMATTER_IMPL(endpoint_info);
BROKER_STD_FORMATTER_IMPL(entity_id);
BROKER_STD_FORMATTER_IMPL(enum_value);
BROKER_STD_FORMATTER_IMPL(network_info);
BROKER_STD_FORMATTER_IMPL(none);
BROKER_STD_FORMATTER_IMPL(peer_info);
BROKER_STD_FORMATTER_IMPL(put_command);
BROKER_STD_FORMATTER_IMPL(put_unique_command);
BROKER_STD_FORMATTER_IMPL(put_unique_result_command);
BROKER_STD_FORMATTER_IMPL(erase_command);
BROKER_STD_FORMATTER_IMPL(expire_command);
BROKER_STD_FORMATTER_IMPL(add_command);
BROKER_STD_FORMATTER_IMPL(subtract_command);
BROKER_STD_FORMATTER_IMPL(clear_command);
BROKER_STD_FORMATTER_IMPL(attach_writer_command);
BROKER_STD_FORMATTER_IMPL(ack_clone_command);
BROKER_STD_FORMATTER_IMPL(cumulative_ack_command);
BROKER_STD_FORMATTER_IMPL(nack_command);
BROKER_STD_FORMATTER_IMPL(keepalive_command);
BROKER_STD_FORMATTER_IMPL(retransmit_failed_command);
BROKER_STD_FORMATTER_IMPL(address);
BROKER_STD_FORMATTER_IMPL(endpoint_id);
BROKER_STD_FORMATTER_IMPL(enum_value_view);
BROKER_STD_FORMATTER_IMPL(envelope);
BROKER_STD_FORMATTER_IMPL(error);
BROKER_STD_FORMATTER_IMPL(internal_command);
BROKER_STD_FORMATTER_IMPL(port);
BROKER_STD_FORMATTER_IMPL(shutdown_options);
BROKER_STD_FORMATTER_IMPL(status);
BROKER_STD_FORMATTER_IMPL(subnet);
BROKER_STD_FORMATTER_IMPL(topic);
BROKER_STD_FORMATTER_IMPL(variant);
BROKER_STD_FORMATTER_IMPL(variant_data);
BROKER_STD_FORMATTER_IMPL(variant_list);
BROKER_STD_FORMATTER_IMPL(variant_set);
BROKER_STD_FORMATTER_IMPL(variant_table);
BROKER_STD_FORMATTER_IMPL(backend);
BROKER_STD_FORMATTER_IMPL(ec);
BROKER_STD_FORMATTER_IMPL(overflow_policy);
BROKER_STD_FORMATTER_IMPL(p2p_message_type);
BROKER_STD_FORMATTER_IMPL(peer_status);
BROKER_STD_FORMATTER_IMPL(sc);
BROKER_STD_FORMATTER_IMPL(data);
BROKER_STD_FORMATTER_IMPL(command_envelope_ptr);
BROKER_STD_FORMATTER_IMPL(data_envelope_ptr);
BROKER_STD_FORMATTER_IMPL(envelope_ptr);
BROKER_STD_FORMATTER_IMPL(ping_envelope_ptr);
BROKER_STD_FORMATTER_IMPL(pong_envelope_ptr);
BROKER_STD_FORMATTER_IMPL(routing_update_envelope_ptr);
BROKER_STD_FORMATTER_IMPL(filter_type);
BROKER_STD_FORMATTER_IMPL(internal::expiry_formatter);

template <class T>
struct formatter<broker::expected<T>, char> {
  using string_formatter = std::formatter<std::string, char>;

  constexpr auto parse(std::format_parse_context& ctx) {
    return string_formatter{}.parse(ctx);
  }

  template <typename FormatContext>
  auto format(const broker::expected<T>& value, FormatContext& ctx) const {
    if (value) {
      std::string out;
      broker::convert(*value, out);
      return string_formatter{}.format(out, ctx);
    }
    std::string out;
    broker::convert(value.error(), out);
    return string_formatter{}.format(out, ctx);
  }
};

} // namespace std
