#include "broker/store_event.hh"

#include <caf/deep_to_string.hpp>

namespace broker {

namespace {

constexpr const char* type_strings[] = {
  "insert",
  "update",
  "erase",
  "expire",
};

bool is_entity_id(const vector& xs, size_t endpoint_index,
                  size_t object_index) {
  return (is<none>(xs[endpoint_index]) && is<none>(xs[object_index]))
         || (can_convert_to<endpoint_id>(xs[endpoint_index])
             && is<uint64_t>(xs[object_index]));
}

template <class T>
std::string opt_to_string(const std::optional<T>& x) {
  if (x)
    return caf::deep_to_string(*x);
  else
    return "null";
}

} // namespace

store_event::insert store_event::insert::make(const vector& xs) noexcept {
  return insert{
    xs.size() == 7 && to<store_event::type>(xs[0]) == store_event::type::insert
        && is<std::string>(xs[1]) && (is<none>(xs[4]) || is<timespan>(xs[4]))
        && is_entity_id(xs, 5, 6)
      ? &xs
      : nullptr};
}

store_event::update store_event::update::make(const vector& xs) noexcept {
  return update{
    xs.size() == 8 && to<store_event::type>(xs[0]) == store_event::type::update
        && is<std::string>(xs[1]) && (is<none>(xs[5]) || is<timespan>(xs[5]))
        && is_entity_id(xs, 6, 7)
      ? &xs
      : nullptr};
}

store_event::erase store_event::erase::make(const vector& xs) noexcept {
  return erase{xs.size() == 5
                   && to<store_event::type>(xs[0]) == store_event::type::erase
                   && is<std::string>(xs[1]) && is_entity_id(xs, 3, 4)
                 ? &xs
                 : nullptr};
}

store_event::expire store_event::expire::make(const vector& xs) noexcept {
  return expire{xs.size() == 5
                    && to<store_event::type>(xs[0]) == store_event::type::expire
                    && is<std::string>(xs[1]) && is_entity_id(xs, 3, 4)
                  ? &xs
                  : nullptr};
}

const char* to_string(store_event::type code) noexcept {
  return type_strings[static_cast<uint8_t>(code)];
}

namespace {

std::string expiry_to_string(const std::optional<timespan>& x) {
  if (x)
    return "*" + caf::deep_to_string(*x);
  else
    return "null";
}

} // namespace

std::string to_string(const store_event::insert& x) {
  std::string result = "insert(";
  result += x.store_id();
  result += ", ";
  result += to_string(x.key());
  result += ", ";
  result += to_string(x.value());
  result += ", ";
  result += expiry_to_string(x.expiry());
  result += ", ";
  result += caf::deep_to_string(x.publisher());
  result += ')';
  return result;
}

std::string to_string(const store_event::update& x) {
  std::string result = "update(";
  result += x.store_id();
  result += ", ";
  result += to_string(x.key());
  result += ", ";
  result += to_string(x.old_value());
  result += ", ";
  result += to_string(x.new_value());
  result += ", ";
  result += expiry_to_string(x.expiry());
  result += ", ";
  result += caf::deep_to_string(x.publisher());
  result += ')';
  return result;
}

std::string to_string(const store_event::erase& x) {
  std::string result = "erase(";
  result += x.store_id();
  result += ", ";
  result += to_string(x.key());
  result += ", ";
  result += to_string(x.publisher());
  result += ')';
  return result;
}

bool convert(const std::string& src, store_event::type& dst) noexcept {
  auto begin = std::begin(type_strings);
  auto end = std::end(type_strings);
  auto i = std::find(begin, end, src);
  if (i != end) {
    auto code = static_cast<uint8_t>(std::distance(begin, i));
    dst = static_cast<store_event::type>(code);
    return true;
  }
  return false;
}

bool convert(const data& src, store_event::type& dst) noexcept {
  if (auto str = get_if<std::string>(src))
    return convert(*str, dst);
  return false;
}

bool convertible_to_store_event_type(const data& src) noexcept {
  if (auto str = get_if<std::string>(src)) {
    auto predicate = [&](const char* x) { return *str == x; };
    return std::any_of(std::begin(type_strings), std::end(type_strings),
                       predicate);
  }
  return false;
}

} // namespace broker
