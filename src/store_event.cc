#include "broker/store_event.hh"

namespace broker {

namespace {

constexpr const char* type_strings[] = {
  "add",
  "put",
  "erase",
};

} // namespace

store_event::add store_event::add::make(const vector& xs) noexcept {
  return add{xs.size() == 4
               && to<store_event::type>(xs[0]) == store_event::type::add
               && (is<none>(xs[3]) || is<timespan>(xs[3]))
             ? &xs
             : nullptr};
}

store_event::put store_event::put::make(const vector& xs) noexcept {
  return put{xs.size() == 4
               && to<store_event::type>(xs[0]) == store_event::type::put
               && (is<none>(xs[3]) || is<timespan>(xs[3]))
             ? &xs
             : nullptr};
}

store_event::erase store_event::erase::make(const vector& xs) noexcept {
  return erase{xs.size() == 2
                 && to<store_event::type>(xs[0]) == store_event::type::erase
               ? &xs
               : nullptr};
}

const char* to_string(store_event::type code) noexcept {
  return type_strings[static_cast<uint8_t>(code)];
}

bool convert(const std::string& src, store_event::type& dst) noexcept{
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

bool convert(const data& src, store_event::type& dst) noexcept{
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
