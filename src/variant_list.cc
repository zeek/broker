#include "broker/variant_list.hh"

#include "broker/format/txt.hh"

#include <iterator>

namespace broker {

data variant_list::to_data() const {
  vector items;
  for (auto&& x : *this)
    items.emplace_back(x.to_data());
  return data{std::move(items)};
}

bool operator==(const variant_list& lhs, const variant_list& rhs) noexcept {
  if (lhs.size() != rhs.size())
    return false;
  auto i = lhs.begin();
  auto j = rhs.begin();
  for (; i != lhs.end(); ++i, ++j)
    if (*i != *j)
      return false;
  return true;
}

bool operator==(const variant_list& lhs, const vector& rhs) noexcept {
  if (lhs.size() != rhs.size())
    return false;
  auto i = lhs.begin();
  auto j = rhs.begin();
  for (; i != lhs.end(); ++i, ++j)
    if (*i != *j)
      return false;
  return true;
}

void convert(const variant_list& value, std::string& out) {
  format::txt::v1::encode(value.raw(), std::back_inserter(out));
}

void convert(const variant_list& what, vector& out) {
  out.clear();
  if (what.empty())
    return;
  out.reserve(what.size());
  for (const auto& x : what)
    out.emplace_back(x.to_data());
}

std::ostream& operator<<(std::ostream& out, const variant_list& what) {
  format::txt::v1::encode(what.raw(), std::ostream_iterator<char>(out));
  return out;
}

} // namespace broker
