#include "broker/internal/radix_filter.hh"

namespace broker::internal {

bool filter_add_ref(radix_filter& tree, const topic& x) {
  auto [i, is_new] = tree.insert({x.string(), 1});
  if (!is_new)
    ++i->second;
  return is_new;
}

size_t filter_add_ref(radix_filter& tree, const filter_type& xs) {
  size_t added = 0;
  for (auto& x : xs)
    if (filter_add_ref(tree, x))
      ++added;
  return added;
}

bool filter_release_ref(radix_filter& tree, const topic& x) {
  auto i = tree.find(x.string());
  if (i == tree.end() || --i->second > 0)
    return false;
  tree.erase(x.string());
  return true;
}

filter_type to_filter(const radix_filter& tree) {
  filter_type result;
  result.reserve(tree.size());
  for (auto& kvp : tree)
    result.emplace_back(kvp.first);
  std::sort(result.begin(), result.end());
  return result;
}

bool filter_release_ref(radix_filter& tree, const filter_type& xs) {
  size_t removed = 0;
  for (auto& x : xs)
    if (filter_release_ref(tree, x))
      ++removed;
  return removed;
}

} // namespace broker::internal
