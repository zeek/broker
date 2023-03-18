#pragma once

#include "broker/detail/radix_tree.hh"
#include "broker/filter_type.hh"

namespace broker::internal {

/// A filter based on a RADIX tree. The key is the always the string (i.e, the
/// topic) and the value is a "reference count" for the subscription.
using radix_filter = detail::radix_tree<int>;

/// Adds a reference to `x` to the filter.
/// @return `true` if we have added a new node, `false` otherwise.
bool filter_add_ref(radix_filter& tree, const topic& x);

/// Convenience function for calling `filter_add_ref` with each topic in `xs`.
/// @return the number of added nodes.
size_t filter_add_ref(radix_filter& tree, const filter_type& xs);

/// Convenience function for calling `filter_extend` with each topic in `other`
/// that matches `predicate`.
/// @return the number of added nodes.
template <class Predicate>
size_t filter_add_ref(radix_filter& tree, const filter_type& other,
                      Predicate predicate) {
  size_t added = 0;
  for (auto& x : other)
    if (predicate(x) && filter_add_ref(tree, x))
      ++added;
  return added > 0;
}

/// Removes a reference for `x` from the filter.
/// @return `true` if the filter has removed the topic entirely, `false`
/// otherwise.
bool filter_release_ref(radix_filter& tree, const topic& x);

/// Convenience function for calling `filter_release_ref` with each topic in
/// `xs`.
/// @return the number of removed nodes.
bool filter_release_ref(radix_filter& tree, const filter_type& xs);

/// Converts the radix-filter to a regular filter.
filter_type to_filter(const radix_filter& tree);

} // namespace broker::internal
