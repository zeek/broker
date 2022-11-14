#pragma once

#include "broker/span.hh"
#include "broker/telemetry/fwd.hh"

#include <caf/telemetry/label_view.hpp>

#include <string_view>

namespace broker::internal {

template <class F>
auto with_native_labels(span<const telemetry::label_view> xs,
                        F&& continuation) {
  namespace ct = caf::telemetry;
  if (xs.size() <= 10) {
    ct::label_view buf[10] = {
      {{}, {}}, {{}, {}}, {{}, {}}, {{}, {}}, {{}, {}},
      {{}, {}}, {{}, {}}, {{}, {}}, {{}, {}}, {{}, {}},
    };
    for (size_t index = 0; index < xs.size(); ++index)
      buf[index] = ct::label_view{xs[index].first, xs[index].second};
    return continuation(span{buf, xs.size()});
  } else {
    std::vector<ct::label_view> buf;
    for (const auto& x : xs)
      buf.emplace_back(x.first, x.second);
    return continuation(span{buf});
  }
}

template <class F>
auto with_native_labels(span<const std::string_view> xs, F&& continuation) {
  if (xs.size() <= 10) {
    caf::string_view buf[10];
    for (size_t index = 0; index < xs.size(); ++index)
      buf[index] = xs[index];
    return continuation(span{buf, xs.size()});
  } else {
    std::vector<caf::string_view> buf;
    for (const auto& x : xs)
      buf.emplace_back(x);
    return continuation(span{buf});
  }
}

} // namespace broker::internal
