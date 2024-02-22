#include <algorithm>

#include "broker/telemetry/metric_registry.hh"

#include "broker/internal/endpoint_access.hh"
#include "broker/internal/with_native_labels.hh"

#include <caf/actor_system.hpp>
#include <caf/telemetry/metric_family.hpp>
#include <caf/telemetry/metric_family_impl.hpp>
#include <caf/telemetry/metric_registry.hpp>

namespace ct = caf::telemetry;

namespace broker::telemetry {

// -- free function interface for the Broker metric registry -------------------

void intrusive_ptr_add_ref(const metric_registry_impl* ptr) {
  ptr->ref();
}

void intrusive_ptr_release(const metric_registry_impl* ptr) {
  ptr->deref();
}

int_counter_family_hdl*
int_counter_fam(metric_registry_impl* impl, std::string_view pre,
                std::string_view name, span<const std::string_view> labels,
                std::string_view helptext, std::string_view unit, bool is_sum) {
  return impl->int_counter_fam(pre, name, labels, helptext, unit, is_sum);
}

dbl_counter_family_hdl*
dbl_counter_fam(metric_registry_impl* impl, std::string_view pre,
                std::string_view name, span<const std::string_view> labels,
                std::string_view helptext, std::string_view unit, bool is_sum) {
  return impl->dbl_counter_fam(pre, name, labels, helptext, unit, is_sum);
}

int_gauge_family_hdl* int_gauge_fam(metric_registry_impl* impl,
                                    std::string_view pre, std::string_view name,
                                    span<const std::string_view> labels,
                                    std::string_view helptext,
                                    std::string_view unit, bool is_sum) {
  return impl->int_gauge_fam(pre, name, labels, helptext, unit, is_sum);
}

dbl_gauge_family_hdl* dbl_gauge_fam(metric_registry_impl* impl,
                                    std::string_view pre, std::string_view name,
                                    span<const std::string_view> labels,
                                    std::string_view helptext,
                                    std::string_view unit, bool is_sum) {
  return impl->dbl_gauge_fam(pre, name, labels, helptext, unit, is_sum);
}

int_histogram_family_hdl*
int_histogram_fam(metric_registry_impl* impl, std::string_view pre,
                  std::string_view name, span<const std::string_view> labels,
                  span<const int64_t> ubounds, std::string_view helptext,
                  std::string_view unit, bool is_sum) {
  return impl->int_histogram_fam(pre, name, labels, ubounds, helptext, unit,
                                 is_sum);
}

dbl_histogram_family_hdl*
dbl_histogram_fam(metric_registry_impl* impl, std::string_view pre,
                  std::string_view name, span<const std::string_view> labels,
                  span<const double> ubounds, std::string_view helptext,
                  std::string_view unit, bool is_sum) {
  return impl->dbl_histogram_fam(pre, name, labels, ubounds, helptext, unit,
                                 is_sum);
}

// -- member functions of metric_registry --------------------------------------

metric_registry::metric_registry(metric_registry_impl* impl,
                                 bool add_ref) noexcept
  : impl_(impl) {
  if (impl_ && add_ref)
    impl_->ref();
}

metric_registry::metric_registry(metric_registry&& other) noexcept
  : impl_(other.impl_) {
  other.impl_ = nullptr;
}

metric_registry::metric_registry(const metric_registry& other) noexcept
  : metric_registry(other.impl_, true) {}

metric_registry& metric_registry::operator=(metric_registry&& other) noexcept {
  std::swap(impl_, other.impl_);
  return *this;
}

metric_registry&
metric_registry::operator=(const metric_registry& other) noexcept {
  metric_registry tmp{other};
  std::swap(impl_, tmp.impl_);
  return *this;
}

metric_registry::~metric_registry() {
  if (impl_)
    impl_->deref();
}

namespace {

// -- free functions used by impl_base::collect()

void extract_labels(const ct::metric* instance, std::vector<label_view>& vec) {
  auto get_value = [](const auto& label) -> label_view {
    auto name = label.name();
    auto val = label.value();
    return {std::string_view{name.data(), name.size()},
            std::string_view{val.data(), val.size()}};
  };
  const auto& labels = instance->labels();
  std::transform(labels.begin(), labels.end(), std::back_inserter(vec),
                 get_value);
}

const auto* opaque(const ct::metric_family* family) {
  return reinterpret_cast<const metric_family_hdl*>(family);
}

#define OPAQUE_OBJ(type)                                                       \
  const auto* opaque(const ct::type* obj) {                                    \
    return reinterpret_cast<const type##_hdl*>(obj);                           \
  }

OPAQUE_OBJ(dbl_counter)
OPAQUE_OBJ(int_counter)
OPAQUE_OBJ(dbl_gauge)
OPAQUE_OBJ(int_gauge)
OPAQUE_OBJ(dbl_histogram)
OPAQUE_OBJ(int_histogram)

// -- impl_base

class impl_base : public metric_registry_impl {
public:
  explicit impl_base(ct::metric_registry* reg) : reg_(reg) {
    // nop
  }

  int_counter_family_hdl* int_counter_fam(std::string_view pre,
                                          std::string_view name,
                                          span<const std::string_view> labels,
                                          std::string_view helptext,
                                          std::string_view unit,
                                          bool is_sum) override {
    return internal::with_native_labels(labels, [=](auto xs) {
      auto ptr = reg_->counter_family(pre, name, xs, helptext, unit, is_sum);
      return reinterpret_cast<int_counter_family_hdl*>(ptr);
    });
  }

  dbl_counter_family_hdl* dbl_counter_fam(std::string_view pre,
                                          std::string_view name,
                                          span<const std::string_view> labels,
                                          std::string_view helptext,
                                          std::string_view unit,
                                          bool is_sum) override {
    return internal::with_native_labels(labels, [=](auto xs) {
      auto ptr = reg_->counter_family<double>(pre, name, xs, helptext, unit,
                                              is_sum);
      return reinterpret_cast<dbl_counter_family_hdl*>(ptr);
    });
  }

  int_gauge_family_hdl*
  int_gauge_fam(std::string_view pre, std::string_view name,
                span<const std::string_view> labels, std::string_view helptext,
                std::string_view unit, bool is_sum) override {
    return internal::with_native_labels(labels, [=](auto xs) {
      auto ptr = reg_->gauge_family(pre, name, xs, helptext, unit, is_sum);
      return reinterpret_cast<int_gauge_family_hdl*>(ptr);
    });
  }

  dbl_gauge_family_hdl*
  dbl_gauge_fam(std::string_view pre, std::string_view name,
                span<const std::string_view> labels, std::string_view helptext,
                std::string_view unit, bool is_sum) override {
    return internal::with_native_labels(labels, [=](auto xs) {
      auto ptr = reg_->gauge_family<double>(pre, name, xs, helptext, unit,
                                            is_sum);
      return reinterpret_cast<dbl_gauge_family_hdl*>(ptr);
    });
  }

  int_histogram_family_hdl*
  int_histogram_fam(std::string_view pre, std::string_view name,
                    span<const std::string_view> labels,
                    span<const int64_t> ubounds, std::string_view helptext,
                    std::string_view unit, bool is_sum) override {
    return internal::with_native_labels(labels, [=](auto xs) {
      auto bounds = caf::span<const int64_t>{ubounds.data(), ubounds.size()};
      auto ptr = reg_->histogram_family(pre, name, xs, bounds, helptext, unit,
                                        is_sum);
      return reinterpret_cast<int_histogram_family_hdl*>(ptr);
    });
  }

  dbl_histogram_family_hdl*
  dbl_histogram_fam(std::string_view pre, std::string_view name,
                    span<const std::string_view> labels,
                    span<const double> ubounds, std::string_view helptext,
                    std::string_view unit, bool is_sum) override {
    return internal::with_native_labels(labels, [=](auto xs) {
      auto bounds = caf::span<const double>{ubounds.data(), ubounds.size()};
      auto ptr = reg_->histogram_family<double>(pre, name, xs, bounds, helptext,
                                                unit, is_sum);
      return reinterpret_cast<dbl_histogram_family_hdl*>(ptr);
    });
  }

  void collect(metrics_collector& collector) override {
    std::vector<label_view> labels_vec; // Reuse for label extraction
    auto fn = [&collector, &labels_vec](const ct::metric_family* family,
                                        const ct::metric* instance,
                                        const auto* obj) {
      labels_vec.clear();
      extract_labels(instance, labels_vec);
      collector(opaque(family), opaque(obj), labels_vec);
    };

    reg_->collect(fn);
  }

protected:
  ct::metric_registry* reg_;
};

// Metrics access before initializing the actor system.
class pre_init_impl : public impl_base {
public:
  using super = impl_base;

  pre_init_impl() : super(&tmp_) {
    // nop
  }

  bool merge(endpoint& where) override {
    auto& sys = internal::endpoint_access{&where}.sys();
    sys.metrics().merge(tmp_);
    return true;
  }

private:
  ct::metric_registry tmp_;
};

// Metrics access after initializing the actor system.
class post_init_impl : public impl_base {
public:
  using super = impl_base;

  post_init_impl(internal::endpoint_context_ptr ctx)
    : super(std::addressof(ctx->sys.metrics())), ctx_(std::move(ctx)) {
    // nop
  }

  bool merge(endpoint&) override {
    return false;
  }

  internal::endpoint_context_ptr ctx_;
};

} // namespace

metric_registry metric_registry::pre_init_instance() {
  return metric_registry{new pre_init_impl, false};
}

metric_registry metric_registry::merge(metric_registry what,
                                       broker::endpoint& where) {
  if (what.impl_->merge(where)) {
    return from(where);
  }
  return what;
}

metric_registry metric_registry::from(broker::endpoint& where) {
  auto ctx = internal::endpoint_access{&where}.ctx();
  return metric_registry{new post_init_impl(std::move(ctx)), false};
}

} // namespace broker::telemetry
