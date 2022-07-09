#pragma once

#include "broker/config.hh"
#include "broker/fwd.hh"
#include "broker/telemetry/fwd.hh"

#include <atomic>
#include <cstddef>

namespace broker::telemetry {

// Collector for the broker::telemetry layer. We string through the labels
// of individual metric instances as there aren't direct accessors for those
// from a hdl.
class metrics_collector {
public:
  virtual void operator()(const metric_family_hdl* family,
                          const dbl_counter_hdl* counter,
                          const_label_list labels) = 0;
  virtual void operator()(const metric_family_hdl* family,
                          const int_counter_hdl* counter,
                          const_label_list labels) = 0;
  virtual void operator()(const metric_family_hdl* family,
                          const dbl_gauge_hdl* gauge,
                          const_label_list labels) = 0;
  virtual void operator()(const metric_family_hdl* family,
                          const int_gauge_hdl* gauge,
                          const_label_list labels) = 0;
  virtual void operator()(const metric_family_hdl* family,
                          const dbl_histogram_hdl* histogram,
                          const_label_list labels) = 0;
  virtual void operator()(const metric_family_hdl* family,
                          const int_histogram_hdl* histogram,
                          const_label_list labels) = 0;
};

class metric_registry_impl {
public:
  using ref_count_type = std::atomic<size_t>;

  metric_registry_impl();

  metric_registry_impl(const metric_registry_impl&) = delete;

  metric_registry_impl& operator-(const metric_registry_impl&) = delete;

  virtual ~metric_registry_impl();

  virtual int_counter_family_hdl*
  int_counter_fam(std::string_view pre, std::string_view name,
                  span<const std::string_view> labels,
                  std::string_view helptext, std::string_view unit,
                  bool is_sum) = 0;

  virtual dbl_counter_family_hdl*
  dbl_counter_fam(std::string_view pre, std::string_view name,
                  span<const std::string_view> labels,
                  std::string_view helptext, std::string_view unit,
                  bool is_sum) = 0;

  virtual int_gauge_family_hdl*
  int_gauge_fam(std::string_view pre, std::string_view name,
                span<const std::string_view> labels, std::string_view helptext,
                std::string_view unit, bool is_sum) = 0;

  virtual dbl_gauge_family_hdl*
  dbl_gauge_fam(std::string_view pre, std::string_view name,
                span<const std::string_view> labels, std::string_view helptext,
                std::string_view unit, bool is_sum) = 0;

  virtual int_histogram_family_hdl*
  int_histogram_fam(std::string_view pre, std::string_view name,
                    span<const std::string_view> labels,
                    span<const int64_t> ubounds, std::string_view helptext,
                    std::string_view unit, bool is_sum) = 0;

  virtual dbl_histogram_family_hdl*
  dbl_histogram_fam(std::string_view pre, std::string_view name,
                    span<const std::string_view> labels,
                    span<const double> ubounds, std::string_view helptext,
                    std::string_view unit, bool is_sum) = 0;

  // Collect metrics.
  virtual void collect(metrics_collector& collector) = 0;

  virtual bool merge(endpoint& where) = 0;

  void ref() const noexcept {
    ++rc_;
  }

  void deref() const noexcept {
    if (--rc_ == 0)
      delete this;
  }

  bool unique() const noexcept {
    return rc_.load() == 1;
  }

private:
  alignas(BROKER_CONSTRUCTIVE_INTERFERENCE_SIZE) mutable ref_count_type rc_;
};

} // namespace broker::telemetry
