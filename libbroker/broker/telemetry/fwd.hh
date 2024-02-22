#pragma once

#include "broker/span.hh"

#include <cstdint>
#include <string_view>
#include <utility>

namespace broker::telemetry {

// -- API types ----------------------------------------------------------------

template <class T>
class counter;

template <class T>
class counter_family;

template <class T>
class gauge;

template <class T>
class gauge_family;

template <class T>
class histogram;

template <class T>
class histogram_family;

// -- convenience aliases ------------------------------------------------------

using label_view = std::pair<std::string_view, std::string_view>;

using const_label_list = span<const label_view>;

using dbl_counter = counter<double>;

using dbl_counter_family = counter_family<double>;

using int_counter = counter<int64_t>;

using int_counter_family = counter_family<int64_t>;

using dbl_gauge = gauge<double>;

using dbl_gauge_family = gauge_family<double>;

using int_gauge = gauge<int64_t>;

using int_gauge_family = gauge_family<int64_t>;

using dbl_histogram = histogram<double>;

using dbl_histogram_family = histogram_family<double>;

using int_histogram = histogram<int64_t>;

using int_histogram_family = histogram_family<int64_t>;

// -- opaque handle types ------------------------------------------------------

class dbl_counter_family_hdl;

class dbl_counter_hdl;

class dbl_gauge_family_hdl;

class dbl_gauge_hdl;

class dbl_histogram_family_hdl;

class dbl_histogram_hdl;

class int_counter_family_hdl;

class int_counter_hdl;

class int_gauge_family_hdl;

class int_gauge_hdl;

class int_histogram_family_hdl;

class int_histogram_hdl;

class metric_family_hdl;

// -- free function interface for counters -------------------------------------

void inc(dbl_counter_hdl*) noexcept;

void inc(dbl_counter_hdl*, double amount) noexcept;

double value(const dbl_counter_hdl*) noexcept;

dbl_counter_hdl* dbl_counter_get_or_add(metric_family_hdl*,
                                        span<const label_view> labels);

int64_t inc(int_counter_hdl*) noexcept;

void inc(int_counter_hdl*, int64_t amount) noexcept;

int64_t value(const int_counter_hdl*) noexcept;

int_counter_hdl* int_counter_get_or_add(metric_family_hdl*,
                                        span<const label_view> labels);

// -- free function interface for gauges ---------------------------------------

void inc(dbl_gauge_hdl* hdl) noexcept;

void inc(dbl_gauge_hdl* hdl, double amount) noexcept;

void dec(dbl_gauge_hdl* hdl, double amount) noexcept;

void dec(dbl_gauge_hdl* hdl) noexcept;

double value(const dbl_gauge_hdl* hdl) noexcept;

dbl_gauge_hdl* dbl_gauge_get_or_add(metric_family_hdl* hdl,
                                    span<const label_view> labels);

int64_t inc(int_gauge_hdl* hdl) noexcept;

void inc(int_gauge_hdl* hdl, int64_t amount) noexcept;

int64_t dec(int_gauge_hdl* hdl) noexcept;

void dec(int_gauge_hdl* hdl, int64_t amount) noexcept;

int64_t value(const int_gauge_hdl* hdl) noexcept;

int_gauge_hdl* int_gauge_get_or_add(metric_family_hdl* hdl,
                                    span<const label_view> labels);

// -- free function interface for histograms families --------------------------

size_t num_buckets(const dbl_histogram_family_hdl*) noexcept;

double upper_bound_at(const dbl_histogram_family_hdl*, size_t index) noexcept;

size_t num_buckets(const int_histogram_family_hdl*) noexcept;

int64_t upper_bound_at(const int_histogram_family_hdl*, size_t index) noexcept;

// -- free function interface for histograms -----------------------------------

void observe(dbl_histogram_hdl*, double value) noexcept;

double sum(const dbl_histogram_hdl*) noexcept;

size_t num_buckets(const dbl_histogram_hdl*) noexcept;

double count_at(const dbl_histogram_hdl*, size_t index) noexcept;

double upper_bound_at(const dbl_histogram_hdl*, size_t index) noexcept;

dbl_histogram_hdl* dbl_histogram_get_or_add(metric_family_hdl*,
                                            span<const label_view> labels);

void observe(int_histogram_hdl*, int64_t value) noexcept;

int64_t sum(const int_histogram_hdl*) noexcept;

size_t num_buckets(const int_histogram_hdl*) noexcept;

int64_t count_at(const int_histogram_hdl*, size_t index) noexcept;

int64_t upper_bound_at(const int_histogram_hdl*, size_t index) noexcept;

int_histogram_hdl* int_histogram_get_or_add(metric_family_hdl*,
                                            span<const label_view> labels);

// -- free function interface for metric families ------------------------------

metric_family_hdl* upcast(dbl_counter_family_hdl*);

const metric_family_hdl* upcast(const dbl_counter_family_hdl*);

metric_family_hdl* upcast(dbl_gauge_family_hdl*);

const metric_family_hdl* upcast(const dbl_gauge_family_hdl*);

metric_family_hdl* upcast(dbl_histogram_family_hdl*);

const metric_family_hdl* upcast(const dbl_histogram_family_hdl*);

metric_family_hdl* upcast(int_counter_family_hdl*);

const metric_family_hdl* upcast(const int_counter_family_hdl*);

metric_family_hdl* upcast(int_gauge_family_hdl*);

const metric_family_hdl* upcast(const int_gauge_family_hdl*);

metric_family_hdl* upcast(int_histogram_family_hdl*);

const metric_family_hdl* upcast(const int_histogram_family_hdl*);

dbl_counter_family_hdl* as_dbl_counter_family(metric_family_hdl*);

const dbl_counter_family_hdl* as_dbl_counter_family(const metric_family_hdl*);

int_counter_family_hdl* as_int_counter_family(metric_family_hdl*);

const int_counter_family_hdl* as_int_counter_family(const metric_family_hdl*);

dbl_gauge_family_hdl* as_dbl_gauge_family(metric_family_hdl*);

const dbl_gauge_family_hdl* as_dbl_gauge_family(const metric_family_hdl*);

int_gauge_family_hdl* as_int_gauge_family(metric_family_hdl*);

const int_gauge_family_hdl* as_int_gauge_family(const metric_family_hdl*);

dbl_histogram_family_hdl* as_dbl_histogram_family(metric_family_hdl*);

const dbl_histogram_family_hdl*
as_dbl_histogram_family(const metric_family_hdl*);

int_histogram_family_hdl* as_int_histogram_family(metric_family_hdl*);

const int_histogram_family_hdl*
as_int_histogram_family(const metric_family_hdl*);

std::string_view prefix(const metric_family_hdl*) noexcept;

std::string_view name(const metric_family_hdl*) noexcept;

span<const std::string> label_names(const metric_family_hdl*) noexcept;

std::string_view helptext(const metric_family_hdl*) noexcept;

std::string_view unit(const metric_family_hdl*) noexcept;

bool is_sum(const metric_family_hdl*) noexcept;

// -- free function interface for the Broker metric registry -------------------

class metric_registry_impl;

void intrusive_ptr_add_ref(const metric_registry_impl* ptr);

void intrusive_ptr_release(const metric_registry_impl* ptr);

inline void Ref(const metric_registry_impl* ptr) {
  intrusive_ptr_add_ref(ptr);
}

inline void Unref(const metric_registry_impl* ptr) {
  intrusive_ptr_release(ptr);
}

int_counter_family_hdl*
int_counter_fam(metric_registry_impl*, std::string_view pre,
                std::string_view name, span<const std::string_view> labels,
                std::string_view helptext, std::string_view unit, bool is_sum);

dbl_counter_family_hdl*
dbl_counter_fam(metric_registry_impl*, std::string_view pre,
                std::string_view name, span<const std::string_view> labels,
                std::string_view helptext, std::string_view unit, bool is_sum);

int_gauge_family_hdl* int_gauge_fam(metric_registry_impl*, std::string_view pre,
                                    std::string_view name,
                                    span<const std::string_view> labels,
                                    std::string_view helptext,
                                    std::string_view unit, bool is_sum);

dbl_gauge_family_hdl* dbl_gauge_fam(metric_registry_impl*, std::string_view pre,
                                    std::string_view name,
                                    span<const std::string_view> labels,
                                    std::string_view helptext,
                                    std::string_view unit, bool is_sum);

int_histogram_family_hdl*
int_histogram_fam(metric_registry_impl*, std::string_view pre,
                  std::string_view name, span<const std::string_view> labels,
                  span<const int64_t> ubounds, std::string_view helptext,
                  std::string_view unit, bool is_sum);

dbl_histogram_family_hdl*
dbl_histogram_fam(metric_registry_impl*, std::string_view pre,
                  std::string_view name, span<const std::string_view> labels,
                  span<const double> ubounds, std::string_view helptext,
                  std::string_view unit, bool is_sum);

} // namespace broker::telemetry

// -- opaque types by value type lookup ----------------------------------------

namespace broker::detail {

template <class T>
struct counter_hdl_oracle;

template <>
struct counter_hdl_oracle<double> {
  using fam_type = telemetry::dbl_counter_family_hdl;
  using hdl_type = telemetry::dbl_counter_hdl;
};

template <>
struct counter_hdl_oracle<int64_t> {
  using fam_type = telemetry::int_counter_family_hdl;
  using hdl_type = telemetry::int_counter_hdl;
};

template <class T>
using counter_fam_t = typename counter_hdl_oracle<T>::fam_type;

template <class T>
using counter_hdl_t = typename counter_hdl_oracle<T>::hdl_type;

template <class T>
struct gauge_hdl_oracle;

template <>
struct gauge_hdl_oracle<double> {
  using fam_type = telemetry::dbl_gauge_family_hdl;
  using hdl_type = telemetry::dbl_gauge_hdl;
};

template <>
struct gauge_hdl_oracle<int64_t> {
  using fam_type = telemetry::int_gauge_family_hdl;
  using hdl_type = telemetry::int_gauge_hdl;
};

template <class T>
using gauge_fam_t = typename gauge_hdl_oracle<T>::fam_type;

template <class T>
using gauge_hdl_t = typename gauge_hdl_oracle<T>::hdl_type;

template <class T>
struct histogram_hdl_oracle;

template <>
struct histogram_hdl_oracle<double> {
  using fam_type = telemetry::dbl_histogram_family_hdl;
  using hdl_type = telemetry::dbl_histogram_hdl;
};

template <>
struct histogram_hdl_oracle<int64_t> {
  using fam_type = telemetry::int_histogram_family_hdl;
  using hdl_type = telemetry::int_histogram_hdl;
};

template <class T>
using histogram_fam_t = typename histogram_hdl_oracle<T>::fam_type;

template <class T>
using histogram_hdl_t = typename histogram_hdl_oracle<T>::hdl_type;

} // namespace broker::detail
