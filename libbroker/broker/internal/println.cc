#include "broker/internal/println.hh"

#include <atomic>

namespace {

std::mutex println_mtx_instance;

std::atomic<bool> verbose_output;

} // namespace

namespace broker::internal {

std::mutex& println_mtx() {
  return println_mtx_instance;
}

} // namespace broker::internal

namespace broker::internal::verbose {

bool enabled() {
  return verbose_output.load();
}

void enabled(bool value) {
  verbose_output = value;
}

} // namespace broker::internal::verbose
