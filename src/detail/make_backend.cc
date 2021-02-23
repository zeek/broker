#include "broker/config.hh"

#include "broker/detail/die.hh"
#include "broker/detail/make_backend.hh"
#include "broker/detail/memory_backend.hh"
#include "broker/detail/sqlite_backend.hh"

namespace broker {
namespace detail {

std::unique_ptr<detail::abstract_backend> make_backend(backend type,
                                                       backend_options opts) {
  switch (type) {
    case backend::memory:
      return std::make_unique<memory_backend>(std::move(opts));
    case backend::sqlite:
      return std::make_unique<sqlite_backend>(std::move(opts));
  }

  die("invalid backend type");
}

} // namespace detail
} // namespace broker
