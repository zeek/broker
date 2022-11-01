#include "broker/config.hh"

#include "broker/detail/die.hh"
#include "broker/detail/make_backend.hh"
#include "broker/detail/memory_backend.hh"
#include "broker/detail/sqlite_backend.hh"

namespace broker::detail {

std::unique_ptr<detail::abstract_backend> make_backend(backend type,
                                                       backend_options opts) {
  switch (type) {
    case backend::memory:
      return std::make_unique<memory_backend>(std::move(opts));
    case backend::sqlite: {
      auto rval = std::make_unique<sqlite_backend>(std::move(opts));
      if (rval->init_failed())
        return nullptr;
      return rval;
    }
  }

  die("invalid backend type");
}

} // namespace broker::detail
