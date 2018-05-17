#include "broker/config.hh"

#include "broker/detail/die.hh"
#include "broker/detail/make_backend.hh"
#include "broker/detail/make_unique.hh"
#include "broker/detail/memory_backend.hh"
#include "broker/detail/rocksdb_backend.hh"
#include "broker/detail/sqlite_backend.hh"

namespace broker {
namespace detail {

std::unique_ptr<detail::abstract_backend> make_backend(backend type,
                                                       backend_options opts) {
  switch (type) {
    case memory:
      return std::make_unique<memory_backend>(std::move(opts));
    case sqlite:
      return std::make_unique<sqlite_backend>(std::move(opts));
    case rocksdb:
#ifdef BROKER_HAVE_ROCKSDB
      return std::make_unique<rocksdb_backend>(std::move(opts));
#else
      die("not compiled with RocksDB support");
#endif
  }

  die("invalid backend type");
}

} // namespace detail
} // namespace broker
