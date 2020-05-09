#pragma once

#include <cstdint>

namespace broker {

/// Describes the supported data store backend.
enum class backend : uint8_t {
  memory,  ///< An in-memory backend based on a simple hash table.
  sqlite,  ///< A SQLite3 backend.
  rocksdb, ///< A RocksDB backend.
};

} // namespace broker
