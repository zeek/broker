#ifndef BROKER_DATA_TYPES_HH
#define BROKER_DATA_TYPES_HH

#include <string>
#include <functional>
#include <unordered_set>
#include <memory>
#include <cstdint>

namespace broker { namespace data {

// TODO: Key and Val should be Bro types?
using Key = std::string;
using Val = std::string;

using LookupCallback = std::function<void (Key key,
                                           std::unique_ptr<Val> val,
                                           void* cookie)>;
using HasKeyCallback = std::function<void (Key key,
                                           bool exists,
                                           void* cookie)>;
using KeysCallback = std::function<void (std::unordered_set<Key> keys,
                                         void* cookie)>;
using SizeCallback = std::function< void (uint64_t size, void* cookie)>;

} // namespace data
} // namespace broker

#endif // BROKER_DATA_TYPES_HH
