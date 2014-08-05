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

enum class CallbackResult {
	success = 0,
	timeout,
	nonexistent,      // No data store exists that is able to respond to query.
	unknown_failure,  // Abnormal failure due to internal broker library error.
	num_results       // Sentinel for last enum value.
};

using LookupCallback = std::function<void (Key key, std::unique_ptr<Val> val,
                                           void* cookie, CallbackResult)>;
using HasKeyCallback = std::function<void (Key key, bool exists,
                                           void* cookie, CallbackResult)>;
using KeysCallback = std::function<void (std::unordered_set<Key> keys,
                                         void* cookie, CallbackResult)>;
using SizeCallback = std::function<void (uint64_t size,
                                         void* cookie, CallbackResult)>;

} // namespace data
} // namespace broker

#endif // BROKER_DATA_TYPES_HH
