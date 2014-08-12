#ifndef BROKER_DATA_TYPES_HH
#define BROKER_DATA_TYPES_HH

#include <string>
#include <functional>
#include <unordered_set>
#include <memory>
#include <cstdint>

namespace broker { namespace data {

// TODO: key/value should be Bro types?
using key = std::string;
using value = std::string;

enum class query_result {
	success,
	timeout,
	nonexistent,      // No data store exists that is able to respond to query.
	unknown_failure,  // Abnormal failure due to internal broker library error.
	num_results       // Sentinel for last enum value.
};

using LookupCallback = std::function<void (key k, std::unique_ptr<value> v,
                                           void* cookie, query_result)>;
using HasKeyCallback = std::function<void (key k, bool exists,
                                           void* cookie, query_result)>;
using KeysCallback = std::function<void (std::unordered_set<key> keys,
                                         void* cookie, query_result)>;
using SizeCallback = std::function<void (uint64_t size,
                                         void* cookie, query_result)>;

} // namespace data
} // namespace broker

#endif // BROKER_DATA_TYPES_HH
