#ifndef BROKER_DATA_HH
#define BROKER_DATA_HH

#include <broker/util/variant.hh>
#include <broker/util/hash.hh>
#include <cstdint>
#include <string>
#include <set>
#include <map>
#include <vector>

namespace broker {

class data;
using set = std::set<data>;
using vector = std::vector<data>;
using table = std::map<data, data>;

class data : util::totally_ordered<data> {
public:

	enum class tag : uint8_t {
		// Primitive types
		boolean,  // bool
		integer,  // int64_t
		count,    // uint64_t
		real,     // double
		string,   // std::string
		// TODO: time
		// TODO: interval
		// TODO: enumeration
		// TODO: port
		// TODO: address
		// TODO: subnet

		// Compound types
		set,
		table,
		vector,
		// TODO: record
	};

	using value_type = util::variant<
	    tag,
	    bool,
	    uint64_t,
	    int64_t,
	    double,
	    std::string,
	    set,
	    table,
	    vector
	>;

	template <typename T>
	using from = util::conditional_t<
        std::is_floating_point<T>::value,
        double,
        util::conditional_t<
          std::is_same<T, bool>::value,
          bool,
          util::conditional_t<
            std::is_unsigned<T>::value,
            uint64_t,
            util::conditional_t<
              std::is_signed<T>::value,
              int64_t,
              util::conditional_t<
                std::is_convertible<T, std::string>::value,
                std::string,
                util::conditional_t<
	                 std::is_same<T, set>::value ||
	                 std::is_same<T, table>::value ||
	                 std::is_same<T, vector>::value,
	                 T, std::false_type>
              >
            >
          >
        >
      >;

	template <typename T>
	using type = from<typename std::decay<T>::type>;

	/**
	 * Default construct data.
	 */
	data()
		{}

	/**
	  * Constructs data.
	  * @param x The instance to construct data from.
	  */
	template <typename T,
	          typename = util::disable_if_t<
	              util::is_same_or_derived<data, T>::value ||
	              std::is_same<type<T>, std::false_type>::value>
	         >
	data(T&& x)
	    : value(type<T>(std::forward<T>(x)))
		{}

	value_type value;
};

inline data::value_type& expose(data& d)
	{ return d.value; }

inline const data::value_type& expose(const data& d)
	{ return d.value; }

inline bool operator==(const data& lhs, const data& rhs)
	{ return lhs.value == rhs.value; }

inline bool operator<(const data& lhs, const data& rhs)
	{ return lhs.value < rhs.value; }

} // namespace broker

namespace std {
template <> struct std::hash<broker::data> {
	inline size_t operator()(const broker::data& d) const
		{ return std::hash<broker::data::value_type>{}(d.value); }
};
template <> struct std::hash<broker::set> :
                   broker::util::container_hasher<broker::set> { };
template <> struct std::hash<broker::vector> :
                   broker::util::container_hasher<broker::vector> { };
template <> struct std::hash<broker::table::value_type> {
	inline size_t operator()(const broker::table::value_type& d) const
		{
		size_t rval = 0;
		broker::util::hash_combine<broker::data>(rval, d.first);
		broker::util::hash_combine<broker::data>(rval, d.second);
		return rval;
		}
};
template <> struct std::hash<broker::table> :
                   broker::util::container_hasher<broker::table> { };
}

#endif // BROKER_DATA_HH
