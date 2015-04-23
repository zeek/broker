#ifndef BROKER_ADDRESS_HH
#define BROKER_ADDRESS_HH

#include <broker/util/operators.hh>
#include <broker/util/optional.hh>
#include <array>
#include <string>
#include <ostream>
#include <cstdint>

namespace broker {

/**
 * Stores an IPv4 or IPv6 address.
 */
class address : util::totally_ordered<address> {
friend class address_type_info;

public:

	/**
	 * Distinguishes between address types.
	 */
	enum class family : uint8_t {
		ipv4,
		ipv6,
	};

	/**
	 * Distinguishes between address byte ordering.
	 */
	enum class byte_order : uint8_t {
		host,
		network,
	};

	/**
	 * @return an address, if one could be made from the string argument.
	 */
	static util::optional<address> from_string(const std::string& s);

	/**
	 * Default construct an invalid address.
	 */
	address();

	/**
	 * Construct an address from raw bytes.
	 * @param bytes A pointer to the raw representation.  This must point
	 * to 4 bytes if \a fam is family::ipv4 and 16 bytes for family::ipv6.
	 * @param fam The type of address.
	 * @param order The byte order in which \a bytes is stored.
	 */
	address(const uint32_t* bytes, family fam, byte_order order);

	/**
	 * Mask out lower bits of the address.
	 * @param top_bits_to_keep The number of bits to *not* mask out, counting
	 * from the highest order bit.  The value is always interpreted relative to
	 * the IPv6 bit width, even if the address is IPv4.  That means to compute
	 * 192.168.1.2/16, pass in 112 (i.e. 96 + 16).  The value must range from
	 * 0 to 128.
	 * @return true on success.
	 */
	bool mask(uint8_t top_bits_to_keep);

	/**
	 * @return true if the address is IPv4.
	 */
	bool is_v4() const;

	/**
	 * @return true if the address is IPv6.
	 */
	bool is_v6() const;

	/**
	 * @return the raw bytes of the address in network order.  For IPv4
	 * addresses, this uses the IPv4-mapped IPv6 address representation.
	 */
	const std::array<uint8_t, 16>& bytes() const;

	/**
	 * @return a string representation of the address argument.
	 */
	friend std::string to_string(const address& a);

	friend std::ostream& operator<<(std::ostream& out, const address& a);

	friend bool operator==(const address& lhs, const address& rhs);

	friend bool operator<(const address& lhs, const address& rhs);

	static const std::array<uint8_t, 12> v4_mapped_prefix;

private:

	std::array<uint8_t, 16> addr; // Always in network order.
};

std::string to_string(const address& a);
std::ostream& operator<<(std::ostream& out, const address& a);
bool operator==(const address& lhs, const address& rhs);
bool operator<(const address& lhs, const address& rhs);

} // namespace broker

namespace std {
template <> struct hash<broker::address> {
	size_t operator()(const broker::address&) const;
};
} // namespace std;

#endif // BROKER_ADDRESS_HH
