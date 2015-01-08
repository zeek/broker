#ifndef BROKER_SUBNET_HH
#define BROKER_SUBNET_HH

#include <broker/address.hh>

namespace broker {

/**
 * Stores an IPv4 or IPv6 subnet (an address prefix).
 */
class subnet : util::totally_ordered<subnet> {
friend class subnet_type_info;

public:

	/**
	 * Default construct empty subnet ::/0.
	 */
	subnet();

	/**
	 * Construct subnet from an address and length.
	 */
	subnet(address addr, uint8_t length);

	/**
	 * @return whether an address is contained within this subnet.
	 */
	bool contains(const address& addr) const;

	/**
	 * @return the network address of the subnet.
	 */
	const address& network() const;

	/**
	 * @return the prefix length of the subnet.
	 */
	uint8_t length() const;

	/**
	 * @return a string representation of the subnet argument.
	 */
	friend std::string to_string(const subnet& s);

	friend std::ostream& operator<<(std::ostream& out, const subnet& s);

	friend bool operator==(const subnet& lhs, const subnet& rhs);

	friend bool operator<(const subnet& lhs, const subnet& rhs);

private:

	bool init();

	address net;
	uint8_t len;
};

std::string to_string(const subnet& s);
std::ostream& operator<<(std::ostream& out, const subnet& s);
bool operator==(const subnet& lhs, const subnet& rhs);
bool operator<(const subnet& lhs, const subnet& rhs);

} // namespace broker

namespace std {
template <> struct hash<broker::subnet> {
	size_t operator()(const broker::subnet&) const;
};
} // namespace std;

#endif // BROKER_SUBNET_HH
