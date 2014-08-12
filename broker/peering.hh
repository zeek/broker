#ifndef BROKER_PEERING_HH
#define BROKER_PEERING_HH

#include <memory>
#include <utility>
#include <cstdint>
#include <string>
#include <chrono>

namespace broker { class peering; }
namespace std { template<> struct std::hash<broker::peering>; }

namespace broker {

class endpoint;

class peering {
friend class endpoint;
friend struct std::hash<peering>;

public:

	peering();

	~peering();

	bool remote() const;

	const std::pair<std::string, uint16_t>& remote_tuple() const;

	void handshake() const;
	bool handshake(std::chrono::duration<double> timeout) const;

	explicit operator bool() const;

	bool operator==(const peering& rhs) const;

private:

	class impl;

	peering(std::shared_ptr<impl> p);

	std::shared_ptr<impl> pimpl;
};

} // namespace broker

namespace std {
template <> struct hash<broker::peering> {
	size_t operator()(const broker::peering& p) const;
};
}

#endif // BROKER_PEERING_HH
