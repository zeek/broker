#ifndef BROKER_PEER_HH
#define BROKER_PEER_HH

#include <memory>
#include <utility>
#include <cstdint>
#include <string>
#include <chrono>

namespace broker {

class Endpoint;

class Peer {
friend class broker::Endpoint;

public:

	Peer();

	Peer(const Peer& other);

	Peer& operator=(const Peer& other);

	~Peer();

	bool Valid() const;

	bool Remote() const;

	const std::pair<std::string, uint16_t>& RemoteTuple() const;

	bool BlockUntilConnected(std::chrono::duration<double> timeout) const;
	void BlockUntilConnected() const;

private:

	class Impl;
	std::unique_ptr<Impl> p;
};

} // namespace broker

#endif // BROKER_PEER_HH
