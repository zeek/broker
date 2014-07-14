#ifndef BROKER_BROKER_HH
#define BROKER_BROKER_HH

#include "broker/broker.h"

#include <memory>
#include <string>
#include <cstdint>
#include <chrono>
#include <functional>
#include <unordered_set>
#include <unordered_map>

namespace broker {

/**
 * Initialize the broker library.  This should be called once before using
 * anything else that's provided by the library.
 * @param flags tune behavior of the library.  No flags exist yet.
 * @return 0 if library is initialized, else an error code that can
 *         be supplied to broker::strerror().
 */
int init(int flags = 0);

/**
 * Shutdown the broker library.  No functionality provided by the library
 * is guaranteed to work after the call.
 */
void done();

/**
 * @return a textual representation of a broker error code.
 */
const char* strerror(int errno);

using PeerNum = int;

class Endpoint {
public:

	/**
	 * Create a local broker endpoint.
	 * @param name a descriptive name for this endpoint.
	 * @param flags tune the behavior of the endpoint.  No flags exist yet.
	 */
	Endpoint(std::string name, int flags = 0);

	/**
	 * Shutdown the local broker endpoint and disconnect from peers.
	 */
	~Endpoint();

	/**
	 * Make this local broker endpoint available for remote peer connections.
	 * @param addr an address to accept on, e.g. "localhost" or "127.0.0.1".
	 * @param port the TCP port on which to accept connections.
	 * @return 0 if the endpoint is now listening, else an error code that
	 *         can be supplied to broker::strerror().
	 */
	int Listen(std::string addr, uint16_t port);

	/**
	 * Connect to a remote endpoint.
	 * @param addr an address to connect to, e.g. "localhost" or "127.0.0.1".
	 * @param port the TCP port on which the remote is listening.
	 * @param retry an interval at which to retry establishing the connection
	 *        with the remote peer.
	 * @return a peer number that this endpoint can use to identify the
	 *         particular peer established by this method.
	 */
	PeerNum AddPeer(std::string addr, uint16_t port,
	        std::chrono::duration<double> retry = std::chrono::seconds(5));

	/**
	 * Connect to a local endpoint.
	 * @param e another local endpoint.
	 * @return a peer number that this endpoint can use to identify the
	 *         particular peer established by this method.
	 */
	PeerNum AddPeer(const Endpoint& e);

	/**
	 * Remove a connection to a peer endpoint.
	 * @param peerno a peer number previously returned by AddPeer().
	 * @return false if no such peer associated exists, else true (and the
	 *         peering is no more).
	 */
	bool RemPeer(PeerNum peernum);

	/**
	 * Sends a message string to all PrintHandler's for a given topic that are
	 * connected to this endpoint directly or indirectly through peer endpoints.
	 * @param topic the topic associated with the message.
	 * @param msg a string to send all handlers for the topic.
	 */
	void Print(const std::string& topic, const std::string& msg) const;

	// TODO: Event() is similar to Print but w/ fancier arguments.
	// TODO: Log() is similar to Print but w/ fancier arguments.

private:

	class Impl;
	std::unique_ptr<Impl> p;
};

class PrintHandler {
public:

	using Callback = std::function<void (const std::string& topic,
	                                     const std::string& msg,
	                                     void* cookie)>;

	/**
	 * Create a handler for print messages sent to an endpoint either directly
	 * or via peers.
	 * @param e local endpoint.
	 * @param topic a topic string associated with the message.  Only
	 *        Endpoint::Print()'s with a matching topic are handled.
	 * @param cb a function to call for each print message received.
	 * @param cookie supplied as an argument when executing the callback.
	 */
	PrintHandler(const Endpoint& e, std::string topic,
	             Callback cb, void* cookie = nullptr);

	/**
	 * @return the topic associated with the handler.
	 */
	const std::string& Topic() const;

	/**
	  * Stop receiving print messages.
	  */
	~PrintHandler();

private:

	class Impl;
	std::unique_ptr<Impl> p;
};

namespace data {

// TODO: Key and Val should be Bro types?
using Key = std::string;
using Val = std::string;

using LookupCallback = std::function<void (Key key, std::unique_ptr<Val> val,
                                           void* cookie)>;
using KeysCallback = std::function<void (std::unordered_set<Key> keys,
                                         void* cookie)>;
using SizeCallback = std::function< void (uint64_t size, void* cookie)>;

class Store {
public:

	virtual ~Store() { }

	void Insert(Key k, Val v)
		{ DoInsert(std::move(k), std::move(v)); }

	void Erase(Key k)
		{ DoErase(std::move(k)); }

	void Clear()
		{ DoClear(); }

	std::unique_ptr<Val> Lookup(Key k)
		{ return DoLookup(std::move(k)); }

	bool HasKey(Key k)
		{ return DoHasKey(std::move(k)); }

	std::unordered_set<Key> Keys()
		{ return DoKeys(); }

	uint64_t Size()
		{ return DoSize(); }

private:

	virtual void DoInsert(Key k, Val v) = 0;

	virtual void DoErase(Key k) = 0;

	virtual void DoClear() = 0;

	virtual std::unique_ptr<Val> DoLookup(Key k) = 0;

	virtual bool DoHasKey(Key k) = 0;

	virtual std::unordered_set<Key> DoKeys() = 0;

	virtual uint64_t DoSize() = 0;
};

class InMemoryStore : public Store {
private:

	void DoInsert(Key k, Val v) override
		{ store.insert(std::make_pair<Key, Val>(std::move(k), std::move(v))); }

	void DoErase(Key k) override
		{ store.erase(k); }

	void DoClear() override
		{ store.clear(); }

	std::unique_ptr<Val> DoLookup(Key k) override
		{
		try { return std::unique_ptr<Val>(new Val(store.at(k))); }
		catch ( std::out_of_range& ) { return {}; }
		}

	bool DoHasKey(Key k) override
		{ if ( store.find(k) == store.end() ) return false; else return true; }

	std::unordered_set<Key> DoKeys() override
		{
		std::unordered_set<Key> rval;
		for ( auto kv : store ) rval.insert(kv.first);
		return rval;
		}

	uint64_t DoSize() override
		{ return store.size(); }

	std::unordered_map<Key, Val> store;
};

class Facade {
public:

	Facade(const Endpoint& e, std::string topic);

	virtual ~Facade();

	const std::string& Topic() const;

	/* Update Interface. */

	void Insert(Key k, Val v) const;

	void Erase(Key k) const;

	void Clear() const;

	/* Synchronous Query Interface.
	   May have high latency as data is not guaranteed to be local. */

	std::unique_ptr<Val> Lookup(Key k) const;

	std::unordered_set<Key> Keys() const;

	uint64_t Size() const;

	/* Asynchronous Query Interface. */

	void Lookup(Key k, LookupCallback cb, void* cookie) const;

	void Keys(KeysCallback cb, void* cookie) const;

	void Size(SizeCallback cb, void* cookie) const;

private:

	class Impl;
	std::unique_ptr<Impl> p;
};

class Master : public Facade {
public:

	Master(const Endpoint& e, std::string topic, Store s = InMemoryStore());

	~Master();

private:

	class Impl;
	std::unique_ptr<Impl> p;
};

class Clone : public Facade {
public:

	Clone(const Endpoint& e, std::string topic);

	~Clone();

private:

	class Impl;
	std::unique_ptr<Impl> p;
};

} // namespace broker::data

} // namespace broker

#endif // BROKER_BROKER_HH
