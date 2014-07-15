#ifndef BROKER_PRINTHANDLER_HH
#define BROKER_PRINTHANDLER_HH

#include <broker/Endpoint.hh>

#include <string>
#include <memory>
#include <functional>

namespace broker {

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

} // namespace broker

#endif // BROKER_PRINTHANDLER_HH
