#ifndef BROKER_STORE_FRONTEND_IMPL_HH
#define BROKER_STORE_FRONTEND_IMPL_HH

#include "broker/store/frontend.hh"
#include "../subscription.hh"
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>

namespace broker { namespace store {

class requester : public caf::sb_actor<requester> {
friend class caf::sb_actor<requester>;

public:

	requester(caf::actor backend, subscription topic, query q, caf::actor queue,
	          std::chrono::duration<double> timeout, void* cookie)
		: request(std::move(q))
		{
		using namespace std;
		using namespace caf;

		bootstrap = (
		after(chrono::seconds::zero()) >> [=]
			{
			send(backend, std::move(topic), request, this);
			become(awaiting_response);
			}
		);

		awaiting_response = (
		on_arg_match >> [=](const actor&, result& r)
			{
			send(queue, response{std::move(request), std::move(r), cookie});
			quit();
			},
		after(timeout) >> [=]
			{
			send(queue, response{std::move(request),
			                     result(result::status::timeout), cookie});
			quit();
			}
		);
		}

private:

	caf::behavior bootstrap;
	caf::behavior awaiting_response;
	caf::behavior& init_state = bootstrap;
	query request;
};

class frontend::impl {
public:

	impl(std::string t, caf::actor e)
		: topic(std::move(t)), endpoint(std::move(e)),
	      request_topic(subscription{subscription_type::store_query, topic}),
	      update_topic(subscription{subscription_type::store_update, topic}),
	      responses()
		{ }

	std::string topic;
	caf::actor endpoint;
	subscription request_topic;
	subscription update_topic;
	response_queue responses;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_FRONTEND_IMPL_HH
