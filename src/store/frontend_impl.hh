#ifndef BROKER_STORE_FRONTEND_IMPL_HH
#define BROKER_STORE_FRONTEND_IMPL_HH

#include "broker/store/frontend.hh"
#include "broker/topic.hh"
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>

namespace broker { namespace store {

class requester : public caf::sb_actor<requester> {
friend class caf::sb_actor<requester>;

public:

	requester(caf::actor backend, topic t, query q, caf::actor queue,
	          std::chrono::duration<double> timeout, void* cookie)
		: request(std::move(q))
		{
		using namespace std;
		using namespace caf;

		bootstrap = (
		after(chrono::seconds::zero()) >> [=]
			{
			send(backend, std::move(t), request, this);
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
		: topic_name(std::move(t)), endpoint(std::move(e)),
	      request_topic(topic{topic_name, topic::tag::store_query}),
	      update_topic(topic{topic_name, topic::tag::store_update}),
	      responses()
		{ }

	std::string topic_name;
	caf::actor endpoint;
	topic request_topic;
	topic update_topic;
	response_queue responses;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_FRONTEND_IMPL_HH
