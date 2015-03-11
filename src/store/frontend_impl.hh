#ifndef BROKER_STORE_FRONTEND_IMPL_HH
#define BROKER_STORE_FRONTEND_IMPL_HH

#include "broker/store/frontend.hh"
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>

namespace broker { namespace store {

class requester : public caf::sb_actor<requester> {
friend class caf::sb_actor<requester>;

public:

	requester(caf::actor backend, identifier master_name, query q,
	          caf::actor queue, std::chrono::duration<double> timeout,
	          void* cookie)
		: request(std::move(q))
		{
		using namespace std;
		using namespace caf;

		bootstrap = {
		after(chrono::seconds::zero()) >> [=]
			{
			send(backend, std::move(master_name), request, this);
			become(awaiting_response);
			}
		};

		awaiting_response = {
		[=](const actor&, result& r)
			{
			send(queue, response{std::move(request), std::move(r), cookie});
			quit();
			},
		after(chrono::duration_cast<chrono::microseconds>(timeout)) >> [=]
			{
			send(queue, response{std::move(request),
			                     result(result::status::timeout), cookie});
			quit();
			}
		};
		}

private:

	caf::behavior bootstrap;
	caf::behavior awaiting_response;
	caf::behavior& init_state = bootstrap;
	query request;
};

class frontend::impl {
public:

	impl(identifier mn, caf::actor e)
		: master_name(std::move(mn)), endpoint(std::move(e)),
		  responses(), self()
		{ }

	std::string master_name;
	caf::actor endpoint;
	response_queue responses;
	caf::scoped_actor self;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_FRONTEND_IMPL_HH
