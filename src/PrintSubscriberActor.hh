#ifndef BROKER_PRINTSUBSCRIBERACTOR_HH
#define BROKER_PRINTSUBSCRIBERACTOR_HH

#include <cppa/cppa.hpp>

#include "broker/PrintHandler.hh"
#include "Subscription.hh"

namespace broker {

class PrintSubscriberActor : public cppa::sb_actor<PrintSubscriberActor> {
friend class cppa::sb_actor<PrintSubscriberActor>;

public:

	PrintSubscriberActor(std::string topic, broker::PrintHandler::Callback cb,
	                     void* cookie)
		{
		using namespace cppa;
		using namespace std;

		active = (
		on(atom("quit")) >> [=]
			{
			quit();
			},
		on<SubscriptionTopic, string>() >> [=](SubscriptionTopic st, string msg)
			{
			cb(move(st.topic), move(msg), cookie);
			},
		others() >> [=]
			{
			// Shouldn't get other messages, drop.
			}
		);
		}

private:

	cppa::behavior active;
	cppa::behavior& init_state = active;
};

} // namespace broker

#endif // BROKER_PRINTSUBSCRIBERACTOR_HH
