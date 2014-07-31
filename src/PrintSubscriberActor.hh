#ifndef BROKER_PRINTSUBSCRIBERACTOR_HH
#define BROKER_PRINTSUBSCRIBERACTOR_HH

#include "broker/PrintHandler.hh"
#include "Subscription.hh"

#include <caf/sb_actor.hpp>

namespace broker {

class PrintSubscriberActor : public caf::sb_actor<PrintSubscriberActor> {
friend class caf::sb_actor<PrintSubscriberActor>;

public:

	PrintSubscriberActor(std::string topic, broker::PrintHandler::Callback cb,
	                     void* cookie)
		{
		using namespace caf;
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

	caf::behavior active;
	caf::behavior& init_state = active;
};

} // namespace broker

#endif // BROKER_PRINTSUBSCRIBERACTOR_HH
