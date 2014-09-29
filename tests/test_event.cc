#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/event_queue.hh"
#include "testsuite.hh"
#include <vector>
#include <string>

using namespace broker;

int main(int argc, char** argv)
	{
	init();

	endpoint node0("node0");
	event_queue q0("a", node0);

	auto remote = argc > 1 && std::string(argv[1]) == "remote";

	if ( remote && ! node0.listen(9999, "127.0.0.1") )
		{
		std::cerr << node0.last_error() << std::endl;
		return 1;
		}

	endpoint node1("node1");
	event_queue q1("b", node1);

	if ( remote )
		{
		if ( node1.peer("127.0.0.1", 9999).handshake() !=
		     peering::handshake_status::success )
			{
			BROKER_TEST(false);
			return 1;
			}
		}
	else
		node0.peer(node1).handshake();

	std::vector<event_msg> pings;
	std::vector<event_msg> pongs;

	for ( int i = 0; i < 4; ++i )
		{
		pings.push_back({"ping", {i, "yo"}});
		node1.event("a", pings[i]);
		}

	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q0.need_pop() )
			{
			msg.name = "pong";
			node0.event("b", msg);
			pongs.push_back(std::move(msg));
			}
		}

	std::vector<event_msg> returned;

	while ( returned.size() != 4 )
		for ( auto& msg : q1.need_pop() )
			returned.push_back(std::move(msg));

	BROKER_TEST(returned.size() == 4);
	BROKER_TEST(returned == pongs);

	for ( int i = 0; i < returned.size(); ++i )
		{
		BROKER_TEST(returned[i].name == "pong");
		BROKER_TEST(returned[i].args[0] == i);
		BROKER_TEST(returned[i].args[1] == "yo");
		}

	done();
	return BROKER_TEST_RESULT();
	}
