#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/message_queue.hh"
#include "testsuite.h"
#include <vector>
#include <string>
#include <iostream>

// A test of "log" style messages -- a sequence of {stream name, data record}
// where some elements of the record may be uninitialized.

using namespace broker;

int main(int argc, char** argv)
	{
	init();

	endpoint node0("node0");
	message_queue q0("a", node0);

	auto remote = argc > 1 && std::string(argv[1]) == "remote";

	if ( remote && ! node0.listen(9999, "127.0.0.1") )
		{
		std::cerr << node0.last_error() << std::endl;
		return 1;
		}

	endpoint node1("node1");
	message_queue q1("b", node1);

	if ( remote )
		node1.peer("127.0.0.1", 9999);
	else
		node1.peer(node0);

	auto ps = node1.outgoing_connection_status().need_pop().front();

	if ( ps.status != outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	BROKER_TEST(ps.peer_name == node0.name());
	BROKER_TEST(node0.name() == "node0");

	std::vector<message> pings;
	std::vector<message> pongs;

	for ( int i = 0; i < 4; ++i )
		{
		record r = {{data(i), data("yo"), {}}};
		pings.push_back(message{"ping", std::move(r)});
		node1.send("a", pings[i]);
		}

	while ( pongs.size() != 4 )
		{
		for ( auto& msg : q0.need_pop() )
			{
			msg[0] = "pong";
			node0.send("b", msg);
			pongs.push_back(std::move(msg));
			}
		}

	std::vector<message> returned;

	while ( returned.size() != 4 )
		for ( auto& msg : q1.need_pop() )
			returned.push_back(std::move(msg));

	BROKER_TEST(returned.size() == 4);
	BROKER_TEST(returned == pongs);

	for ( int i = 0; i < (int)returned.size(); ++i )
		{
		BROKER_TEST(returned[i][0] == "pong");
		const auto r = get<record>(returned[i][1]);
		BROKER_TEST(r->get(0) == data(i));
		BROKER_TEST(r->get(1) == data("yo"));
		BROKER_TEST(! r->get(2));
		}

	return BROKER_TEST_RESULT();
	}
