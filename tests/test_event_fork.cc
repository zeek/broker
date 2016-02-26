#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/message_queue.hh"
#include "testsuite.h"
#include <vector>
#include <string>
#include <iostream>
#include <unistd.h>
#include <signal.h>

// A test of "event" style messages -- vectors of data w/ first element
// being the event name.

using namespace broker;

static const int num_msgs = 4;

static void check_peering(bool incoming, broker::peering p, const std::string& rpn)
	{
	auto rt = p.remote_tuple();
	auto remote = p.remote();

	if ( incoming )
		std::cout << "incoming connection: " << std::endl;
	else
		std::cout << "outgoing connection: " << std::endl;

	std::cout << "  remote_peer_name = " << rpn << std::endl;
	std::cout << "  remote_tuple = " << rt.first << ":" << rt.second << std::endl;
	std::cout << "  is_remote = " << remote << std::endl;

	return;

	if ( incoming )
		{
		BROKER_TEST(rpn == "node1");
		BROKER_TEST(rt.first == "1.2.3.4" && rt.second == 1947);
		BROKER_TEST(remote == true);
		}
	else
		{
		BROKER_TEST(rpn == "node0");
		BROKER_TEST(rt.first == "1.2.3.4" && rt.second == 1947);
		BROKER_TEST(remote == true);
		}
	}

int main(int argc, char** argv)
	{
	pid_t pid = fork();

	if (pid == 0)
		{
		// Child. Node 0.
		init();

		endpoint node0("node0");
		message_queue q0("a", node0);

		if ( ! node0.listen(9999, "127.0.0.1") )
			{
			std::cerr << "listen error: " << node0.last_error() << std::endl;
			return 1;
			}

		auto ics = node0.incoming_connection_status().need_pop().front();

		if ( ics.status !=
		     incoming_connection_status::tag::established)
			{
			BROKER_TEST(false);
			return 1;
			}

		check_peering(true, ics.relation, ics.peer_name);

		std::vector<message> pongs;

		while ( pongs.size() != num_msgs )
			{
			for ( auto& msg : q0.need_pop() )
				{
				msg[0] = "pong";
				node0.send("b", msg);
				pongs.push_back(std::move(msg));
				}
			}

		while ( true )
			sleep(1);

		return 0;
		}
	else
		{
		// Parent. Node 1.
		init();

		endpoint node1("node1");
		message_queue q1("b", node1);

		node1.peer("127.0.0.1", 9999);

		auto ocs = node1.outgoing_connection_status().need_pop().front();

		if ( ocs.status !=
		     outgoing_connection_status::tag::established)
			{
			BROKER_TEST(false);
			return 1;
			}

		check_peering(false, ocs.relation, ocs.peer_name);

		std::vector<message> pings;

		for ( int i = 0; i < num_msgs; ++i )
			{
			pings.push_back(message{"ping", vector{i, "yo"}});
			node1.send("a", pings[i]);
			}

		std::vector<message> returned;

		while ( returned.size() != num_msgs )
			for ( auto& msg : q1.need_pop() )
				returned.push_back(std::move(msg));

		BROKER_TEST(returned.size() == num_msgs);
		// BROKER_TEST(returned == pongs);

		for ( int i = 0; i < (int)returned.size(); ++i )
			{
			std::cout << "Received pong " << i << std::endl;
			BROKER_TEST(returned[i][0] == "pong");
			const auto& v = *get<vector>(returned[i][1]);
			BROKER_TEST(v[0] == i);
			BROKER_TEST(v[1] == "yo");
			}

		kill(pid, SIGTERM);

		return BROKER_TEST_RESULT();
		}
	}
