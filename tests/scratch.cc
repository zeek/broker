#include <caf/all.hpp>
#include <cstdio>
#include <iostream>
#include <cstdint>
#include <string>

using namespace std;
using namespace caf;

behavior server(event_based_actor* self)
	{
	return {
		on_arg_match >> [=](string val)
			{
			aout(self) << "server got msg: " << val << endl;
			return make_message("delivered '" + val + "'");
			}
	};
	}

class mitm : public sb_actor<mitm> {
friend class sb_actor<mitm>;
public:

	mitm(actor other) : middle(other)
		{
		auto intercept_request = [=](const string& str) -> optional<bool>
			{
			if ( ! str.compare(0, 4, "mitm") )
				{
				aout(this) << "mitm intercept proj success: " << str << endl;
				return true;
				}

			aout(this) << "mitm intercept proj failed: " << str << endl;
			forward_to(other);
			return {};
			};

		auto forward_request = [=](const string& str) -> optional<bool>
			{
			if ( ! str.compare(0, 6, "server") )
				{
				aout(this) << "mitm forward proj success: " << str << endl;
				return true;
				}

			aout(this) << "mitm forward proj failed: " << str << endl;
			return {};
			};

		active = (
		on(intercept_request) >> [=](bool b)
			{
			return make_message("intercepted");
			},
		/*
		on(forward_request) >> [=](bool b)
			{
			forward_to(other);
			},
		*/
		on_arg_match >> [=](string val)
			{
			aout(this) << "mitm got msg: " << val << endl;
			}
		);
		}

private:

	caf::actor middle;
	behavior active;
	behavior& init_state = active;
};

behavior client(event_based_actor* self, actor other)
	{
	return {
	after(chrono::seconds(0)) >> [=]
		{
		self->send(other, "hi");
		self->sync_send(other, "mitm_ping").then(
			on_arg_match >> [=](string val)
				{
				aout(self) << "client got msg: " << val << endl;
				}
			);
		self->sync_send(other, "server_ping").then(
			on_arg_match >> [=](string val)
				{
				aout(self) << "client got msg: " << val << endl;
				}
			);
		}
	};
	}

int main(int argc, char** argv)
	{
	auto a = spawn(client, spawn<mitm>(spawn(server)));
	await_all_actors_done();
	return 0;
	}
