
redef Broker::default_listen_retry=1secs;
redef exit_only_after_terminate = T;

type Stats: record {
    start: time;
    dt: interval;
    num_events: count &default=0;
};

global stats : Stats;

global stats_update: event(stats: Stats);

event event_1(i: int, s: string)
	{
	stats$num_events += 1;
	}

function clear_stats()
	{
	local s: Stats;
	stats = s;
	stats$start = current_time();
	}

event send_stats()
	{
	stats$dt = (current_time() - stats$start);
	local e = Broker::make_event(stats_update, stats);
	Broker::publish("/benchmark/stats", e);
	clear_stats();
	schedule 1secs { send_stats() };
	}

event bro_init()
	{
	Broker::subscribe("/benchmark/events");
	Broker::listen("127.0.0.1");
	clear_stats();
	schedule 1secs { send_stats() };
	}
