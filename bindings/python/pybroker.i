%module pybroker

%{
#include "broker/broker.hh"
#include "broker/report.hh"
#include "broker/endpoint.hh"
#include "broker/data.hh"
%}

%include "std_string.i"
%include "std_deque.i"

// Suppress "Specialization of non-template" warnings (e.g. std::hash)
#pragma SWIG nowarn=317

%include "broker/broker.hh"

%ignore broker::peering::operator=;
%ignore broker::peering::peering(peering&&);
%ignore broker::peering::peering(std::unique_ptr<impl>);
%include "broker/peering.hh"

%include "broker/topic.hh"
%include "broker/message.hh"
%include "broker/outgoing_connection_status.hh"
%include "broker/incoming_connection_status.hh"

%ignore broker::queue::operator=;
%include "broker/queue.hh"

%template(message_queue_base) broker::queue<broker::message>;
%ignore broker::message_queue::operator=;
%include "broker/message_queue.hh"

%template(outgoing_connection_status_queue)
          broker::queue<broker::outgoing_connection_status>;
%template(incoming_connection_status_queue)
          broker::queue<broker::incoming_connection_status>;

%ignore broker::endpoint::operator=;
%include "broker/endpoint.hh"

%warnfilter(454) broker::report::manager;
%warnfilter(454) broker::report::default_queue;
%rename(report_manager) broker::report::manager;
%rename(report_default_queue) broker::report::default_queue;
%nspace broker::report::level;
%ignore broker::report::operator+;
%rename(report_init) broker::report::init;
%rename(report_done) broker::report::done;
%rename(report_send) broker::report::send;
%rename(report_info) broker::report::info;
%rename(report_warn) broker::report::warn;
%rename(report_error) broker::report::error;

%include "broker/report.hh"
