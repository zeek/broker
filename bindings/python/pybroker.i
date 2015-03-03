%module pybroker

%{
#include "broker/broker.hh"
#include "broker/report.hh"
#include "broker/endpoint.hh"
#include "broker/data.hh"

static std::string swig_exception;

static inline void set_swig_exception(const char* e)
    { swig_exception = e; }
%}

%exception {
    $action

    if ( ! swig_exception.empty() )
        {
        PyErr_SetString(PyExc_RuntimeError, swig_exception.data());
        swig_exception.clear();
        return NULL;
        }
}

%init %{
    broker::init();
%}

%include "stdint.i"
%include "std_string.i"
%include "std_deque.i"
%include "std_vector.i"
%include "std_set.i"
%include "std_map.i"

// Suppress "Specialization of non-template" warnings (e.g. std::hash)
#pragma SWIG nowarn=317
// Suppress "No access specifier given for base class" (e.g. totally_ordered)
#pragma SWIG nowarn=319
// Suppress "Nothing known about base class" (e.g. totally_ordered)
#pragma SWIG nowarn=401

%include "broker/broker.hh"

%typemap(out) const std::array<uint8_t, 16>& {
    $result = PyByteArray_FromStringAndSize(
                  reinterpret_cast<const char*>($1->data()), $1->size());
}

%typemap(in) std::chrono::duration<double> {
    $1 = std::chrono::duration<double>(PyFloat_AsDouble($input));
}

%typecheck(SWIG_TYPECHECK_DOUBLE) std::chrono::duration<double> {
    $1 = (PyFloat_Check($input) || PyInt_Check($input) || PyLong_Check($input)) ? 1 : 0;
}

%ignore broker::address::from_string;
%ignore broker::to_string(const address&);
%ignore operator<(const address&, const address&);
%ignore operator==(const address&, const address&);
%ignore operator<<(std::ostream&, const address&);
%include "broker/address.hh"
%extend broker::address {
    std::string __str__()
        { return broker::to_string(*$self); }
    bool __eq__(const broker::address& other)
        { return *$self == other; }
    bool __lt__(const broker::address& other)
        { return *$self < other; }

    %rename(from_string) wrap_from_string;
    static broker::address wrap_from_string(const std::string& s)
        {
        auto rval = broker::address::from_string(s);

        if ( rval )
            return *rval;

        set_swig_exception("Invalid address string");
        return broker::address();
        }
}

%ignore broker::to_string(const subnet&);
%ignore operator<(const subnet&, const subnet&);
%ignore operator==(const subnet&, const subnet&);
%ignore operator<<(std::ostream&, const subnet&);
%include "broker/subnet.hh"
%extend broker::subnet {
    std::string __str__()
        { return broker::to_string(*$self); }
    bool __eq__(const broker::subnet& other)
        { return *$self == other; }
    bool __lt__(const broker::subnet& other)
        { return *$self < other; }
}

%typemap(in) broker::port::number_type { $1 = PyInt_AsLong($input); }
%typemap(out) broker::port::number_type { $result = PyInt_FromLong($1); }
%ignore broker::to_string(const port&);
%ignore operator<(const port&, const port&);
%ignore operator==(const port&, const port&);
%ignore operator<<(std::ostream&, const port&);
%include "broker/port.hh"
%extend broker::port {
    std::string __str__()
        { return broker::to_string(*$self); }
    bool __eq__(const broker::port& other)
        { return *$self == other; }
    bool __lt__(const broker::port& other)
        { return *$self < other; }
}

%ignore operator<<(std::ostream&, const time_duration&);
%include "broker/time_duration.hh"

%ignore operator<<(std::ostream&, const time_point&);
%include "broker/time_point.hh"

%ignore broker::enum_value::operator=;
%ignore broker::enum_value::enum_value(enum_value&&);
%ignore operator<<(std::ostream&, const enum_value&);
%include "broker/enum_value.hh"

%ignore broker::peering::operator=;
%ignore broker::peering::peering(peering&&);
%ignore broker::peering::peering(std::unique_ptr<impl>);
%include "broker/peering.hh"

%include "broker/topic.hh"

class field {
public:

    field() {}
    field(broker::data arg);
    broker::data get();
    bool valid() const;
};

%{
// Kind of hacky...
typedef broker::record::field field;
%}

%template(vector_of_field) std::vector<field>;

namespace broker {

class record {
public:

    record() {}
    record(broker::record arg);
    record(std::vector<broker::record::field> arg);
    size_t size() const;
};

%extend record {
    std::string __str__()
        { return broker::to_string(*$self); }
    bool __eq__(const broker::record& other)
        { return *$self == other; }
    bool __lt__(const broker::record& other)
        { return *$self < other; }
    std::vector<field>& fields()
        { return $self->fields; }
}

class data {
public:

    data() {}
    data(broker::data arg);
    data(bool arg);
    data(int64_t arg);
    data(uint64_t arg);
    data(double arg);
    data(std::string arg);
    data(address arg);
    data(subnet arg);
    data(port arg);
    data(time_point arg);
    data(time_duration arg);
    data(enum_value arg);
    data(std::vector<broker::data> arg);
    data(std::set<broker::data> arg);
    data(std::map<broker::data, broker::data> arg);
    data(broker::record arg);
};

%extend data {
    std::string __str__()
        { return broker::to_string(*$self); }
    bool __eq__(const broker::data& other)
        { return *$self == other; }
    bool __lt__(const broker::data& other)
        { return *$self < other; }
}

typedef std::vector<broker::data> message;
}

%template(vector_of_data) std::vector<broker::data>;

%pythoncode %{
message = vector_of_data
%}

%template(set_of_data) std::set<broker::data>;

%template(map_of_data) std::map<broker::data, broker::data>;

%include "broker/outgoing_connection_status.hh"
%include "broker/incoming_connection_status.hh"

%ignore broker::queue::operator=;
%include "broker/queue.hh"

%template(deque_of_message) std::deque<broker::message>;
%template(message_queue_base) broker::queue<broker::message>;
%ignore broker::message_queue::operator=;
%include "broker/message_queue.hh"

%template(deque_of_outgoing_connection_status)
          std::deque<broker::outgoing_connection_status>;
%template(outgoing_connection_status_queue)
          broker::queue<broker::outgoing_connection_status>;
%template(deque_of_incoming_connection_status)
          std::deque<broker::incoming_connection_status>;
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
