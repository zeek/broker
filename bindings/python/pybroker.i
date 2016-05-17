%module pybroker

%{
#include "broker/broker.hh"
#include "broker/report.hh"
#include "broker/endpoint.hh"
#include "broker/data.hh"
#include "broker/store/memory_backend.hh"
#include "broker/store/sqlite_backend.hh"

#ifdef HAVE_ROCKSDB
#include "broker/store/rocksdb_backend.hh"
#endif

#include "broker/store/master.hh"
#include "broker/store/clone.hh"

#if PY_MAJOR_VERSION >= 3
#define PyInt_AsSsize_t PyLong_AsSsize_t
#endif

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
// NIT: Kind of hacky... working around SWIG lack of support for nested classes
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
    std::vector<field> fields()
        { return $self->fields; }
}

class data {
public:

    enum class tag : uint8_t {
        // Primitive types
        boolean,     // bool
        count,       // uint64_t
        integer,     // int64_t
        real,        // double
        string,      // std::string
        address,     // broker::address
        subnet,      // broker::subnet
        port,        // broker::port
        time,        // broker::time_point
        duration,    // broker::time_duration
        enum_value,  // broker::enum_value
        // Compound types
        set,
        table,
        vector,
        record
    };

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
    data::tag which()
        { return broker::which($self->value); }
    bool as_bool()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::boolean )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<bool>($self->value);
        }
    uint64_t as_count()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::count )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<uint64_t>($self->value);
        }
    int64_t as_int()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::integer )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<int64_t>($self->value);
        }
    double as_real()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::real )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<double>($self->value);
        }
    std::string as_string()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::string )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<std::string>($self->value);
        }
    broker::address as_address()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::address )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<broker::address>($self->value);
        }
    broker::subnet as_subnet()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::subnet )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<broker::subnet>($self->value);
        }
    broker::port as_port()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::port )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<broker::port>($self->value);
        }
    broker::time_point as_time()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::time )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<broker::time_point>($self->value);
        }
    broker::time_duration as_duration()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::duration )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<broker::time_duration>($self->value);
        }
    broker::enum_value as_enum()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::enum_value )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<broker::enum_value>($self->value);
        }
    std::set<broker::data> as_set()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::set )
            {
            set_swig_exception("access to wrong data variant");
            return broker::set{};
            }

        return *broker::get<broker::set>($self->value);
        }
    std::map<broker::data, broker::data> as_table()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::table )
            {
            set_swig_exception("access to wrong data variant");
            return broker::table{};
            }

        return *broker::get<broker::table>($self->value);
        }
    std::vector<broker::data> as_vector()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::vector )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<broker::vector>($self->value);
        }
    broker::record as_record()
        {
        if ( broker::which($self->value) !=
             broker::data::tag::record )
            {
            set_swig_exception("access to wrong data variant");
            return {};
            }

        return *broker::get<broker::record>($self->value);
        }
}

typedef std::vector<broker::data> message;
typedef std::vector<broker::data> vector;
} // namespace broker

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

%template(deque_of_string) std::deque<std::string>;
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

%include "broker/store/identifier.hh"

%ignore broker::store::operator==(const expiration_time&,
                                  const expiration_time&);
%ignore broker::store::operator==(const expirable&, const expirable&);
%include "broker/store/expiration_time.hh"
%extend broker::store::expiration_time {
    bool __eq__(const broker::store::expiration_time& other)
        { return *$self == other; }
}
%extend broker::store::expirable {
    bool __eq__(const broker::store::expirable& other)
        { return *$self == other; }
}

%ignore broker::store::modification_result;
%ignore broker::store::backend::init;
%ignore broker::store::backend::sequence;
%ignore broker::store::backend::insert;
%ignore broker::store::backend::increment;
%ignore broker::store::backend::add_to_set;
%ignore broker::store::backend::remove_from_set;
%ignore broker::store::backend::erase;
%ignore broker::store::backend::expire;
%ignore broker::store::backend::clear;
%ignore broker::store::backend::push_left;
%ignore broker::store::backend::push_right;
%ignore broker::store::backend::pop_left;
%ignore broker::store::backend::pop_right;
%ignore broker::store::backend::lookup;
%ignore broker::store::backend::exists;
%ignore broker::store::backend::keys;
%ignore broker::store::backend::size;
%ignore broker::store::backend::snap;
%ignore broker::store::backend::expiries;
%include "broker/store/backend.hh"

%ignore broker::store::memory_backend::memory_backend(memory_backend&&);
%ignore broker::store::memory_backend::operator=;
%include "broker/store/memory_backend.hh"

%ignore broker::store::sqlite_backend::operator=;
%include "broker/store/sqlite_backend.hh"

#ifdef HAVE_ROCKSDB
%ignore broker::store::rocksdb_backend::operator=;
%ignore broker::store::rocksdb_backend::open;
%include "broker/store/rocksdb_backend.hh"
%extend broker::store::rocksdb_backend {
    %rename(open) wrap_open;
    bool wrap_open(std::string db_path)
        {
        rocksdb::Options options;
        options.create_if_missing = true;
        return $self->open(std::move(db_path), options).ok();
        }
}
#endif

%ignore broker::store::query::process;
%ignore broker::store::operator==(const query&, const query&);
%include "broker/store/query.hh"
%extend broker::store::query {
    bool __eq__(const broker::store::query& other)
        { return *$self == other; }
}

namespace broker { namespace store {
class result {
public:

    enum class tag: uint8_t {
        exists_result,
        size_result,
        lookup_or_pop_result,
        keys_result,
        snapshot_result,
    };

    enum class status : uint8_t {
        success,
        failure,
        timeout
    } stat;
};

%extend result {
    bool __eq__(const broker::store::result& other)
        { return *$self == other; }
    result::tag which()
        { return broker::which($self->value); }
    bool exists()
        {
        if ( broker::which($self->value) !=
             broker::store::result::tag::exists_result )
            {
            set_swig_exception("access to wrong store result variant");
            return {};
            }

        return *broker::get<bool>($self->value);
        }
    uint64_t size()
        {
        if ( broker::which($self->value) !=
             broker::store::result::tag::size_result )
            {
            set_swig_exception("access to wrong store result variant");
            return {};
            }

        return *broker::get<uint64_t>($self->value);
        }
    broker::data data()
        {
        if ( broker::which($self->value) !=
             broker::store::result::tag::lookup_or_pop_result )
            {
            set_swig_exception("access to wrong store result variant");
            return {};
            }

        return *broker::get<broker::data>($self->value);
        }
    std::vector<broker::data> keys()
        {
        if ( broker::which($self->value) !=
             broker::store::result::tag::keys_result )
            {
            set_swig_exception("access to wrong store result variant");
            return {};
            }

        return *broker::get<std::vector<broker::data>>($self->value);
        }
}
}}

%ignore broker::store::operator==(const response&, const response&);
%include "broker/store/response.hh"
%extend broker::store::response {
    bool __eq__(const broker::store::response& other)
        { return *$self == other; }
}

%template(deque_of_response) std::deque<broker::store::response>;
%template(response_queue) broker::queue<broker::store::response>;

%typemap(in) void* {
    if ( PyLong_Check($input) )
        $1 = (void*)(PyLong_AsSsize_t($input));
    else
        $1 = (void*)(PyInt_AsSsize_t($input));
}

%typecheck(SWIG_TYPECHECK_INTEGER) void* {
    $1 = (PyInt_Check($input) || PyLong_Check($input)) ? 1 : 0;
}

%typemap(out) void* { $result = PyLong_FromSize_t((size_t)$1); }

%ignore broker::store::frontend::operator=;
%include "broker/store/frontend.hh"

%ignore broker::store::master::operator=;
%ignore broker::store::master::master;
%include "broker/store/master.hh"
%extend broker::store::master {
    static master* create(const endpoint& e, identifier name,
                          backend* b = nullptr)
        // NIT: a bit hacky... working around SWIG lack of unique_ptr support
        {
        using namespace std;
        using namespace broker::store;

        if ( ! b )
            b = new memory_backend();
        else if ( dynamic_cast<memory_backend*>(b) )
            b = new memory_backend(move(*dynamic_cast<memory_backend*>(b)));
        else if ( dynamic_cast<sqlite_backend*>(b) )
            b = new sqlite_backend(move(*dynamic_cast<sqlite_backend*>(b)));
#ifdef HAVE_ROCKSDB
        else if ( dynamic_cast<rocksdb_backend*>(b) )
            b = new rocksdb_backend(move(*dynamic_cast<rocksdb_backend*>(b)));
#endif
        else
            {
            set_swig_exception("unsupported data store backend");
            return nullptr;
            }

        return new master(e, move(name), unique_ptr<backend>(b));
        }
}

%ignore broker::store::clone::clone;
%include "broker/store/clone.hh"
%extend broker::store::clone {
    static clone* create(const endpoint& e, identifier master_name,
                         std::chrono::duration<double> resync_interval,
                         backend* b = nullptr)
        // NIT: a bit hacky... working around SWIG lack of unique_ptr support
        {
        using namespace std;
        using namespace broker::store;

        if ( ! b )
            b = new memory_backend();
        else if ( dynamic_cast<memory_backend*>(b) )
            b = new memory_backend(move(*dynamic_cast<memory_backend*>(b)));
        else if ( dynamic_cast<sqlite_backend*>(b) )
            b = new sqlite_backend(move(*dynamic_cast<sqlite_backend*>(b)));
#ifdef HAVE_ROCKSDB
        else if ( dynamic_cast<rocksdb_backend*>(b) )
            b = new rocksdb_backend(move(*dynamic_cast<rocksdb_backend*>(b)));
#endif
        else
            {
            set_swig_exception("unsupported data store backend");
            return nullptr;
            }

        return new broker::store::clone(e, move(master_name), resync_interval,
                                        unique_ptr<backend>(b));
        }
}
