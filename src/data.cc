#include "broker/data.hh"
#include <sstream>
#include <iomanip>

struct io_format_guard {
	io_format_guard(std::ios& arg_stream)
	    : stream(arg_stream), flags(stream.flags()),
	      precision(stream.precision()), fill(stream.fill())
		{}

	~io_format_guard()
		{
		stream.flags(flags);
		stream.precision(precision);
		stream.fill(fill);
		}

	std::ios& stream;
	std::ios::fmtflags flags;
	std::streamsize precision;
	char fill;
};

struct stream_data {
	using result_type = std::ostream&;

	template <typename T>
	static void stream_element(const T& e, std::ostream& out)
		{ broker::visit(stream_data{out}, e); }

	static void stream_element(const broker::record::field& e,
	                           std::ostream& out)
		{
		if ( e )
			broker::visit(stream_data{out}, *e);
		else
			out << "<nil>";
		}

	static void stream_element(const broker::table::value_type& e,
	                           std::ostream& out)
		{
		out << "[";
		broker::visit(stream_data{out}, e.first);
		out << "] = ";
		broker::visit(stream_data{out}, e.second);
		}

	template <typename T>
	static result_type stream_container(const T& container, std::ostream& out,
	                                    std::string begin, std::string end,
	                                    std::string delimiter = ", ")
		{
		out << begin;

		bool first = true;

		for ( const auto& e : container )
			{
			if ( first )
				first = false;
			else
				out << delimiter;

			stream_element(e, out);
			}

		out << end;
		return out;
		}

	result_type operator()(const broker::set& d)
		{ return stream_container(d, out, "{", "}"); }

	result_type operator()(const broker::table& d)
		{ return stream_container(d, out, "{", "}"); }

	result_type operator()(const broker::vector& d)
		{ return stream_container(d, out, "[", "]"); }

	result_type operator()(const broker::record& d)
		{ return stream_container(d.fields, out, "(", ")"); }

	template <typename T>
	result_type operator()(const T& d)
		{ return out << d; }

	stream_data(std::ostream& arg_out)
	    : out(arg_out), ifg(out)
		{ out << std::fixed << std::showpoint; }

	std::ostream& out;
	io_format_guard ifg;
};

template <typename T>
static std::string data_to_string(const T& d)
	{
	std::ostringstream out;
	broker::visit(stream_data{out}, d);
	return out.str();
	}

std::string broker::to_string(const broker::data& d)
	{ return data_to_string(d); }

std::string broker::to_string(const broker::vector& d)
	{ return data_to_string(d); }

std::string broker::to_string(const broker::set& d)
	{ return data_to_string(d); }

std::string broker::to_string(const broker::table& d)
	{ return data_to_string(d); }

std::string broker::to_string(const broker::record& d)
	{ return data_to_string(d); }

std::ostream& operator<<(std::ostream& out, const broker::data& rhs)
	{
	out << broker::to_string(rhs);
	return out;
	}

std::ostream& operator<<(std::ostream& out, const broker::vector& rhs)
	{ return stream_data{out}(rhs); }

std::ostream& operator<<(std::ostream& out, const broker::set& rhs)
	{ return stream_data{out}(rhs); }

std::ostream& operator<<(std::ostream& out, const broker::table& rhs)
	{ return stream_data{out}(rhs); }

std::ostream& operator<<(std::ostream& out, const broker::record& rhs)
	{ return stream_data{out}(rhs); }
