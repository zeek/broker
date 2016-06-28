#include "broker/data.hh"
#include "broker/convert.hh"

namespace broker {
namespace {

template <class Container>
void container_convert(Container& c, std::string& str,
                       const char* left, const char* right,
                       const char* delim = ", ") {
  auto first = begin(c);
  auto last = end(c);
  str += left;
  if (first != last) {
    str += to_string(*first);
    while (++first != last)
      str += delim + to_string(*first);
  }
  str += right;
}

struct data_converter {
  using result_type = bool;

  template <class T>
  result_type operator()(const T& x) {
    return convert(x, str);
  }

  result_type operator()(const std::string& x) {
    str = x;
    return true;
  }

  std::string& str;
};

} // namespace <anonymous>

bool convert(const table::value_type& e, std::string& str) {
  str += to_string(e.first) + " -> " + to_string(e.second);
  return true;
}

bool convert(const vector& v, std::string& str) {
  container_convert(v, str, "[", "]");
  return true;
}

bool convert(const set& s, std::string& str) {
  container_convert(s, str, "{", "}");
  return true;
}

bool convert(const table& t, std::string& str) {
  container_convert(t, str, "{", "}");
  return true;
}

bool convert(const record& r, std::string& str) {
  container_convert(r, str, "(", ")");
  return true;
}

bool convert(const data& d, std::string& str) {
  visit(data_converter{str}, d);
  return true;
}

} // namespace broker

// Begin C API
//#include "broker/broker.h"
//using std::nothrow;
//
//int broker_bool_true(const broker_bool* b) {
//  return *reinterpret_cast<const bool*>(b) ? 1 : 0;
//}
//
//void broker_bool_set(broker_bool* b, int true_or_false) {
//  *reinterpret_cast<bool*>(b) = true_or_false;
//}
//
//broker_data* broker_data_create() {
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data());
//}
//
//void broker_data_delete(broker_data* d) {
//  delete reinterpret_cast<broker::data*>(d);
//}
//
//broker_data* broker_data_copy(const broker_data* d) {
//  auto dd = reinterpret_cast<const broker::data*>(d);
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(*dd));
//}
//
//broker_data_type broker_data_which(const broker_data* d) {
//  auto dd = reinterpret_cast<const broker::data*>(d);
//  return static_cast<broker_data_type>(which(*dd));
//}
//
//broker_string* broker_data_to_string(const broker_data* d) {
//  auto dd = reinterpret_cast<const broker::data*>(d);
//  try {
//    auto rval = broker::to_string(*dd);
//    return reinterpret_cast<broker_string*>(new std::string(std::move(rval)));
//  } catch (std::bad_alloc&) {
//    return nullptr;
//  }
//}
//
//int broker_data_eq(const broker_data* a, const broker_data* b) {
//  auto aa = reinterpret_cast<const broker::data*>(a);
//  auto bb = reinterpret_cast<const broker::data*>(b);
//  return *aa == *bb;
//}
//
//int broker_data_lt(const broker_data* a, const broker_data* b) {
//  auto aa = reinterpret_cast<const broker::data*>(a);
//  auto bb = reinterpret_cast<const broker::data*>(b);
//  return *aa < *bb;
//}
//
//size_t broker_data_hash(const broker_data* d) {
//  auto dd = reinterpret_cast<const broker::data*>(d);
//  return std::hash<broker::data>{}(*dd);
//}
//
//broker_data* broker_data_from_bool(int i) {
//  bool b = i;
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(b));
//}
//
//broker_data* broker_data_from_count(uint64_t i) {
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(i));
//}
//
//broker_data* broker_data_from_integer(int64_t i) {
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(i));
//}
//
//broker_data* broker_data_from_real(double i) {
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(i));
//}
//
//broker_data* broker_data_from_string(const broker_string* i) {
//  try {
//    std::string s(broker_string_data(i), broker_string_size(i));
//    return reinterpret_cast<broker_data*>(new broker::data(std::move(s)));
//  } catch (std::bad_alloc&) {
//    return nullptr;
//  }
//}
//
//broker_data* broker_data_from_address(const broker_address* i) {
//  auto ii = reinterpret_cast<const broker::address*>(i);
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(*ii));
//}
//
//broker_data* broker_data_from_subnet(const broker_subnet* i) {
//  auto ii = reinterpret_cast<const broker::subnet*>(i);
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(*ii));
//}
//
//broker_data* broker_data_from_port(const broker_port* i) {
//  auto ii = reinterpret_cast<const broker::port*>(i);
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(*ii));
//}
//
//broker_data* broker_data_from_time(const broker_time_point* i) {
//  auto ii = reinterpret_cast<const broker::time::point*>(i);
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(*ii));
//}
//
//broker_data* broker_data_from_duration(const broker_time_duration* i) {
//  auto ii = reinterpret_cast<const broker::time::duration*>(i);
//  return reinterpret_cast<broker_data*>(new (nothrow) broker::data(*ii));
//}
//
//broker_data* broker_data_from_enum(const broker_enum_value* i) {
//  auto ii = reinterpret_cast<const broker::enum_value*>(i);
//  try {
//    return reinterpret_cast<broker_data*>(new broker::data(*ii));
//  } catch (std::bad_alloc&) {
//    return nullptr;
//  }
//}
//
//broker_data* broker_data_from_set(const broker_set* i) {
//  auto ii = reinterpret_cast<const broker::set*>(i);
//  try {
//    return reinterpret_cast<broker_data*>(new broker::data(*ii));
//  } catch (std::bad_alloc&) {
//    return nullptr;
//  }
//}
//
//broker_data* broker_data_from_table(const broker_table* i) {
//  auto ii = reinterpret_cast<const broker::table*>(i);
//  try {
//    return reinterpret_cast<broker_data*>(new broker::data(*ii));
//  } catch (std::bad_alloc&) {
//    return nullptr;
//  }
//}
//
//broker_data* broker_data_from_vector(const broker_vector* i) {
//  auto ii = reinterpret_cast<const broker::vector*>(i);
//  try {
//    return reinterpret_cast<broker_data*>(new broker::data(*ii));
//  } catch (std::bad_alloc&) {
//    return nullptr;
//  }
//}
//
//broker_data* broker_data_from_record(const broker_record* i) {
//  auto ii = reinterpret_cast<const broker::record*>(i);
//  try {
//    return reinterpret_cast<broker_data*>(new broker::data(*ii));
//  } catch (std::bad_alloc&) {
//    return nullptr;
//  }
//}
//
//broker_bool* broker_data_as_bool(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_bool*>(broker::get<bool>(*ii));
//}
//
//uint64_t* broker_data_as_count(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return broker::get<uint64_t>(*ii);
//}
//
//int64_t* broker_data_as_integer(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return broker::get<int64_t>(*ii);
//}
//
//double* broker_data_as_real(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return broker::get<double>(*ii);
//}
//
//broker_string* broker_data_as_string(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_string*>(broker::get<std::string>(*ii));
//}
//
//broker_address* broker_data_as_address(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_address*>(broker::get<broker::address>(*ii));
//}
//
//broker_subnet* broker_data_as_subnet(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_subnet*>(broker::get<broker::subnet>(*ii));
//}
//
//broker_port* broker_data_as_port(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_port*>(broker::get<broker::port>(*ii));
//}
//
//broker_time_point* broker_data_as_time(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_time_point*>(
//    broker::get<broker::time::point>(*ii));
//}
//
//broker_time_duration* broker_data_as_duration(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_time_duration*>(
//    broker::get<broker::time::duration>(*ii));
//}
//
//broker_enum_value* broker_data_as_enum(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_enum_value*>(
//    broker::get<broker::enum_value>(*ii));
//}
//
//broker_set* broker_data_as_set(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_set*>(broker::get<broker::set>(*ii));
//}
//
//broker_table* broker_data_as_table(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_table*>(broker::get<broker::table>(*ii));
//}
//
//broker_vector* broker_data_as_vector(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_vector*>(broker::get<broker::vector>(*ii));
//}
//
//broker_record* broker_data_as_record(broker_data* i) {
//  auto ii = reinterpret_cast<broker::data*>(i);
//  return reinterpret_cast<broker_record*>(broker::get<broker::record>(*ii));
//}
//
//const broker_bool* broker_data_as_bool_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_bool*>(broker::get<bool>(*ii));
//}
//
//const uint64_t* broker_data_as_count_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return broker::get<uint64_t>(*ii);
//}
//
//const int64_t* broker_data_as_integer_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return broker::get<int64_t>(*ii);
//}
//
//const double* broker_data_as_real_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return broker::get<double>(*ii);
//}
//
//const broker_string* broker_data_as_string_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_string*>(broker::get<std::string>(*ii));
//}
//
//const broker_address* broker_data_as_address_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_address*>(
//    broker::get<broker::address>(*ii));
//}
//
//const broker_subnet* broker_data_as_subnet_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_subnet*>(
//    broker::get<broker::subnet>(*ii));
//}
//
//const broker_port* broker_data_as_port_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_port*>(broker::get<broker::port>(*ii));
//}
//
//const broker_time_point* broker_data_as_time_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_time_point*>(
//    broker::get<broker::time::point>(*ii));
//}
//
//const broker_time_duration*
//broker_data_as_duration_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_time_duration*>(
//    broker::get<broker::time::duration>(*ii));
//}
//
//const broker_enum_value* broker_data_as_enum_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_enum_value*>(
//    broker::get<broker::enum_value>(*ii));
//}
//
//const broker_set* broker_data_as_set_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_set*>(broker::get<broker::set>(*ii));
//}
//
//const broker_table* broker_data_as_table_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_table*>(broker::get<broker::table>(*ii));
//}
//
//const broker_vector* broker_data_as_vector_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_vector*>(
//    broker::get<broker::vector>(*ii));
//}
//
//const broker_record* broker_data_as_record_const(const broker_data* i) {
//  auto ii = reinterpret_cast<const broker::data*>(i);
//  return reinterpret_cast<const broker_record*>(
//    broker::get<broker::record>(*ii));
//}
