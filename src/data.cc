#include "broker/data.hh"
#include "broker/convert.hh"

namespace broker {

namespace detail {

command::command(variant_type ys) : xs(std::move(ys)) {
  // nop
}

void intrusive_ptr_add_ref(command* x) {
  x->ref();
}

void intrusive_ptr_release(command* x) {
  x->deref();
}

} // namespace detail

internal_command::internal_command() {
  ptr_.reset(new detail::command(nil));
}

internal_command::internal_command(pointer ptr) : ptr_(std::move(ptr)) {
  // nop
}

detail::command& internal_command::exclusive() {
  if (ptr_->unique())
    return *ptr_;
  auto copy = caf::make_counted<detail::command>(*ptr_);
  ptr_.swap(copy);
  return *ptr_;
}

const detail::command& internal_command::shared() const {
  return *ptr_;
}

long internal_command::compare(const internal_command& x) const {
  // TODO: fixme
  return 0;
}

bool convert(const internal_command& t, std::string& str) {
  caf::detail::stringification_inspector f{str};
  inspect(f, const_cast<internal_command&>(t));
  return true;
}

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

  result_type operator()(bool b) {
    str = b ? 'T' : 'F';
    return true;
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

bool convert(const data& d, std::string& str) {
  visit(data_converter{str}, d);
  return true;
}

} // namespace broker
