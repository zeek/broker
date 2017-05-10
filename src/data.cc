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

namespace {

using namespace detail;

template <class T>
long fc(const T& x, const T& y) {
  return (x < y) ? -1 : ((x == y) ? 0 : 1);
}

long fuse(std::initializer_list<long> xs) {
  for (auto x : xs)
    if (x != 0)
      return x;
  return 0;
}

long compare_impl(none, none) {
  return 0;
}

long compare_impl(const put_command& x, const put_command& y) {
  return fuse({fc(x.key, y.key), fc(x.value, y.value), fc(x.expiry, y.expiry)});
}

long compare_impl(const erase_command& x, const erase_command& y) {
  return fc(x.key, y.key);
}

long compare_impl(const add_command& x, const add_command& y) {
  return fuse({fc(x.key, y.key), fc(x.value, y.value), fc(x.expiry, y.expiry)});
}

long compare_impl(const subtract_command& x, const subtract_command& y) {
  return fuse({fc(x.key, y.key), fc(x.value, y.value), fc(x.expiry, y.expiry)});
}

long compare_impl(const snapshot_command& x, const snapshot_command& y) {
  return fc(x.clone, y.clone);
}

template <class T>
struct double_dispatch_phase2 {
  using result_type = long;

  const T& x;

  double_dispatch_phase2(const T& xref) : x(xref) {
    // nop
  }

  long operator()(const T& y) const {
    return compare_impl(x, y);
  }

  template <class U>
  long operator()(const U&) const {
    // must not happen
    return -2;
  }
};

struct double_dispatch_phase1 {
  using result_type = long;

  const internal_command& y;

  template <class T>
  long operator()(const T& x) const {
    double_dispatch_phase2<T> f{x};
    return visit(f, y.shared().xs);
  }
};

} // namespace <anonymous>

long internal_command::compare(const internal_command& x) const {
  auto i0 = static_cast<long>(ptr_->xs.index());
  auto i1 = static_cast<long>(x.shared().xs.index());
  if (i0 != i1)
    return i0 - i1;
  double_dispatch_phase1 f{x};
  return visit(f, shared().xs);
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
