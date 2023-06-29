#include "broker/data.hh"
#include "broker/data_view.hh"

#include "caf/detail/print.hpp"

#include <string>
#include <string_view>

namespace broker::internal {

struct stringifier {
  using dvv = detail::data_view_value;

  std::string& str;

  template <class T>
  void operator()(const T& value) {
    if constexpr (std::is_same_v<T, std::string_view>
                  || std::is_same_v<T, std::string>) {
      str.insert(str.end(), value.begin(), value.end());
    } else if constexpr (std::is_same_v<T, enum_value_view>
                         || std::is_same_v<T, enum_value>) {
      str.insert(str.end(), value.name.begin(), value.name.end());
    } else if constexpr (std::is_same_v<T, dvv::set_view*>
                         || std::is_same_v<T, set>) {
      add_range(value, '{', '}');
    } else if constexpr (std::is_same_v<T, dvv::table_view*>
                         || std::is_same_v<T, table>) {
      add_range(value, '{', '}');
    } else if constexpr (std::is_same_v<T, dvv::vector_view*>
                         || std::is_same_v<T, vector>) {
      add_range(value, '(', ')');
    } else if constexpr (std::is_same_v<T,bool>) {
      if (value)
        str += 'T';
      else
        str += 'F';
    } else if constexpr (std::is_same_v<T, timespan>) {
      str += std::to_string(value.count());
      str += "ns";
    } else if constexpr (std::is_same_v<T, timestamp>) {
      (*this)(value.time_since_epoch());
    } else {
      using std::to_string;
      str += to_string(value);
    }
  }

  template <class T1, class T2>
  void operator()(const std::pair<T1, T2>& kvp) {
    (*this)(kvp.first);
    str += " -> ";
    (*this)(kvp.second);
  }

  void operator()(const data& value) {
    std::visit(*this, value.get_data());
  }

  void operator()(const dvv& value) {
    std::visit(*this, value.data);
  }

  void operator()(const data_view& value) {
    std::visit(*this, value.as_variant());
  }

  template <class Iterator, class Sentinel>
  void add_range(Iterator first, Sentinel last, char left, char right) {
    str += left;
    if (first != last) {
      (*this)(*first++);
      while (first != last) {
        str += ", ";
        (*this)(*first++);
      }
    }
    str += right;
  }

  template <class Range>
  void add_range(const Range& range, char left, char right) {
    if constexpr (std::is_pointer_v<Range>)
      add_range(range->begin(), range->end(), left, right);
    else
      add_range(range.begin(), range.end(), left, right);
  }
};

} // namespace broker::internal
