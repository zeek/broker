#ifndef BROKER_BAD_VARIANT_ACCESS_HH
#define BROKER_BAD_VARIANT_ACCESS_HH

#include <exception>

namespace broker {

class bad_variant_access : public std::exception {
public:
  bad_variant_access() = default;

  const char* what() const noexcept override {
    return "bad variant access";
  }
};

} // namespace broker

#endif // BROKER_BAD_VARIANT_ACCESS_HH
