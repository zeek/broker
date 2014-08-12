#ifndef BROKER_DATA_CLONE_HH
#define BROKER_DATA_CLONE_HH

#include <broker/data/frontend.hh>

namespace broker { namespace data {

class clone : public frontend {
public:

	clone(const endpoint& e, std::string topic);

private:

	void* handle() const override;

	class impl;
	std::shared_ptr<impl> pimpl;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_CLONE_HH
