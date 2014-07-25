#ifndef BROKER_DATA_CLONE_HH
#define BROKER_DATA_CLONE_HH

#include <broker/data/Facade.hh>

namespace broker { namespace data {

class Clone : public Facade {
public:

	Clone(const Endpoint& e, std::string topic);

	~Clone();

private:

	void* GetBackendHandle() const override;

	class Impl;
	std::unique_ptr<Impl> p;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_CLONE_HH
