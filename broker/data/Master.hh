#ifndef BROKER_DATA_MASTER_HH
#define BROKER_DATA_MASTER_HH

#include <broker/data/Facade.hh>
#include <broker/data/Store.hh>
#include <broker/data/InMemoryStore.hh>

namespace broker { namespace data {

class Master : public Facade {
public:

	Master(const Endpoint& e, std::string topic, std::unique_ptr<Store> s =
	                            std::unique_ptr<Store>{new InMemoryStore});

	~Master();

private:

	void* GetBackendHandle() const override;

	class Impl;
	std::unique_ptr<Impl> p;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_MASTER_HH
