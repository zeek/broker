#ifndef BROKER_DATA_MASTER_HH
#define BROKER_DATA_MASTER_HH

#include <broker/data/frontend.hh>
#include <broker/data/store.hh>
#include <broker/data/mem_store.hh>

namespace broker { namespace data {

class master : public frontend {
public:

	master(const endpoint& e, std::string topic, std::unique_ptr<store> s =
	                            std::unique_ptr<store>{new mem_store});

	~master();

	master(const master& other) = delete;

	master(master&& other);

	master& operator=(const master& other) = delete;

	master& operator=(master&& other);

private:

	void* handle() const override;

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_MASTER_HH
