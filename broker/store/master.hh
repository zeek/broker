#ifndef BROKER_STORE_MASTER_HH
#define BROKER_STORE_MASTER_HH

#include <broker/store/frontend.hh>
#include <broker/store/store.hh>
#include <broker/store/mem_store.hh>

namespace broker { namespace store {

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

} // namespace store
} // namespace broker

#endif // BROKER_STORE_MASTER_HH
