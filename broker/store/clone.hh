#ifndef BROKER_STORE_CLONE_HH
#define BROKER_STORE_CLONE_HH

#include <broker/store/frontend.hh>

namespace broker { namespace store {

class clone : public frontend {
public:

	clone(const endpoint& e, std::string topic,
	      std::chrono::duration<double> resync_interval =
	                                        std::chrono::seconds(1));

	~clone();

	clone(const clone& other) = delete;

	clone(clone&& other);

	clone& operator=(const clone& other) = delete;

	clone& operator=(clone&& other);

private:

	void* handle() const override;

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_CLONE_HH
