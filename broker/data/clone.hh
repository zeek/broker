#ifndef BROKER_DATA_CLONE_HH
#define BROKER_DATA_CLONE_HH

#include <broker/data/frontend.hh>

namespace broker { namespace data {

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

} // namespace data
} // namespace broker

#endif // BROKER_DATA_CLONE_HH
