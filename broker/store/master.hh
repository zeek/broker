#ifndef BROKER_STORE_MASTER_HH
#define BROKER_STORE_MASTER_HH

#include <broker/store/identifier.hh>
#include <broker/store/frontend.hh>
#include <broker/store/backend.hh>
#include <broker/store/memory_backend.hh>

namespace broker { namespace store {

/**
 * Data store visible locally and for neighbors only, 
 * via single-hop communication 
 */
constexpr int LOCAL_STORE = 0x01;

/**
 * Data store visible globally 
 * via multi-hop communication 
 */
constexpr int GLOBAL_STORE = 0x02;
/**
 * A master data store.  This type of store is "authoritative" over all its
 * contents meaning that if a clone makes an update, it sends it to the master
 * so that it can make all updates and rebroadcast them to all other clones
 * in a canonical order.
 */

class master : public frontend {
public:

	/**
	 * Construct a master data store.
	 * @param e the broker endpoint to attach the master.
	 * @param name a unique name associated with the master store.
	 * A frontend/clone of the master must also use this name and connect via
	 * the same endpoint or via one of its peers.
	 * @param s the storage backend implementation to use.
	 * @param flags determines the visibility of the store (local=single vs. global=multi-hop)
	 */
	master(const endpoint& e, identifier name,
	       std::unique_ptr<backend> s =
	       std::unique_ptr<backend>(new memory_backend),
				 int flags = LOCAL_STORE 
				 );

	/**
	 * Destructor.
	 */
	~master();

	/**
	 * Copying a master store is not allowed.
	 */
	master(const master& other) = delete;

	/**
	 * Construct a master store by stealing another.
	 */
	master(master&& other);

	/**
	 * Copying a master store is not allowed.
	 */
	master& operator=(const master& other) = delete;

	/**
	 * Construct a master store by stealing another.
	 */
	master& operator=(master&& other);

private:

	void* handle() const override;

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_MASTER_HH
