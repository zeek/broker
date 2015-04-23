#ifndef BROKER_STORE_CLONE_HH
#define BROKER_STORE_CLONE_HH

#include <broker/store/frontend.hh>
#include <broker/store/memory_backend.hh>

namespace broker { namespace store {

/**
 * A clone of a master data store.  The clone automatically synchronizes to
 * the master version by receiving updates made to the master and applying them
 * locally.  Queries to a cloned store may be quicker than queries to a
 * non-local master store.
 */
class clone : public frontend {
public:

	/**
	 * Construct a data store clone.
	 * @param e the broker endpoint to attach the clone.
	 * @param master_name the exact name that the master data store is using.
	 * The master store must be attached either directly to the same endpoint
	 * or to one of its peers.  If attached to a peer, the endpoint must
	 * allow advertising interest in this name.
	 * @param resync_interval the interval at which to re-attempt synchronizing
	 * with the master store should the connection be lost.  If the
	 * clone has not yet synchronized for the first time, updates and queries
	 * queue up until the synchronization completes.  Afterwards, if the
	 * connection to the master store is lost, queries continue to use the
	 * clone's version of the store, but updates will be lost until the master
	 * is once again available.
	 * @param b a backend storage implementation for the clone to use.
	 */
	clone(const endpoint& e, identifier master_name,
	      std::chrono::duration<double> resync_interval =
	                               std::chrono::seconds(1),
	      std::unique_ptr<backend> b =
	                               std::unique_ptr<backend>(new memory_backend)
	      );

	/**
	 * Destructor.
	 */
	~clone();

	/**
	 * Copying a clone is not allowed.
	 */
	clone(const clone& other) = delete;

	/**
	 * Construct a data store clone by stealing another one.
	 */
	clone(clone&& other);

	/**
	 * Copying a clone is not allowed.
	 */
	clone& operator=(const clone& other) = delete;

	/**
	 * Assign to a data store clone by stealing another one.
	 */
	clone& operator=(clone&& other);

private:

	void* handle() const override;

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_CLONE_HH
