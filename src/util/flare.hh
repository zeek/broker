#ifndef BROKER_UTIL_FLARE_HH
#define BROKER_UTIL_FLARE_HH

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <cstdlib>
#include <cstdio>
#include <utility>
#include <memory>

namespace broker {

class pipe {
public:

	pipe()
	    : fds(std::make_shared<file_descriptors>())
		{ }

	int read_fd() const
		{ return (*fds)[0]; }

	int write_fd() const
		{ return (*fds)[1]; }

private:

	class file_descriptors {
	public:

		file_descriptors()
			{
			// TODO: pipe2 too newish, but can atomically set cloexec
			if ( ::pipe(fds) ) fail();
			fcntl(fds[0], F_SETFD, fcntl(fds[0], F_GETFD) | FD_CLOEXEC);
			fcntl(fds[1], F_SETFD, fcntl(fds[1], F_GETFD) | FD_CLOEXEC);
			}

		~file_descriptors()
			{
			close(fds[0]);
			close(fds[1]);
			}

		const int& operator[] (const int idx) const
			{ return fds[idx]; }

	private:

		void fail()
			{
			char tmp[256];
			strerror_r(errno, tmp, sizeof(tmp));
			fprintf(stderr, "pipe falure: %s\n", tmp);
			abort();
			}

		int fds[2];
	};

	std::shared_ptr<file_descriptors> fds;
};

class flare {
public:

	flare()
	    : fired(false), p({})
		{ }

	int fd() const
		{
		return p.read_fd();
		}

	void fire()
		{
		if ( fired ) return;

		char tmp;
		write(p.write_fd(), &tmp, 1);
		fired = true;
		}

	void extinguish()
		{
		if ( ! fired ) return;

		char tmp;
		read(p.read_fd(), &tmp, 1);
		fired = false;
		}

private:

	bool fired;
	pipe p;
};

} // namespace broker

#endif // BROKER_UTIL_FLARE_HH
