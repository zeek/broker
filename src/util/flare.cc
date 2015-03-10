#include "flare.hh"
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

broker::util::flare::flare()
    : p(FD_CLOEXEC, FD_CLOEXEC, O_NONBLOCK, O_NONBLOCK)
    {
    }

void broker::util::flare::fire()
    {
    char tmp = 0;

    for ( ; ; )
        {
        int n = write(p.write_fd(), &tmp, 1);

        if ( n > 0 )
            // Success -- wrote a byte to pipe.
            break;

        if ( n < 0 && errno == EAGAIN )
            // Success -- pipe is full and just need at least one byte in it.
            break;

        // Loop because either the byte wasn't written or got EINTR error.
        }
    }

void broker::util::flare::extinguish()
    {
    char tmp[256];

    for ( ; ; )
        if ( read(p.read_fd(), &tmp, sizeof(tmp)) == -1 && errno == EAGAIN )
            // Pipe is now drained.
            break;
    }
