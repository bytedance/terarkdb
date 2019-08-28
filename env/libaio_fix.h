//
// Created by leipeng on 2019-08-15.
//
// To workaround missing libaio-dev
//

#pragma once

#if __has_include(<libaio.h>)

#include <libaio.h> // requires libaio-dev

#else

#include <unistd.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */
#include <linux/aio_abi.h>
#include <linux/time.h>

inline int io_setup(unsigned nr_events, aio_context_t *ctx_idp) {
    int ret = (int)syscall(__NR_io_setup, nr_events, ctx_idp);
    if (ret >= 0)
        return ret;
    else
        return -errno; // behave like libaio
}

inline int io_destroy(aio_context_t ctx_id) {
    int ret = (int)syscall(__NR_io_destroy, ctx_id);
    if (ret >= 0)
        return ret;
    else
        return -errno; // behave like libaio
}

inline int io_submit(aio_context_t ctx_id, long nr, struct iocb **iocbpp) {
    int ret = (int)syscall(__NR_io_submit, ctx_id, nr, iocbpp);
    if (ret >= 0)
        return ret;
    else
        return -errno; // behave like libaio
}

inline int io_getevents(aio_context_t ctx_id, long min_nr, long nr,
                        struct io_event *events, struct timespec *timeout) {
    int ret = (int)syscall(__NR_io_getevents, ctx_id, min_nr, nr, events, timeout);
    if (ret >= 0)
        return ret;
    else
        return -errno; // behave like libaio
}

#endif
