/*
 * Cloudletfs - virtual filesystem for synthesized VM at Cloudlet
 *
 * Copyright (C) 2006-2014 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/* When a FUSE filesystem operation is interrupted, FUSE delivers SIGUSR1
   to the specific thread performing the operation.  If the FS operation is
   a blocking read on a stream, the signal handler must be able to
   interrupt it.  However, if the blocking read is implemented using GCond
   (and therefore pthread_cond), we have two problems:

   1. pthread_cond_wait() is not guaranteed to return after a signal.

   2. pthread_cond_broadcast() is not async-signal safe, so the signal
   handler can't use it to terminate the wait.

   We therefore implement our own condition variables on top of SIGUSR1.
   libfuse will have already installed a no-op SIGUSR1 handler. */

#include <signal.h>
#include <pthread.h>
#include "cloudletfs-private.h"

struct cloudletfs_cond {
    GMutex *lock;
    GList *threads;
};

struct cond_waiter {
    pthread_t thr;
    bool signaled;
};

struct cloudletfs_cond *_cloudletfs_cond_new(void)
{
    struct cloudletfs_cond *cond;

    cond = g_slice_new0(struct cloudletfs_cond);
    cond->lock = g_mutex_new();
    return cond;
}

void _cloudletfs_cond_free(struct cloudletfs_cond *cond)
{
    g_assert(cond->threads == NULL);
    g_mutex_free(cond->lock);
    g_slice_free(struct cloudletfs_cond, cond);
}

/* Similar to g_cond_wait().  Returns when _cloudletfs_cond_signal() or
   _cloudletfs_cond_broadcast() is called or the current FUSE request is
   interrupted.  Returns true if the request was interrupted, false
   otherwise. */
bool _cloudletfs_cond_wait(struct cloudletfs_cond *cond, GMutex *lock)
{
    struct cond_waiter waiter = {
        .thr = pthread_self(),
    };
    GList *el;
    sigset_t mask;
    sigset_t orig;

    /* Ensure we're notified of lock-protected events */
    sigfillset(&mask);
    pthread_sigmask(SIG_SETMASK, &mask, &orig);
    g_mutex_lock(cond->lock);
    el = cond->threads = g_list_prepend(cond->threads, &waiter);
    g_mutex_unlock(cond->lock);

    /* Permit lock-protected events to occur */
    g_mutex_unlock(lock);

    /* Wait for event, provided that FUSE was not already interrupted
       before we blocked signals */
    if (!_cloudletfs_interrupted()) {
        sigdelset(&mask, SIGUSR1);
        sigsuspend(&mask);
    }

    /* Clean up */
    g_mutex_lock(cond->lock);
    cond->threads = g_list_delete_link(cond->threads, el);
    g_mutex_unlock(cond->lock);
    pthread_sigmask(SIG_SETMASK, &orig, NULL);

    /* Re-acquire parent lock */
    g_mutex_lock(lock);

    return _cloudletfs_interrupted();
}

static void signal_cond(struct cond_waiter *waiter, int32_t *max)
{
    if (!waiter->signaled && *max != 0) {
        pthread_kill(waiter->thr, SIGUSR1);
        waiter->signaled = true;
        if (*max != -1) {
            --*max;
        }
    }
}

void _cloudletfs_cond_signal(struct cloudletfs_cond *cond)
{
    int32_t max = 1;

    g_mutex_lock(cond->lock);
    g_list_foreach(cond->threads, (GFunc) signal_cond, &max);
    g_mutex_unlock(cond->lock);
}

void _cloudletfs_cond_broadcast(struct cloudletfs_cond *cond)
{
    int32_t max = -1;

    g_mutex_lock(cond->lock);
    g_list_foreach(cond->threads, (GFunc) signal_cond, &max);
    g_mutex_unlock(cond->lock);
}
