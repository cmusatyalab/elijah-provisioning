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

#include "cloudletfs-private.h"

struct cloudletfs_stat {
    GMutex *lock;
    bool closed;
    uint64_t u64;
};

struct cloudletfs_stat *_cloudletfs_stat_new(void)
{
    struct cloudletfs_stat *stat;

    stat = g_slice_new0(struct cloudletfs_stat);
    stat->lock = g_mutex_new();
    return stat;
}

void _cloudletfs_stat_close(struct cloudletfs_stat *stat)
{
    g_mutex_lock(stat->lock);
    stat->closed = true;
    g_mutex_unlock(stat->lock);
}

bool _cloudletfs_stat_is_closed(struct cloudletfs_stat *stat)
{
    bool ret;

    g_mutex_lock(stat->lock);
    ret = stat->closed;
    g_mutex_unlock(stat->lock);
    return ret;
}

void _cloudletfs_stat_free(struct cloudletfs_stat *stat)
{
    if (stat == NULL) {
        return;
    }
    g_mutex_free(stat->lock);
    g_slice_free(struct cloudletfs_stat, stat);
}


void _cloudletfs_u64_stat_increment(struct cloudletfs_stat *stat, uint64_t val)
{
    g_mutex_lock(stat->lock);
    stat->u64 += val;
    g_mutex_unlock(stat->lock);
}

uint64_t _cloudletfs_u64_stat_get(struct cloudletfs_stat *stat)
{
    uint64_t ret;

    g_mutex_lock(stat->lock);
    ret = stat->u64;
    g_mutex_unlock(stat->lock);
    return ret;
}
