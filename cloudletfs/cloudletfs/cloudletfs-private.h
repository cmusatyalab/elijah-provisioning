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

#ifndef CLOUDLETFS_PRIVATE_H
#define CLOUDLETFS_PRIVATE_H

#include <sys/stat.h>
#include <stdint.h>
#include <stdbool.h>
#include <glib.h>
#include "config.h"
#include <unistd.h>
#include <sys/types.h>

struct cloudletfs {
    struct cloudletfs_image *disk;
    struct cloudletfs_image *memory;
    struct cloudletfs_fuse *fuse;
    GMainLoop *glib_loop;
};

struct cloudletfs_image {
    char *type;
    uint64_t image_size;
    uint32_t chunk_size;

    /* io */
    struct chunk_state *chunk_state;
    struct bitmap_group *bitmaps;
    struct bitmap *accessed_map;

    /* ll_pristine */
    struct bitmap *total_overlay_map;
    struct bitmap *current_overlay_map;
    int base_fd;
    int overlay_fd;

    /* ll_modified */
    int write_fd;
    struct bitmap *modified_map;

    /* stats */
    struct cloudletfs_stream_group *io_stream;
    struct cloudletfs_stat *bytes_read;
    struct cloudletfs_stat *bytes_written;
    struct cloudletfs_stat *chunk_dirties;
};

struct cloudletfs_fuse {
    struct cloudletfs *fs;
    char *mountpoint;
    struct cloudletfs_fuse_dentry *root;
    struct fuse *fuse;
    struct fuse_chan *chan;
};

struct chunk_synthesis_info {
    int chunk_number;
    bool valid; // whether chunk is synthesized or not
};

struct cloudletfs_fuse_fh {
    const struct cloudletfs_fuse_ops *ops;
    void *data;
    void *buf;
    uint64_t length;
    bool blocking;
};

struct cloudletfs_fuse_ops {
    int (*getattr)(void *dentry_ctx, struct stat *st);
    int (*open)(void *dentry_ctx, struct cloudletfs_fuse_fh *fh);
    int (*read)(struct cloudletfs_fuse_fh *fh, void *buf, uint64_t start,
            uint64_t count);
    int (*write)(struct cloudletfs_fuse_fh *fh, const void *buf,
            uint64_t start, uint64_t count);
    void (*release)(struct cloudletfs_fuse_fh *fh);
    bool nonseekable;
};

#define CLOUDLETFS_CONFIG_ERROR _cloudletfs_config_error_quark()
#define CLOUDLETFS_FUSE_ERROR _cloudletfs_fuse_error_quark()
#define CLOUDLETFS_IO_ERROR _cloudletfs_io_error_quark()
#define CLOUDLETFS_STREAM_ERROR _cloudletfs_stream_error_quark()

enum CLOUDLETFSConfigError {
    CLOUDLETFS_CONFIG_ERROR_INVALID_ARGUMENT,
};

enum CLOUDLETFSFUSEError {
    CLOUDLETFS_FUSE_ERROR_FAILED,
    CLOUDLETFS_FUSE_ERROR_BAD_MOUNTPOINT,
};

enum CLOUDLETFSIOError {
    CLOUDLETFS_IO_ERROR_EOF,
    CLOUDLETFS_IO_ERROR_PREMATURE_EOF,
    CLOUDLETFS_IO_ERROR_INVALID_CACHE,
    CLOUDLETFS_IO_ERROR_INTERRUPTED,
};

enum CLOUDLETFSStreamError {
    CLOUDLETFS_STREAM_ERROR_NONBLOCKING,
    CLOUDLETFS_STREAM_ERROR_CLOSED,
};

/* fuse */
struct cloudletfs_fuse *_cloudletfs_fuse_new(struct cloudletfs *fs, GError **err);
void _cloudletfs_fuse_run(struct cloudletfs_fuse *fuse);
void _cloudletfs_fuse_terminate(struct cloudletfs_fuse *fuse);
void _cloudletfs_fuse_free(struct cloudletfs_fuse *fuse);
struct cloudletfs_fuse_dentry *_cloudletfs_fuse_add_dir(
        struct cloudletfs_fuse_dentry *parent, const char *name);
void _cloudletfs_fuse_add_file(struct cloudletfs_fuse_dentry *parent,
        const char *name, const struct cloudletfs_fuse_ops *ops, void *ctx);
void _cloudletfs_fuse_image_populate(struct cloudletfs_fuse_dentry *dir,
        struct cloudletfs_image *img);
void _cloudletfs_fuse_stats_populate(struct cloudletfs_fuse_dentry *dir,
        struct cloudletfs_image *img);
void _cloudletfs_fuse_stream_populate(struct cloudletfs_fuse_dentry *dir,
        struct cloudletfs_image *img);
bool _cloudletfs_interrupted(void);

/* io */
bool _cloudletfs_io_init(struct cloudletfs_image *img, GList* synthesis_chunk_info, GError **err);
void _cloudletfs_io_close(struct cloudletfs_image *img);
bool _cloudletfs_io_image_is_closed(struct cloudletfs_image *img);
void _cloudletfs_io_destroy(struct cloudletfs_image *img);
uint64_t _cloudletfs_io_read_chunk(struct cloudletfs_image *img, void *data,
        uint64_t chunk, uint32_t offset, uint32_t length, GError **err);
uint64_t _cloudletfs_io_write_chunk(struct cloudletfs_image *img, const void *data,
        uint64_t chunk, uint32_t offset, uint32_t length, GError **err);

/* ll_pristine */
bool _cloudletfs_ll_pristine_init(struct cloudletfs_image *img, GList* synthesis_chunk_info, GError **err);
void _cloudletfs_ll_pristine_destroy(struct cloudletfs_image *img);
/* cloudlet */
bool _cloudlet_read_chunk(struct cloudletfs_image *img, int read_fd, void *data,
		uint64_t chunk, uint32_t offset, uint32_t length, GError **err);

/* ll_modified */
bool _cloudletfs_ll_modified_init(struct cloudletfs_image *img, GError **err);
void _cloudletfs_ll_modified_destroy(struct cloudletfs_image *img);
bool _cloudletfs_ll_modified_read_chunk(struct cloudletfs_image *img,
        void *data, uint64_t chunk, uint32_t offset, uint32_t length, GError **err);
bool _cloudletfs_ll_modified_write_chunk(struct cloudletfs_image *img,
        const void *data, uint64_t chunk, uint32_t offset, uint32_t length, GError **err);


/* bitmap */
struct bitmap_group *_cloudletfs_bit_group_new(uint64_t initial_bits);
void _cloudletfs_bit_group_free(struct bitmap_group *mgrp);
void _cloudletfs_bit_group_close(struct bitmap_group *mgrp);
struct bitmap *_cloudletfs_bit_new(struct bitmap_group *mgrp);
void _cloudletfs_bit_free(struct bitmap *map);
void _cloudletfs_bit_set(struct bitmap *map, uint64_t bit);
void _cloudletfs_bit_set_force(struct bitmap *map, uint64_t bit, bool is_force_notify);
bool _cloudletfs_bit_test(struct bitmap *map, uint64_t bit);
struct cloudletfs_stream_group *_cloudletfs_bit_get_stream_group(struct bitmap *map);

// added for on-demand bitmap update
void _cloudletfs_bit_update(struct bitmap *map, uint64_t bit);
void _cloudletfs_wait_on_bit(struct bitmap *map, uint64_t bit);

/* stream */
struct cloudletfs_stream;
typedef void (populate_stream_fn)(struct cloudletfs_stream *strm, void *data);
struct cloudletfs_stream_group *_cloudletfs_stream_group_new(
        populate_stream_fn *populate, void *populate_data);
void _cloudletfs_stream_group_close(struct cloudletfs_stream_group *sgrp);
void _cloudletfs_stream_group_free(struct cloudletfs_stream_group *sgrp);
struct cloudletfs_stream *_cloudletfs_stream_new(struct cloudletfs_stream_group *sgrp);
void _cloudletfs_stream_free(struct cloudletfs_stream *strm);
uint64_t _cloudletfs_stream_read(struct cloudletfs_stream *strm, void *buf,
        uint64_t count, bool blocking, GError **err);
void _cloudletfs_stream_write(struct cloudletfs_stream *strm, const char *fmt, ...);
void _cloudletfs_stream_group_write(struct cloudletfs_stream_group *sgrp,
        const char *fmt, ...);

/* stats */
struct cloudletfs_stat_handle;
struct cloudletfs_stat *_cloudletfs_stat_new(void);
void _cloudletfs_stat_close(struct cloudletfs_stat *stat);
bool _cloudletfs_stat_is_closed(struct cloudletfs_stat *stat);
void _cloudletfs_u64_stat_increment(struct cloudletfs_stat *stat, uint64_t val);
void _cloudletfs_stat_free(struct cloudletfs_stat *stat);
uint64_t _cloudletfs_u64_stat_get(struct cloudletfs_stat *stat);

/* cond */
struct cloudletfs_cond *_cloudletfs_cond_new(void);
void _cloudletfs_cond_free(struct cloudletfs_cond *cond);
bool _cloudletfs_cond_wait(struct cloudletfs_cond *cond, GMutex *lock);
void _cloudletfs_cond_signal(struct cloudletfs_cond *cond);
void _cloudletfs_cond_broadcast(struct cloudletfs_cond *cond);

/* utility */
GQuark _cloudletfs_config_error_quark(void);
GQuark _cloudletfs_fuse_error_quark(void);
GQuark _cloudletfs_io_error_quark(void);
GQuark _cloudletfs_stream_error_quark(void);
bool _cloudletfs_safe_pread(const char *file, int fd, void *buf, uint64_t count,
        uint64_t offset, GError **err);
bool _cloudletfs_safe_pwrite(const char *file, int fd, const void *buf,
        uint64_t count, uint64_t offset, GError **err);


#endif
