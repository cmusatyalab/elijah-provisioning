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

#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#define FUSE_USE_VERSION 26
#include <fuse.h>
#include "cloudletfs-private.h"


uid_t DEFAULT_UID = 0;
uid_t DEFAULT_GID = 0;

struct cloudletfs_fuse_dentry {
    const struct cloudletfs_fuse_ops *ops;
    GHashTable *children;
    uint32_t nlink;
    void *ctx;
};

static int dir_getattr(void *dentry_ctx G_GNUC_UNUSED, struct stat *st)
{
    st->st_mode = S_IFDIR | 0770;
    return 0;
}

static const struct cloudletfs_fuse_ops directory_ops = {
    .getattr = dir_getattr,
};

static void dentry_free(struct cloudletfs_fuse_dentry *dentry)
{
    if (dentry->children) {
        g_hash_table_destroy(dentry->children);
    }
    g_slice_free(struct cloudletfs_fuse_dentry, dentry);
}

struct cloudletfs_fuse_dentry *_cloudletfs_fuse_add_dir(
        struct cloudletfs_fuse_dentry *parent, const char *name)
{
    struct cloudletfs_fuse_dentry *dentry;

    dentry = g_slice_new0(struct cloudletfs_fuse_dentry);
    dentry->ops = &directory_ops;
    dentry->children = g_hash_table_new_full(g_str_hash, g_str_equal, g_free,
            (GDestroyNotify) dentry_free);
    dentry->nlink = 2;
    dentry->ctx = dentry;
    if (parent != NULL) {
        parent->nlink++;
        g_hash_table_insert(parent->children, g_strdup(name), dentry);
    }
    return dentry;
}

void _cloudletfs_fuse_add_file(struct cloudletfs_fuse_dentry *parent,
        const char *name, const struct cloudletfs_fuse_ops *ops, void *ctx)
{
    struct cloudletfs_fuse_dentry *dentry;

    dentry = g_slice_new0(struct cloudletfs_fuse_dentry);
    dentry->ops = ops;
    dentry->nlink = 1;
    dentry->ctx = ctx;
    g_hash_table_insert(parent->children, g_strdup(name), dentry);
}

static struct cloudletfs_fuse_dentry *lookup(struct cloudletfs_fuse *fuse,
        const char *path)
{
    struct cloudletfs_fuse_dentry *dentry = fuse->root;
    gchar **components;
    gchar **cur;

    if (g_str_equal(path, "/")) {
        return dentry;
    }

    components = g_strsplit(path, "/", 0);
    /* Drop leading slash */
    for (cur = components + 1; *cur != NULL; cur++) {
        if (dentry->children == NULL) {
            /* Not a directory */
            dentry = NULL;
            break;
        }
        dentry = g_hash_table_lookup(dentry->children, *cur);
        if (dentry == NULL) {
            /* No such dentry */
            break;
        }
    }
    g_strfreev(components);
    return dentry;
}

static int do_getattr(const char *path, struct stat *st)
{
    struct cloudletfs_fuse *fuse = fuse_get_context()->private_data;
    struct cloudletfs_fuse_dentry *dentry;
    int ret;

    dentry = lookup(fuse, path);
    if (dentry == NULL) {
        return -ENOENT;
    }
    if (dentry->ops->getattr == NULL) {
        return -ENOSYS;
    }

    st->st_nlink = dentry->nlink;
    st->st_uid = DEFAULT_UID;
    st->st_gid = DEFAULT_GID;
    st->st_size = 0;
    st->st_atime = st->st_mtime = st->st_ctime = time(NULL);

    ret = dentry->ops->getattr(dentry->ctx, st);
    st->st_blocks = (st->st_size + 511) / 512;
    return ret;
}

static int do_open(const char *path, struct fuse_file_info *fi)
{
    struct cloudletfs_fuse *fuse = fuse_get_context()->private_data;
    struct cloudletfs_fuse_dentry *dentry;
    struct cloudletfs_fuse_fh *fh;
    int ret;

    dentry = lookup(fuse, path);
    if (dentry == NULL) {
        return -ENOENT;
    }
    if (dentry->ops->open == NULL) {
        return -ENOSYS;
    }

    fh = g_slice_new0(struct cloudletfs_fuse_fh);
    fh->ops = dentry->ops;
    fh->blocking = !(fi->flags & O_NONBLOCK);
    ret = dentry->ops->open(dentry->ctx, fh);
    if (ret) {
        g_slice_free(struct cloudletfs_fuse_fh, fh);
    } else {
        fi->fh = (uintptr_t) fh;
        fi->nonseekable = fh->ops->nonseekable;
    }
    return ret;
}

static int do_read(const char *path G_GNUC_UNUSED, char *buf, size_t count,
        off_t start, struct fuse_file_info *fi)
{
    struct cloudletfs_fuse_fh *fh = (void *) (uintptr_t) fi->fh;

    if (fh->ops->read) {
        return fh->ops->read(fh, buf, start, count);
    } else {
        return -ENOSYS;
    }
}

static int do_write(const char *path G_GNUC_UNUSED, const char *buf,
        size_t count, off_t start, struct fuse_file_info *fi)
{
    struct cloudletfs_fuse_fh *fh = (void *) (uintptr_t) fi->fh;

    if (fh->ops->write) {
        return fh->ops->write(fh, buf, start, count);
    } else {
        return -ENOSYS;
    }
}

static int do_release(const char *path G_GNUC_UNUSED,
        struct fuse_file_info *fi)
{
    struct cloudletfs_fuse_fh *fh = (void *) (uintptr_t) fi->fh;

    if (fh->ops->release) {
        fh->ops->release(fh);
    }
    g_slice_free(struct cloudletfs_fuse_fh, fh);
    return 0;
}

static int do_opendir(const char *path, struct fuse_file_info *fi)
{
    struct cloudletfs_fuse *fuse = fuse_get_context()->private_data;
    struct cloudletfs_fuse_dentry *dentry;

    dentry = lookup(fuse, path);
    if (dentry == NULL) {
        return -ENOENT;
    }
    if (dentry->children == NULL) {
        return -ENOTDIR;
    }
    fi->fh = (uintptr_t) dentry;
    return 0;
}

static int do_chown(const char *path G_GNUC_UNUSED, uid_t owner, gid_t group)
{
	DEFAULT_UID = owner;
	DEFAULT_GID = group;
	return 0;
}

struct fill_data {
    fuse_fill_dir_t filler;
    void *buf;
    int failed;
};

static void collect_names(char *name,
        struct cloudletfs_fuse_dentry *dentry G_GNUC_UNUSED,
        struct fill_data *fill)
{
    if (!fill->failed) {
        fill->failed = fill->filler(fill->buf, name, NULL, 0);
    }
}

static int do_readdir(const char *path G_GNUC_UNUSED, void *buf,
        fuse_fill_dir_t filler, off_t off G_GNUC_UNUSED,
        struct fuse_file_info *fi)
{
    struct cloudletfs_fuse_dentry *dentry = (void *) (uintptr_t) fi->fh;
    struct fill_data fill = {
        .filler = filler,
        .buf = buf,
        .failed = 0,
    };

    g_assert(dentry->children != NULL);

    fill.failed = filler(buf, ".", NULL, 0);
    if (!fill.failed) {
        fill.failed = filler(buf, "..", NULL, 0);
    }
    g_hash_table_foreach(dentry->children, (GHFunc) collect_names, &fill);
    return fill.failed ? -EIO : 0;
}

static int do_statfs(const char *path G_GNUC_UNUSED, struct statvfs *st)
{
    struct cloudletfs_fuse *fuse = fuse_get_context()->private_data;
    uint64_t image_size;

    image_size = fuse->fs->disk->image_size;
    if (fuse->fs->memory != NULL) {
        image_size += fuse->fs->memory->image_size;
    }

    st->f_bsize = 512;
    st->f_blocks = image_size / 512;
    st->f_bfree = st->f_bavail = 0;
    st->f_namemax = 256;
    return 0;
}

static const struct fuse_operations fuse_ops = {
    .getattr = do_getattr,
    .open = do_open,
    .read = do_read,
    .write = do_write,
    .release = do_release,
    .opendir = do_opendir,
    .readdir = do_readdir,
    .statfs = do_statfs,
    .flag_nullpath_ok = 1,
    .chown = do_chown,
};

static void add_image(struct cloudletfs_fuse_dentry *parent,
        struct cloudletfs_image *img, const char *name)
{
    struct cloudletfs_fuse_dentry *dir;

    dir = _cloudletfs_fuse_add_dir(parent, name);
    _cloudletfs_fuse_image_populate(dir, img);
    _cloudletfs_fuse_stats_populate(dir, img);
    _cloudletfs_fuse_stream_populate(dir, img);
}

struct cloudletfs_fuse *_cloudletfs_fuse_new(struct cloudletfs *fs, GError **err)
{
    struct cloudletfs_fuse *fuse;
    GPtrArray *argv;
    struct fuse_args args;

    /* Set up data structures */
    fuse = g_slice_new0(struct cloudletfs_fuse);
    fuse->fs = fs;
    fuse->root = _cloudletfs_fuse_add_dir(NULL, NULL);
    add_image(fuse->root, fs->disk, "disk");
    if (fs->memory != NULL) {
        add_image(fuse->root, fs->memory, "memory");
    }
    DEFAULT_UID = getuid();
    DEFAULT_GID = getgid();

    /* Construct mountpoint */
    fuse->mountpoint = g_strdup("/var/tmp/cloudletfs-XXXXXX");
    if (mkdtemp(fuse->mountpoint) == NULL) {
        g_set_error(err, CLOUDLETFS_FUSE_ERROR,
                CLOUDLETFS_FUSE_ERROR_BAD_MOUNTPOINT,
                "Could not create mountpoint: %s", strerror(errno));
        goto bad_dealloc;
    }

    /* Build FUSE command line */
    argv = g_ptr_array_new();
    g_ptr_array_add(argv, g_strdup("-odefault_permissions"));
	//g_ptr_array_add(argv, g_strdup("allow_root"));
	g_ptr_array_add(argv, g_strdup("-oallow_other"));
    g_ptr_array_add(argv, g_strdup_printf("-ofsname=cloudletfs#%d", getpid()));
    g_ptr_array_add(argv, g_strdup("-osubtype=cloudletfs"));
    g_ptr_array_add(argv, g_strdup("-obig_writes"));
    g_ptr_array_add(argv, g_strdup("-ointr"));
    /* Avoid kernel page cache in order to preserve semantics of read()
       and write() return values. */
    g_ptr_array_add(argv, g_strdup("-odirect_io"));
    g_ptr_array_add(argv, NULL);
    args.argv = (gchar **) g_ptr_array_free(argv, FALSE);
    args.argc = g_strv_length(args.argv);
    args.allocated = 0;

    /* Initialize FUSE */
    fuse->chan = fuse_mount(fuse->mountpoint, &args);
    if (fuse->chan == NULL) {
        g_set_error(err, CLOUDLETFS_FUSE_ERROR, CLOUDLETFS_FUSE_ERROR_FAILED,
                "Couldn't mount FUSE filesystem");
        g_strfreev(args.argv);
        goto bad_rmdir;
    }
    fuse->fuse = fuse_new(fuse->chan, &args, &fuse_ops, sizeof(fuse_ops),
            fuse);
    g_strfreev(args.argv);
    if (fuse->fuse == NULL) {
        g_set_error(err, CLOUDLETFS_FUSE_ERROR, CLOUDLETFS_FUSE_ERROR_FAILED,
                "Couldn't create FUSE filesystem");
        goto bad_unmount;
    }

    return fuse;

bad_unmount:
    fuse_unmount(fuse->mountpoint, fuse->chan);
bad_rmdir:
    rmdir(fuse->mountpoint);
bad_dealloc:
    g_free(fuse->mountpoint);
    dentry_free(fuse->root);
    g_slice_free(struct cloudletfs_fuse, fuse);
    return NULL;
}

void _cloudletfs_fuse_run(struct cloudletfs_fuse *fuse)
{
    fuse_loop_mt(fuse->fuse);
}

void _cloudletfs_fuse_terminate(struct cloudletfs_fuse *fuse)
{
    char *argv[] = {"fusermount", "-uqz", "--", fuse->mountpoint, NULL};

    /* swallow errors */
    g_spawn_sync("/", argv, NULL, G_SPAWN_SEARCH_PATH, NULL, NULL, NULL,
            NULL, NULL, NULL);
}

void _cloudletfs_fuse_free(struct cloudletfs_fuse *fuse)
{
    if (fuse == NULL) {
        return;
    }
    /* Normally the filesystem will already have been unmounted.  Try
       to make sure. */
    fuse_unmount(fuse->mountpoint, fuse->chan);
    fuse_destroy(fuse->fuse);
    rmdir(fuse->mountpoint);
    g_free(fuse->mountpoint);
    dentry_free(fuse->root);
    g_slice_free(struct cloudletfs_fuse, fuse);
}

/* Return true if the current FUSE request was interrupted. */
bool _cloudletfs_interrupted(void)
{
    return fuse_interrupted();
}
