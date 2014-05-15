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
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include "cloudletfs-private.h"

#define IMAGE_ARG_COUNT 5

#define CLOUDLET_IO
#ifdef CLOUDLET_IO
#define CPRINTF(fmt, ...) \
    do { fprintf(stdout, "[FUSE][cloudletfs] " fmt, ## __VA_ARGS__); fflush(stdout);} while (0)
#else
#define CPRINTF(fmt, ...) \
    do { } while (0)
#endif


static void _image_free(struct cloudletfs_image *img)
{
    _cloudletfs_stream_group_free(img->io_stream);
    _cloudletfs_stat_free(img->bytes_read);
    _cloudletfs_stat_free(img->bytes_written);
    _cloudletfs_stat_free(img->chunk_dirties);
    g_free(img->type);
    g_slice_free(struct cloudletfs_image, img);
}

static void image_free(struct cloudletfs_image *img)
{
    if (img == NULL) {
        return;
    }
    _cloudletfs_io_destroy(img);
    _image_free(img);
}

static uint64_t parse_uint(const char *str, GError **err)
{
    char *endptr;
    uint64_t ret;

    ret = g_ascii_strtoull(str, &endptr, 10);
    if (*str == 0 || *endptr != 0) {
        g_set_error(err, CLOUDLETFS_CONFIG_ERROR,
                CLOUDLETFS_CONFIG_ERROR_INVALID_ARGUMENT,
                "Invalid integer argument: %s", str);
        return 0;
    }
    return ret;
}

static char *read_line(GIOChannel *chan, GError **err)
{
    char *buf;
    gsize terminator;

    switch (g_io_channel_read_line(chan, &buf, NULL, &terminator, err)) {
    case G_IO_STATUS_ERROR:
        return NULL;
    case G_IO_STATUS_NORMAL:
        buf[terminator] = 0;
        return buf;
    case G_IO_STATUS_EOF:
        g_set_error(err, G_IO_CHANNEL_ERROR, G_IO_CHANNEL_ERROR_IO,
                "Unexpected EOF");
        return NULL;
    case G_IO_STATUS_AGAIN:
        g_set_error(err, G_IO_CHANNEL_ERROR, G_IO_CHANNEL_ERROR_IO,
                "Unexpected EAGAIN");
        return NULL;
    default:
        g_assert_not_reached();
    }
}

static char **get_arguments(GIOChannel *chan, GError **err)
{
    uint64_t n;
    uint64_t count;
    char *str;
    GPtrArray *args;
    GError *my_err = NULL;

    /* Get argument count */
    str = read_line(chan, err);
    if (str == NULL) {
        return NULL;
    }
    count = parse_uint(str, &my_err);
    if (my_err) {
        g_propagate_error(err, my_err);
        return NULL;
    }

    /* Get arguments */
    args = g_ptr_array_new_with_free_func(g_free);
    for (n = 0; n < count; n++) {
        str = read_line(chan, err);
        if (str == NULL) {
            g_ptr_array_free(args, TRUE);
            return NULL;
        }
        g_ptr_array_add(args, str);
    }
    g_ptr_array_add(args, NULL);
    return (char **) g_ptr_array_free(args, FALSE);
}


static GList *synthesis_chunk_list_new(const gchar *overlay_chunks,
        GError **err) {
    GList *chunk_list = NULL;
    uint64_t chunk_number = 0;
    long valid_bit = 0;
    gchar **components;
    gchar **overlay_info;
    gchar **cur;
    gchar *end;

    components = g_strsplit(overlay_chunks, ",", 0);
    for (cur = components; *cur != NULL; cur++) {
        overlay_info = g_strsplit(*cur, ":", 0);
        if (g_strv_length(overlay_info) != 2) {
            g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(errno),
                    "Invalid overlay format %s", *cur);
            g_strfreev(overlay_info);
            g_strfreev(components);
            return NULL;
        }
        chunk_number = g_ascii_strtoull(overlay_info[0], &end, 10);
        if (overlay_info[0][0] == 0 || *end != 0) {
            g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(errno),
                    "Invalid overlay format at chunk number %s",
                    overlay_info[0]);
            g_strfreev(overlay_info);
            g_strfreev(components);
            return NULL;
        }
        valid_bit = strtol(overlay_info[1], &end, 10);
        if (overlay_info[1][0] == 0 || *end != 0) {
            g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(errno),
                    "Invalid overlay format at valid bit %s", overlay_info[1]);
            g_strfreev(overlay_info);
            g_strfreev(components);
            return NULL;
        }

        struct chunk_synthesis_info *chunk =
                g_slice_new0(struct chunk_synthesis_info);
        chunk->chunk_number = chunk_number;
        chunk->valid = (valid_bit == 1) ? true : false;
        chunk_list = g_list_append(chunk_list, chunk);
        g_strfreev(overlay_info);
    }
    g_strfreev(components);
    return chunk_list;
}


static void synthesis_chunk_unref(struct chunk_synthesis_info *info)
{
    g_slice_free(struct chunk_synthesis_info, info);
}

static void synthesis_chunk_list_free(GList *synthesis_chunk_info)
{
    g_list_free_full(synthesis_chunk_info, (GDestroyNotify) synthesis_chunk_unref);
}

static struct cloudletfs_image *image_new(char **argv, const char *type, GError **err)
{
    struct cloudletfs_image *img;
    int arg = 0;
    GError *my_err = NULL;

    const char *base_path = argv[arg++];
    const char *overlay_path = argv[arg++];
    const char *overlay_info = argv[arg++];
    const uint64_t size = parse_uint(argv[arg++], &my_err);
    if (my_err) {
        g_propagate_error(err, my_err);
        return NULL;
    }
    const uint32_t chunk_size = parse_uint(argv[arg++], &my_err);
    if (my_err) {
        g_propagate_error(err, my_err);
        return NULL;
    }

    const int base_fd = open(base_path, O_RDONLY);
    if (base_fd < 0){
        g_set_error(err, CLOUDLETFS_CONFIG_ERROR,
                CLOUDLETFS_CONFIG_ERROR_INVALID_ARGUMENT,
                "Invalid path for base image: %s", base_path);
        return NULL;
    }
    int overlay_fd = -1;
    if (strlen(overlay_path) != 0){
        overlay_fd = open(overlay_path, O_RDONLY);
        if (overlay_fd < 0){
                g_set_error(err, CLOUDLETFS_CONFIG_ERROR,
				CLOUDLETFS_CONFIG_ERROR_INVALID_ARGUMENT,
				"Invalid path for overlay image: %s", overlay_path);
		return NULL;
        }
    }
    GList *synthesis_chunk_info = synthesis_chunk_list_new(overlay_info, &my_err);
    if (my_err) {
	g_propagate_error(err, my_err);
	return NULL;
    }

    img = g_slice_new0(struct cloudletfs_image);
    img->type = strdup(type);
    img->image_size = size;
    img->chunk_size = chunk_size;
    img->base_fd = base_fd;
    img->overlay_fd = overlay_fd;
    img->io_stream = _cloudletfs_stream_group_new(NULL, NULL);
    img->bytes_read = _cloudletfs_stat_new();
    img->bytes_written = _cloudletfs_stat_new();
    img->chunk_dirties = _cloudletfs_stat_new();

    if (!_cloudletfs_io_init(img, synthesis_chunk_info, err)) {
        synthesis_chunk_list_free(synthesis_chunk_info);
        _image_free(img);
        return NULL;
    }

    synthesis_chunk_list_free(synthesis_chunk_info);
    return img;
}

static void image_close(struct cloudletfs_image *img)
{
    _cloudletfs_io_close(img);
    _cloudletfs_stat_close(img->bytes_read);
    _cloudletfs_stat_close(img->bytes_written);
    _cloudletfs_stat_close(img->chunk_dirties);
    _cloudletfs_stream_group_close(img->io_stream);
}

static void *glib_loop_thread(void *data)
{
    struct cloudletfs *fs = data;

    fs->glib_loop = g_main_loop_new(NULL, TRUE);
    g_main_loop_run(fs->glib_loop);
    g_main_loop_unref(fs->glib_loop);
    fs->glib_loop = NULL;
    return NULL;
}

static bool handle_stdin(struct cloudletfs *fs, const char *oneline, GError **err) {
    // valid format : (image_index: chunk_number),
    // ex) 1:10251,1:10252, ..

    // check end signal
    if (strcmp(oneline, "END_OF_TRANSMISSION") == 0) {
        fprintf(stdout, "[FUSE] Receive END_OF_TRANSMISSION\n");
        fflush(stdout);
        return true;
    }

    struct cloudletfs_image *img;
    uint64_t image_index = -1;
    guint64 chunk_number = -1;

    gchar **components;
    gchar **cur;
    gchar *end;
    components = g_strsplit(oneline, ",", 0);
    if (!components) {
        return false;
    }

    for (cur = components; *cur != NULL; cur++) {
        gchar **overlay_info = g_strsplit(*cur, ":", 0);
        if ((overlay_info[0] == NULL) || (overlay_info[1] == NULL)) {
            g_strfreev(overlay_info);
            g_strfreev(components);
            return false;
        }

        // 1 for disk, 2 for memory
        image_index = g_ascii_strtoull(overlay_info[0], NULL, 10);
        if (image_index < 1 || image_index > 2) {
            g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(errno),
                    "Invalid overlay format at image index %s",overlay_info[0]);
            g_strfreev(overlay_info);
            g_strfreev(components);
            return false;
        }
        // If the string conversion fails, zero is returned, 
        // and endptr(end) returns nptr(overlay_info[1]).
        chunk_number = g_ascii_strtoull(overlay_info[1], &end, 10);
        if (overlay_info[1] == end) {
            g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(errno),
                    "Invalid overlay format at chunk number %s", overlay_info[1]);
            g_strfreev(overlay_info);
            g_strfreev(components);
            return false;
        }

        if (image_index == 1) {
            img = fs->disk;
        } else if (image_index == 2) {
            img = fs->memory;
        } else {
            g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(errno),
                    "Invalid index number %ld", image_index);
            g_strfreev(overlay_info);
            return false;
        }
        _cloudletfs_bit_update(img->current_overlay_map, chunk_number);
        g_strfreev(overlay_info);
    }
    g_strfreev(components);
    return true;
}

static gboolean read_stdin(GIOChannel *source, GIOCondition cond G_GNUC_UNUSED,
        void *data) {
    struct cloudletfs *fs = data;
    char *buf;
    gsize terminator;
    bool success_stdin;
    GError *err = NULL;

    /* See if stdin has been closed. */
    while (1) {
        switch (g_io_channel_read_line(source, &buf, NULL, &terminator, &err)) {
        case G_IO_STATUS_ERROR:
            return TRUE;
        case G_IO_STATUS_NORMAL:
            buf[terminator] = 0;
            break;
        case G_IO_STATUS_EOF:
            goto out;
        case G_IO_STATUS_AGAIN:
            return TRUE;
        default:
            g_assert_not_reached();
            break;
        }

        success_stdin = handle_stdin(fs, buf, &err);
        g_free(buf);
        if (!success_stdin) {
            CPRINTF("FUSE TERMINATED: Invalid stdin format\n");
            goto out;
        }
    }
out:
    /* Stop allowing blocking reads on streams (to prevent unmount from
     blocking forever) and lazy-unmount the filesystem.  For complete
     correctness, this should disallow new image opens, wait for existing
     image fds to close, disallow new stream opens and blocking reads,
     then lazy unmount. */
    image_close(fs->disk);
    if (fs->memory != NULL) {
        image_close(fs->memory);
    }
    _cloudletfs_fuse_terminate(fs->fuse);
    return FALSE;
}

static gboolean shutdown_callback(void *data)
{
    struct cloudletfs *fs = data;

    g_main_loop_quit(fs->glib_loop);
    return FALSE;
}

int main(int argc G_GNUC_UNUSED, char **argv G_GNUC_UNUSED)
{
    fflush(stdout);
    struct cloudletfs *fs;
    GThread *loop_thread = NULL;
    GIOChannel *chan;
    GIOFlags flags;
    int argc_stdin;
    char **argv_stdin;
    int arg = 0;
    int images;
    GError *err = NULL;
    int ret = 1;

    /* Initialize */
    if (!g_thread_supported()) {
        g_thread_init(NULL);
    }

    /* Read arguments */
    chan = g_io_channel_unix_new(0);
    argv_stdin = get_arguments(chan, &err);


    if (argv_stdin == NULL) {
        printf("%s\n", err->message);
        g_clear_error(&err);
        return 1;
    }

    /* Check argc_stdin */
    argc_stdin = g_strv_length(argv_stdin);
    images = (argc_stdin) / IMAGE_ARG_COUNT;
    if (images < 1 || images > 2) {
        printf("Incorrect argument count. # of argument is %d\n", argc_stdin);
        return 1;
    }

    /* Set up disk */
    fs = g_slice_new0(struct cloudletfs);
    fs->disk = image_new(argv_stdin, "disk", &err);
    if (err) {
        printf("%s\n", err->message);
        goto out;
    }
    arg += IMAGE_ARG_COUNT;

    /* Set up memory */
    if (images > 1) {
        fs->memory = image_new(argv_stdin + arg, "memory", &err);
        if (err) {
            printf("%s\n", err->message);
            goto out;
        }
        arg += IMAGE_ARG_COUNT;
    }

    /* Set up fuse */
    fs->fuse = _cloudletfs_fuse_new(fs, &err);
    if (err) {
        printf("%s\n", err->message);
        goto out;
    }

    /* Start main loop thread */
    loop_thread = g_thread_create(glib_loop_thread, fs, TRUE, &err);
    if (err) {
        printf("%s\n", err->message);
        goto out;
    }

    /* Add watch for stdin being closed */
    flags = g_io_channel_get_flags(chan);
    g_io_channel_set_flags(chan, flags | G_IO_FLAG_NONBLOCK, &err);
    if (err) {
        printf("%s\n", err->message);
        g_io_channel_unref(chan);
        goto out;
    }
    g_io_add_watch(chan, G_IO_IN | G_IO_ERR | G_IO_HUP | G_IO_NVAL, read_stdin, fs);

    /* Started successfully.  Send the mountpoint back to the parent and
       run FUSE event loop until the filesystem is unmounted. */
    printf("%s\n", fs->fuse->mountpoint);
    fflush(stdout);
    _cloudletfs_fuse_run(fs->fuse);
    ret = 0;

out:
    /* Shut down */
    if (err != NULL) {
        g_clear_error(&err);
    }
    if (loop_thread != NULL) {
        g_idle_add(shutdown_callback, fs);
        g_thread_join(loop_thread);
    }
    _cloudletfs_fuse_free(fs->fuse);
    image_free(fs->disk);
    image_free(fs->memory);
    g_slice_free(struct cloudletfs, fs);
    g_strfreev(argv_stdin);
    g_io_channel_unref(chan);
    return ret;
}

