#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

#include "erl_nif_compat.h"
#include "erl_nif.h"

static ErlNifResourceType* blcs_log_file_RESOURCE;

typedef struct
{
    int fd;
} blcs_log_file_handle;

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_ALLOCATION_ERROR;
static ERL_NIF_TERM ATOM_ALREADY_EXISTS;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_FTRUNCATE_ERROR;
static ERL_NIF_TERM ATOM_GETFL_ERROR;
static ERL_NIF_TERM ATOM_ILT_CREATE_ERROR; /* Iteration lock thread creation error */
static ERL_NIF_TERM ATOM_ITERATION_IN_PROCESS;
static ERL_NIF_TERM ATOM_ITERATION_NOT_PERMITTED;
static ERL_NIF_TERM ATOM_ITERATION_NOT_STARTED;
static ERL_NIF_TERM ATOM_LOCK_NOT_WRITABLE;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_NOT_READY;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_OUT_OF_DATE;
static ERL_NIF_TERM ATOM_PREAD_ERROR;
static ERL_NIF_TERM ATOM_PWRITE_ERROR;
static ERL_NIF_TERM ATOM_READY;
static ERL_NIF_TERM ATOM_SETFL_ERROR;
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_UNDEFINED;
static ERL_NIF_TERM ATOM_EOF;
static ERL_NIF_TERM ATOM_CREATE;
static ERL_NIF_TERM ATOM_READONLY;
static ERL_NIF_TERM ATOM_O_SYNC;
// lseek equivalents for file_position
static ERL_NIF_TERM ATOM_CUR;
static ERL_NIF_TERM ATOM_BOF;

static void blcs_log_nifs_file_resource_cleanup(ErlNifEnv* env, void* arg);

ERL_NIF_TERM errno_atom(ErlNifEnv* env, int error);
ERL_NIF_TERM errno_error_tuple(ErlNifEnv* env, ERL_NIF_TERM key, int error);

ERL_NIF_TERM blcs_log_nifs_file_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM blcs_log_nifs_file_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM blcs_log_nifs_file_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM blcs_log_nifs_file_pread(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM blcs_log_nifs_file_pwrite(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM blcs_log_nifs_file_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM blcs_log_nifs_file_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM blcs_log_nifs_file_position(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM blcs_log_nifs_file_seekbof(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM blcs_log_nifs_file_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static void blcs_log_nifs_file_resource_cleanup(ErlNifEnv* env, void* arg);

static ErlNifFunc nif_funcs[] =
{
    {"file_open_int",   2, blcs_log_nifs_file_open},
    {"file_close_int",  1, blcs_log_nifs_file_close},
    {"file_sync_int",   1, blcs_log_nifs_file_sync},
    {"file_pread_int",  3, blcs_log_nifs_file_pread},
    {"file_pwrite_int", 3, blcs_log_nifs_file_pwrite},
    {"file_read_int",   2, blcs_log_nifs_file_read},
    {"file_write_int",  2, blcs_log_nifs_file_write},
    {"file_position_int",  2, blcs_log_nifs_file_position},
    {"file_seekbof_int", 1,   blcs_log_nifs_file_seekbof},
    {"file_truncate_int", 1,  blcs_log_nifs_file_truncate}
};


static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    blcs_log_file_RESOURCE = enif_open_resource_type_compat(env, "blcs_log_file_resource",
                                                            &blcs_log_nifs_file_resource_cleanup,
                                                            ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER,
                                                            0);  

    // Initialize atoms that we use throughout the NIF.
    ATOM_ALLOCATION_ERROR = enif_make_atom(env, "allocation_error");
    ATOM_ALREADY_EXISTS = enif_make_atom(env, "already_exists");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_FTRUNCATE_ERROR = enif_make_atom(env, "ftruncate_error");
    ATOM_GETFL_ERROR = enif_make_atom(env, "getfl_error");
    ATOM_ILT_CREATE_ERROR = enif_make_atom(env, "ilt_create_error");
    ATOM_ITERATION_IN_PROCESS = enif_make_atom(env, "iteration_in_process");
    ATOM_ITERATION_NOT_PERMITTED = enif_make_atom(env, "iteration_not_permitted");
    ATOM_ITERATION_NOT_STARTED = enif_make_atom(env, "iteration_not_started");
    ATOM_LOCK_NOT_WRITABLE = enif_make_atom(env, "lock_not_writable");
    ATOM_NOT_FOUND = enif_make_atom(env, "not_found");
    ATOM_NOT_READY = enif_make_atom(env, "not_ready");
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_OUT_OF_DATE = enif_make_atom(env, "out_of_date");
    ATOM_PREAD_ERROR = enif_make_atom(env, "pread_error");
    ATOM_PWRITE_ERROR = enif_make_atom(env, "pwrite_error");
    ATOM_READY = enif_make_atom(env, "ready");
    ATOM_SETFL_ERROR = enif_make_atom(env, "setfl_error");
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_UNDEFINED = enif_make_atom(env, "undefined");
    ATOM_EOF = enif_make_atom(env, "eof");
    ATOM_CREATE = enif_make_atom(env, "create");
    ATOM_READONLY = enif_make_atom(env, "readonly");
    ATOM_O_SYNC = enif_make_atom(env, "o_sync");
    ATOM_CUR = enif_make_atom(env, "cur");
    ATOM_BOF = enif_make_atom(env, "bof");

    return 0;
}

static void blcs_log_nifs_file_resource_cleanup(ErlNifEnv* env, void* arg) 
{
    blcs_log_file_handle* handle = (blcs_log_file_handle*)arg;
    if (handle->fd > -1)
    {    
        close(handle->fd);
    }    
}

int get_file_open_flags(ErlNifEnv* env, ERL_NIF_TERM list)
{
    int flags = O_RDWR | O_APPEND;
    ERL_NIF_TERM head, tail;
    while (enif_get_list_cell(env, list, &head, &tail))
    {    
        if (head == ATOM_CREATE)
        {    
            //tianwanli01
            flags = O_CREAT | O_RDWR | O_APPEND;
            //flags = O_CREAT | O_EXCL | O_RDWR | O_APPEND;
        }    
        else if (head == ATOM_READONLY)
        {    
            flags = O_RDONLY;
        }    
        else if (head == ATOM_O_SYNC)
        {    
            flags |= O_SYNC;
        }    

        list = tail;
    }    
    return flags;
}

ERL_NIF_TERM blcs_log_nifs_file_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{   
    char filename[4096];
    if (enif_get_string(env, argv[0], filename, sizeof(filename), ERL_NIF_LATIN1) &&
        enif_is_list(env, argv[1]))
    {   
        int flags = get_file_open_flags(env, argv[1]);
        int fd = open(filename, flags, S_IREAD | S_IWRITE);
        if (fd > -1)
        {   
            // Setup a resource for our handle
            blcs_log_file_handle* handle = enif_alloc_resource_compat(env,
                                                                    blcs_log_file_RESOURCE,
                                                                    sizeof(blcs_log_file_handle));
            memset(handle, '\0', sizeof(blcs_log_file_handle));
            handle->fd = fd;

            ERL_NIF_TERM result = enif_make_resource(env, handle);
            enif_release_resource_compat(env, handle);
            return enif_make_tuple2(env, ATOM_OK, result);
        }
        else
        {
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM blcs_log_nifs_file_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    blcs_log_file_handle* handle;
    if (enif_get_resource(env, argv[0], blcs_log_file_RESOURCE, (void**)&handle))
    {    
        if (handle->fd > 0) 
        {    
            /* TODO: Check for EIO */
            close(handle->fd);
            handle->fd = -1;
        }    
        return ATOM_OK;
    }    
    else 
    {    
        return enif_make_badarg(env);
    }    
}

ERL_NIF_TERM blcs_log_nifs_file_sync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    blcs_log_file_handle* handle;
    if (enif_get_resource(env, argv[0], blcs_log_file_RESOURCE, (void**)&handle))
    {    
        int rc = fsync(handle->fd);
        if (rc != -1)
        {    
            return ATOM_OK;
        }    
        else 
        {    
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }    
    }    
    else 
    {    
        return enif_make_badarg(env);
    }    
}

ERL_NIF_TERM blcs_log_nifs_file_pread(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{   
    blcs_log_file_handle* handle;
    unsigned long offset_ul;
    unsigned long count_ul;
    if (enif_get_resource(env, argv[0], blcs_log_file_RESOURCE, (void**)&handle) &&
        enif_get_ulong(env, argv[1], &offset_ul) && /* Offset */
        enif_get_ulong(env, argv[2], &count_ul))    /* Count */
    {   
        ErlNifBinary bin;
        off_t offset = offset_ul;
        size_t count = count_ul;
        if (!enif_alloc_binary(count, &bin))
        {   
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR);
        }

        ssize_t bytes_read = pread(handle->fd, bin.data, count, offset);
        if (bytes_read == count)
        {   
            /* Good read; return {ok, Bin} */
            return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin));
        }
        else if (bytes_read > 0)
        {   
            /* Partial read; need to resize our binary (bleh) and return {ok, Bin} */
            if (enif_realloc_binary(&bin, bytes_read))
            {
                return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin));
            }
            else
            {
                /* Realloc failed; cleanup and bail */
                enif_release_binary(&bin);
                return enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR);
            }
        }
        else if (bytes_read == 0)
        {
            /* EOF */
            enif_release_binary(&bin);
            return ATOM_EOF;
        }
        else
        {
            /* Read failed altogether */
            enif_release_binary(&bin);
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM blcs_log_nifs_file_pwrite(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{   
    blcs_log_file_handle* handle;
    unsigned long offset_ul;
    ErlNifBinary bin;

    if (enif_get_resource(env, argv[0], blcs_log_file_RESOURCE, (void**)&handle) &&
        enif_get_ulong(env, argv[1], &offset_ul) && /* Offset */
        enif_inspect_iolist_as_binary(env, argv[2], &bin)) /* Bytes to write */
    {
        unsigned char* buf = bin.data;
        ssize_t bytes_written = 0;
        ssize_t count = bin.size;
        off_t offset = offset_ul;

        while (count > 0)
        {   
            bytes_written = pwrite(handle->fd, buf, count, offset);
            if (bytes_written > 0)
            {
                count -= bytes_written;
                offset += bytes_written;
                buf += bytes_written;
            }
            else
            {
                /* Write failed altogether */
                return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
            }
        }

        /* Write done */
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM blcs_log_nifs_file_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{   
    blcs_log_file_handle* handle;
    size_t count;

    if (enif_get_resource(env, argv[0], blcs_log_file_RESOURCE, (void**)&handle) &&
        enif_get_ulong(env, argv[1], &count))    /* Count */
    {
        ErlNifBinary bin;
        if (!enif_alloc_binary(count, &bin))
        {
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR);
        }

        ssize_t bytes_read = read(handle->fd, bin.data, count);
        if (bytes_read == count)
        {
            /* Good read; return {ok, Bin} */
            return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin));
        }
        else if (bytes_read > 0)
        {   
            /* Partial read; need to resize our binary (bleh) and return {ok, Bin} */
            if (enif_realloc_binary(&bin, bytes_read))
            {
                return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &bin));
            }
            else
            {
                /* Realloc failed; cleanup and bail */
                enif_release_binary(&bin);
                return enif_make_tuple2(env, ATOM_ERROR, ATOM_ALLOCATION_ERROR);
            }
        }
        else if (bytes_read == 0)
        {
            /* EOF */
            enif_release_binary(&bin);
            return ATOM_EOF;
        }
        else
        {
            /* Read failed altogether */
            enif_release_binary(&bin);
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM blcs_log_nifs_file_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    blcs_log_file_handle* handle;
    ErlNifBinary bin;

    if (enif_get_resource(env, argv[0], blcs_log_file_RESOURCE, (void**)&handle) &&
        enif_inspect_iolist_as_binary(env, argv[1], &bin)) /* Bytes to write */
    {
        unsigned char* buf = bin.data;
        ssize_t bytes_written = 0;
        ssize_t count = bin.size;
        while (count > 0)
        {   
            bytes_written = write(handle->fd, buf, count);
            if (bytes_written > 0)
            {
                count -= bytes_written;
                buf += bytes_written;
            }
            else
            {
                /* Write failed altogether */
                return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
            }
        }

        /* Write done */
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

// Returns 0 if failed to parse lseek style argument for file_position
static int parse_seek_offset(ErlNifEnv* env, ERL_NIF_TERM arg, off_t * ofs, int * whence)
{   
    long long_ofs;
    int arity;
    const ERL_NIF_TERM* tuple_elements;
    if (enif_get_long(env, arg, &long_ofs))
    {   
        *whence = SEEK_SET;
        *ofs = (off_t)long_ofs;
        return 1;
    }
    else if (enif_get_tuple(env, arg, &arity, &tuple_elements) && arity == 2
            && enif_get_long(env, tuple_elements[1], &long_ofs))
    {
        *ofs = (off_t)long_ofs;
        if (tuple_elements[0] == ATOM_CUR)
        {
            *whence = SEEK_CUR;
        }
        else if (tuple_elements[0] == ATOM_BOF)
        {
            *whence = SEEK_SET;
        }
        else if (tuple_elements[0] == ATOM_EOF)
        {
            *whence = SEEK_END;
        }
        else
        {
            return 0;
        }
        return 1;
    }
    else
    {
        return 0;
    }
}

ERL_NIF_TERM blcs_log_nifs_file_position(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{   
    blcs_log_file_handle* handle;
    off_t offset;
    int whence;

    if (enif_get_resource(env, argv[0], blcs_log_file_RESOURCE, (void**)&handle) &&
        parse_seek_offset(env, argv[1], &offset, &whence))
    {

        off_t new_offset = lseek(handle->fd, offset, whence);
        if (new_offset != -1)
        {
            return enif_make_tuple2(env, ATOM_OK, enif_make_ulong(env, new_offset));
        }
        else
        {
            /* Write failed altogether */
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM blcs_log_nifs_file_seekbof(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    blcs_log_file_handle* handle;

    if (enif_get_resource(env, argv[0], blcs_log_file_RESOURCE, (void**)&handle))
    {
        if (lseek(handle->fd, 0, SEEK_SET) != (off_t)-1)
        {
            return ATOM_OK;
        }
        else
        {
            /* Write failed altogether */
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM blcs_log_nifs_file_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{   
    blcs_log_file_handle* handle;

    if (enif_get_resource(env, argv[0], blcs_log_file_RESOURCE, (void**)&handle))
    {
        off_t ofs = lseek(handle->fd, 0, SEEK_CUR);
        if (ofs == (off_t)-1)
        {
            return enif_make_tuple2(env, ATOM_ERROR, errno_atom(env, errno));
        }

        if (ftruncate(handle->fd, ofs) == -1)
        {
            return errno_error_tuple(env, ATOM_FTRUNCATE_ERROR, errno);
        }

        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM errno_atom(ErlNifEnv* env, int error)
{
    return enif_make_atom(env, erl_errno_id(error));
}

ERL_NIF_TERM errno_error_tuple(ErlNifEnv* env, ERL_NIF_TERM key, int error)
{
    // Construct a tuple of form: {error, {Key, ErrnoAtom}}
    return enif_make_tuple2(env, ATOM_ERROR,
                            enif_make_tuple2(env, key, errno_atom(env, error)));
}
ERL_NIF_INIT(blcs_log_nifs, nif_funcs, &on_load, NULL, NULL, NULL);
