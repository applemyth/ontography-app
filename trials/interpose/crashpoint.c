/*
 * Crash points: a library the trials load into the server under test, and
 * only there, with DYLD_INSERT_LIBRARIES. It counts the server's durable
 * system calls on files under TRIALS_CRASH_ROOT (the data directory): fsync,
 * fcntl with F_FULLFSYNC or F_BARRIERFSYNC, rename and renameat, unlink and
 * unlinkat, mkdir, and write, pwrite and writev to files there. It records
 * each counted call, with its index and path, to TRIALS_CRASH_LOG, so a dry
 * run learns every point and a label for it. At the TRIALS_CRASH_AT-th call
 * it acts as TRIALS_CRASH_MODE says:
 *
 *   before  kills the process with SIGKILL before the call
 *   after   makes the call, then kills the process
 *   eio     fails that one call with EIO
 *   enospc  fails that one call with ENOSPC
 *
 * TRIALS_CRASH_SKIP lists paths, separated by colons, whose calls are not
 * counted, such as the server's log. Without TRIALS_CRASH_ROOT the library
 * does nothing.
 *
 * It uses macOS's interposition (a __DATA,__interpose section): dyld points
 * every other image's calls to these functions at the replacements, while
 * this library's own calls reach the originals, so the replacements call the
 * originals by name and never recurse into themselves. A per-thread guard
 * also covers anything the system library might call back while a call is
 * being examined. The constructor removes DYLD_INSERT_LIBRARIES and these
 * variables from the environment, so programs the server starts run without
 * the library.
 */

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#define INTERPOSE(replacement, original)                                      \
    __attribute__((used)) static struct {                                    \
        const void *replacement;                                             \
        const void *original;                                                \
    } interpose_##original __attribute__((section("__DATA,__interpose"))) = { \
        (const void *)(unsigned long)&replacement,                           \
        (const void *)(unsigned long)&original}

enum mode { BEFORE, AFTER, FAIL_EIO, FAIL_ENOSPC };

#define MAX_SKIPS 8

static char root[MAXPATHLEN];
static size_t root_length;
static char skips[MAX_SKIPS][MAXPATHLEN];
static size_t skip_lengths[MAX_SKIPS];
static int skip_count;
/* The armed call's index, from 1; 0 counts without acting. */
static long crash_at;
static enum mode mode = BEFORE;
static int log_fd = -1;
static atomic_long counter;
static atomic_int ready;
static pthread_key_t guard;

static void configure(void) {
    const char *value = getenv("TRIALS_CRASH_ROOT");
    if (value != NULL && value[0] == '/' && strlen(value) < sizeof root) {
        strcpy(root, value);
        root_length = strlen(root);
        while (root_length > 1 && root[root_length - 1] == '/') {
            root[--root_length] = '\0';
        }
    }
    value = getenv("TRIALS_CRASH_AT");
    if (value != NULL) {
        crash_at = strtol(value, NULL, 10);
    }
    value = getenv("TRIALS_CRASH_MODE");
    if (value != NULL) {
        if (strcmp(value, "after") == 0) {
            mode = AFTER;
        } else if (strcmp(value, "eio") == 0) {
            mode = FAIL_EIO;
        } else if (strcmp(value, "enospc") == 0) {
            mode = FAIL_ENOSPC;
        }
    }
    value = getenv("TRIALS_CRASH_SKIP");
    while (value != NULL && *value != '\0' && skip_count < MAX_SKIPS) {
        const char *end = strchr(value, ':');
        size_t length = end != NULL ? (size_t)(end - value) : strlen(value);
        if (length > 0 && length < MAXPATHLEN) {
            memcpy(skips[skip_count], value, length);
            skips[skip_count][length] = '\0';
            skip_lengths[skip_count] = length;
            skip_count++;
        }
        value = end != NULL ? end + 1 : NULL;
    }
    value = getenv("TRIALS_CRASH_LOG");
    if (value != NULL) {
        log_fd = open(value, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    }
}

__attribute__((constructor)) static void setup(void) {
    configure();
    pthread_key_create(&guard, NULL);
    unsetenv("DYLD_INSERT_LIBRARIES");
    unsetenv("TRIALS_CRASH_ROOT");
    unsetenv("TRIALS_CRASH_AT");
    unsetenv("TRIALS_CRASH_MODE");
    unsetenv("TRIALS_CRASH_SKIP");
    unsetenv("TRIALS_CRASH_LOG");
    atomic_store(&ready, root_length > 0);
}

/* Whether `path` is at or below `prefix`, ignoring case as the file system
 * does. */
static int within(const char *path, const char *prefix, size_t length) {
    return strncasecmp(path, prefix, length) == 0 &&
           (path[length] == '/' || path[length] == '\0');
}

static int counted(const char *path) {
    if (!within(path, root, root_length)) {
        return 0;
    }
    for (int i = 0; i < skip_count; i++) {
        if (within(path, skips[i], skip_lengths[i])) {
            return 0;
        }
    }
    return 1;
}

/* The path an open descriptor names; 0 for a socket, pipe or closed one. */
static int descriptor_path(int fd, char *out) {
    return fd >= 0 && fcntl(fd, F_GETPATH, out) != -1;
}

/* The absolute path `path` names, taken relative to `dirfd` (or the working
 * directory) when it is relative. */
static int absolute(int dirfd, const char *path, char *out) {
    if (path == NULL) {
        return 0;
    }
    if (path[0] == '/') {
        if (strlen(path) >= MAXPATHLEN) {
            return 0;
        }
        strcpy(out, path);
        return 1;
    }
    char base[MAXPATHLEN];
    if (dirfd == AT_FDCWD) {
        if (getcwd(base, sizeof base) == NULL) {
            return 0;
        }
    } else if (!descriptor_path(dirfd, base)) {
        return 0;
    }
    size_t length = strlen(base);
    if (length + 1 + strlen(path) >= MAXPATHLEN) {
        return 0;
    }
    memcpy(out, base, length);
    out[length] = '/';
    strcpy(out + length + 1, path);
    return 1;
}

static size_t append(char *buffer, size_t at, size_t size, const char *text) {
    while (*text != '\0' && at + 1 < size) {
        buffer[at++] = *text++;
    }
    return at;
}

/* One line per counted call: index, call and path(s), separated by tabs. The
 * whole line goes out in one write, so lines from threads never mix. */
static void record(long index, const char *call, const char *path, const char *other) {
    if (log_fd < 0) {
        return;
    }
    char line[2 * MAXPATHLEN + 64];
    char digits[24];
    int n = 0;
    unsigned long value = (unsigned long)index;
    do {
        digits[n++] = (char)('0' + value % 10);
        value /= 10;
    } while (value > 0);
    size_t at = 0;
    while (n > 0) {
        line[at++] = digits[--n];
    }
    at = append(line, at, sizeof line - 1, "\t");
    at = append(line, at, sizeof line - 1, call);
    at = append(line, at, sizeof line - 1, "\t");
    at = append(line, at, sizeof line - 1, path);
    if (other != NULL) {
        at = append(line, at, sizeof line - 1, "\t");
        at = append(line, at, sizeof line - 1, other);
    }
    line[at++] = '\n';
    ssize_t ignored = write(log_fd, line, at);
    (void)ignored;
}

static void die(void) {
    kill(getpid(), SIGKILL);
    for (;;) {
        pause();
    }
}

/* What becomes of one call about to be made. */
struct verdict {
    /* Fail the call with this errno instead of making it; 0 makes it. */
    int fail;
    /* Kill the process once the call returns. */
    int kill_after;
};

/* Counts a call on `path` (and `other`, for a rename), records it, and says
 * what to do; at the armed call in `before` mode it does not return. */
static struct verdict count(const char *call, const char *path, const char *other) {
    struct verdict verdict = {0, 0};
    long index = atomic_fetch_add(&counter, 1) + 1;
    record(index, call, path, other);
    if (index != crash_at) {
        return verdict;
    }
    switch (mode) {
    case BEFORE:
        die();
        break;
    case AFTER:
        verdict.kill_after = 1;
        break;
    case FAIL_EIO:
        verdict.fail = EIO;
        break;
    case FAIL_ENOSPC:
        verdict.fail = ENOSPC;
        break;
    }
    return verdict;
}

/* Enters the examination of a call, unless this thread is already examining
 * one or the library is not configured. */
static int enter(void) {
    if (!atomic_load(&ready) || pthread_getspecific(guard) != NULL) {
        return 0;
    }
    pthread_setspecific(guard, (const void *)1);
    return 1;
}

static void leave(void) { pthread_setspecific(guard, NULL); }

static struct verdict on_descriptor(const char *call, int fd) {
    struct verdict verdict = {0, 0};
    if (!enter()) {
        return verdict;
    }
    char path[MAXPATHLEN];
    if (descriptor_path(fd, path) && counted(path)) {
        verdict = count(call, path, NULL);
    }
    leave();
    return verdict;
}

static struct verdict on_paths(const char *call, int dirfd, const char *path, int other_dirfd,
                               const char *other) {
    struct verdict verdict = {0, 0};
    if (!enter()) {
        return verdict;
    }
    char first[MAXPATHLEN];
    char second[MAXPATHLEN];
    int has_first = absolute(dirfd, path, first);
    int has_second = other != NULL && absolute(other_dirfd, other, second);
    if ((has_first && counted(first)) || (has_second && counted(second))) {
        verdict = count(call, has_first ? first : "?", other == NULL ? NULL : has_second ? second : "?");
    }
    leave();
    return verdict;
}

/* Applies a verdict around the original call's result. */
#define ACT(verdict, type, call)                                                                  \
    do {                                                                                          \
        if ((verdict).fail != 0) {                                                                \
            errno = (verdict).fail;                                                               \
            return (type)-1;                                                                      \
        }                                                                                         \
        type result = (call);                                                                     \
        if ((verdict).kill_after) {                                                               \
            die();                                                                                \
        }                                                                                         \
        return result;                                                                            \
    } while (0)

static ssize_t crash_write(int fd, const void *buffer, size_t size) {
    struct verdict verdict = on_descriptor("write", fd);
    ACT(verdict, ssize_t, write(fd, buffer, size));
}

static ssize_t crash_pwrite(int fd, const void *buffer, size_t size, off_t offset) {
    struct verdict verdict = on_descriptor("pwrite", fd);
    ACT(verdict, ssize_t, pwrite(fd, buffer, size, offset));
}

static ssize_t crash_writev(int fd, const struct iovec *vectors, int count) {
    struct verdict verdict = on_descriptor("writev", fd);
    ACT(verdict, ssize_t, writev(fd, vectors, count));
}

static int crash_fsync(int fd) {
    struct verdict verdict = on_descriptor("fsync", fd);
    ACT(verdict, int, fsync(fd));
}

/* fcntl is variadic; its one optional argument is read as a pointer-sized
 * word and passed on unchanged. Only the two sync commands are counted. */
static int crash_fcntl(int fd, int command, ...) {
    va_list arguments;
    va_start(arguments, command);
    void *argument = va_arg(arguments, void *);
    va_end(arguments);
    struct verdict verdict = {0, 0};
    if (command == F_FULLFSYNC) {
        verdict = on_descriptor("fcntl(F_FULLFSYNC)", fd);
    } else if (command == F_BARRIERFSYNC) {
        verdict = on_descriptor("fcntl(F_BARRIERFSYNC)", fd);
    }
    ACT(verdict, int, fcntl(fd, command, argument));
}

static int crash_rename(const char *from, const char *to) {
    struct verdict verdict = on_paths("rename", AT_FDCWD, from, AT_FDCWD, to);
    ACT(verdict, int, rename(from, to));
}

static int crash_renameat(int from_dir, const char *from, int to_dir, const char *to) {
    struct verdict verdict = on_paths("renameat", from_dir, from, to_dir, to);
    ACT(verdict, int, renameat(from_dir, from, to_dir, to));
}

static int crash_unlink(const char *path) {
    struct verdict verdict = on_paths("unlink", AT_FDCWD, path, AT_FDCWD, NULL);
    ACT(verdict, int, unlink(path));
}

static int crash_unlinkat(int dirfd, const char *path, int flags) {
    const char *call = (flags & AT_REMOVEDIR) ? "unlinkat(dir)" : "unlinkat";
    struct verdict verdict = on_paths(call, dirfd, path, AT_FDCWD, NULL);
    ACT(verdict, int, unlinkat(dirfd, path, flags));
}

static int crash_mkdir(const char *path, mode_t permissions) {
    struct verdict verdict = on_paths("mkdir", AT_FDCWD, path, AT_FDCWD, NULL);
    ACT(verdict, int, mkdir(path, permissions));
}

INTERPOSE(crash_write, write);
INTERPOSE(crash_pwrite, pwrite);
INTERPOSE(crash_writev, writev);
INTERPOSE(crash_fsync, fsync);
INTERPOSE(crash_fcntl, fcntl);
INTERPOSE(crash_rename, rename);
INTERPOSE(crash_renameat, renameat);
INTERPOSE(crash_unlink, unlink);
INTERPOSE(crash_unlinkat, unlinkat);
INTERPOSE(crash_mkdir, mkdir);
