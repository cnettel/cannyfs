/*
  cannyfs, getting high file system performance by a "can do" attitude.
  Intended for batch-based processing where "rm -rf" on all outputs
  and rerun is a real option.

  Copyright (C) 2016       Carl Nettelblad <carl.nettelblad@it.uu.se>

  Based on fusexmp_fh.c example, notices below.

  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

/** @file
 * @tableofcontents
 *
 * cannyfs.cpp - FUSE: Filesystem in Userspace
 *
 * \section section_compile compiling this example
 *
 * g++ -Wall cannyfs.cpp `pkg-config fuse3 --cflags --libs` -lulockmgr -o cannyfs
 *
 * \section section_source the complete source
 * \include cannyfs.cpp
 */

#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define HAVE_UTIMENSAT
//#define HAVE_SETXATTR

#define _GNU_SOURCE

#include <fuse.h>

#ifdef HAVE_LIBULOCKMGR
#include <ulockmgr.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <sys/file.h> /* flock(2) */

#include <atomic>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <map>
#include <set>
#include <string>
#include <functional>
#include <queue>

#define TBB_PREVIEW_GLOBAL_CONTROL 1
#include <tbb/global_control.h>

#include <tbb/task_scheduler_init.h>
#include <tbb/task_arena.h>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_queue.h>

#include <boost/lockfree/stack.hpp>
#include <boost/filesystem.hpp>

using namespace tbb;
using namespace boost::filesystem;

using namespace std;

atomic_llong eventId(0);
atomic_llong retiredCount(0);

struct cannyfs_filedata
{
	mutex datalock;
	mutex oplock;
	atomic_llong firstEventId{ -1 };
	atomic_llong lastEventId{ -1 };
	bool running = false;

	// Kill it if we don't have any ops.
	// TODO: What if file is closed with close list?
	// long long numOps;
	condition_variable processed;

	queue<function<int(void)> > ops;

	struct stat ourstats = {};
	atomic_bool created{ false };
	atomic_bool missing{ false };
	atomic_bool knowndir{ false };

	void run()
	{
		unique_lock<mutex> locallock(this->datalock);
		running = true;
		while (!ops.empty())
		{
			function<int(void)> op = ops.front();
			ops.pop();

			locallock.unlock();
			op();
			locallock.lock();
		}
		running = false;
	}

	void spinevent(unique_lock<mutex>& locallock)
	{
		long long eventId = lastEventId;
		while (firstEventId < eventId)
		{
			processed.wait(locallock);
		}
	}
};

struct cannyfs_filehandle
{
	mutex lock;
	int64_t fd;
	int pipefds[2];
	condition_variable opened;

	cannyfs_filehandle() : fd(-1), pipefds{-1, -1}
	{
	}

	~cannyfs_filehandle()
	{
		// We might want to report the error up
		/*if (fd != -1)
		{
			close(fd);
		}*/
		if (pipefds[0] != -1)
		{
			close(pipefds[0]);
			close(pipefds[1]);
		}
	}

	int getpipefd(int dir)
	{
		lock_guard<mutex> locallock(lock);
		if (pipefds[0] == -1)
		{
			// TODO: CHECK ERRORS.
			pipe(pipefds);
			//fcntl(pipefds[0], F_SETPIPE_SZ, 131072);
		}

		return pipefds[dir];
	}

	void setfh(uint64_t fd)
	{
		lock_guard<mutex> locallock(lock);
		this->fd = fd;
		opened.notify_all();
	}

	uint64_t getfh()
	{
		unique_lock<mutex> locallock(lock);
		while (fd == -1)
		{
			opened.wait(locallock);
		}
		
		return fd;
	}
};

typedef concurrent_vector<cannyfs_filehandle> fhstype;
fhstype fhs;
boost::lockfree::stack<fhstype::iterator> freefhs(16);

fhstype::iterator getnewfh()
{
	fhstype::iterator toreturn;
	if (freefhs.pop(toreturn))
	{
		return toreturn;
	}
	return fhs.grow_by(1);
}

cannyfs_filehandle* getcfh(int fd)
{
	return &fhs[fd];
}

uint64_t getfh(const fuse_file_info* fi)
{
	return getcfh(fi->fh)->getfh();
}


struct cannyfs_options
{
	bool eagerflush = true;
	bool eagerlink = true;
	bool eagerchmod = true;
	bool veryeageraccess = true;
	bool eageraccess = true;
	bool eagerutimens = false;
	bool eagerchown = true;
	bool eagerclose = true;
	bool closeverylate = false;
	bool restrictivedirs = false;
	bool eagerfsync = true;
	bool eagercreate = true;
	bool ignorefsync = true;
	bool verbose = false;
	bool eagerxattr = true;
	bool inaccuratestat = true;
	bool cachemissing = true;
	bool assumecreateddirempty = true;
	int numThreads = 16;
} options;


const int NO_BARRIER = 1;
const int JUST_BARRIER = 0;
const int LOCK_WHOLE = 2;


void cannyfs_reporterror()
{
	fprintf(stderr, "ERROR: %d\n", errno);
}

void cannyfs_negerrorchecker(int code)
{
	if (code < 0)
	{
		cannyfs_reporterror();
	}
	// abort();
	// TODO: ADD OPTION
}

struct cannyfs_closer
{
private:
	int fd;
public:
	cannyfs_closer() = delete;
	cannyfs_closer(int fd) : fd(fd) {}
	~cannyfs_closer()
	{
		cannyfs_negerrorchecker(close(fd));
	}
};

vector<cannyfs_closer> closes;

struct comp {
	bool operator()(const std::string& lhs, const std::string& rhs)
	{
		if (lhs.length() != rhs.length())
			return lhs.length() < rhs.length();
		return lhs < rhs;
	}
};

struct cannyfs_filemap
{
private:
	map<string, cannyfs_filedata, comp> data;
	shared_timed_mutex lock;
public:
	cannyfs_filedata* get(const std::string& path, bool always, unique_lock<mutex>& lock, bool lockdata = false)
	{
		cannyfs_filedata* result = nullptr;
		auto locktransferline = [&] { lock = unique_lock<mutex>(lockdata ? result->datalock : result->oplock); };

		{
			shared_lock<shared_timed_mutex> maplock(this->lock);
			auto i = data.find(path);
			if (i != data.end())
			{
				result = &i->second;
				maplock.unlock();
				locktransferline();
			}
		}

		if (always && !result)
		{
			unique_lock<shared_timed_mutex> maplock(this->lock);
			auto i = data.find(path);
			if (i != data.end())
			{
				result = &i->second;
			}
			else
			{
				result = &data[path];
			}
			maplock.unlock();
			locktransferline();
		}

		return result;
	}
} filemap;

struct cannyfs_reader
{
public:
	unique_lock<mutex> lock;
	cannyfs_filedata* fileobj;
	cannyfs_reader(const std::string& path, int flag)
	{
		if (options.verbose) fprintf(stderr, "Waiting for reading %s\n", path.c_str());

		unique_lock<mutex> locallock;
		fileobj = filemap.get(path, flag & LOCK_WHOLE, locallock, flag & NO_BARRIER);

		if (!(flag & NO_BARRIER) && fileobj)
		{
			fileobj->spinevent(locallock);
		}

		if (flag & LOCK_WHOLE)
		{
			swap(lock, locallock);
		}

		if (options.verbose) fprintf(stderr, "Got reader lock %s\n", path.c_str());
	}
};

struct cannyfs_writer
{
private:
	unique_lock<mutex> lock;
	cannyfs_filedata* fileobj;
	cannyfs_writer* generalwriter;
	long long eventId;
	bool global;
public:
	cannyfs_writer(const std::string& path, int flag, long long eventId) : eventId(eventId), global(path != "")
	{
		if (options.verbose) fprintf(stderr, "Entering write lock for %s\n", path.c_str());
		generalwriter = nullptr;	
		fileobj = filemap.get(path, true, lock);

		if (flag != LOCK_WHOLE)
		{
			lock.unlock();
		}

		if (!global && options.restrictivedirs) generalwriter = new cannyfs_writer("", JUST_BARRIER, eventId);
	}

	~cannyfs_writer()
	{
		if (!lock.owns_lock()) lock.lock();
		fileobj->firstEventId = eventId;
		fileobj->processed.notify_all();
		if (generalwriter) delete generalwriter;
	}
};


struct cannyfs_dirreader : cannyfs_reader
{
private:
public:
	cannyfs_dirreader() = delete;
	cannyfs_dirreader(std::string path, int flag) :
		cannyfs_reader(options.restrictivedirs ? "" : path, flag)
	{
	}
};

task_scheduler_init init(16);
task_arena workQueue(16);

int cannyfs_add_write_inner(bool defer, const std::string& path, auto fun)
{
	long long eventIdNow;

	unique_lock<mutex> lock;
	cannyfs_filedata* fileobj = filemap.get(path, true, lock, true);

	eventIdNow = ++::eventId;

	if (!defer) fileobj->spinevent(lock);

	fileobj->lastEventId = eventIdNow;

	auto worker = [eventIdNow, fun]() {
		if (options.verbose) fprintf(stderr, "Doing event ID %lld\n", eventIdNow);
		int retval = fun(eventIdNow);
		if (options.verbose) fprintf(stderr, "Did event ID %lld with result %d\n", eventIdNow, retval);
		retiredCount++;
		return retval;
	};

	// TODO: NOT ALL EVENTS ARE RETIRED
	//fprintf(stderr, "In flight %lld\n", eventIdNow - retiredCount);

	while (eventIdNow - retiredCount > 400)
	{
		fprintf(stderr, "Stalling for time...\n");
		usleep(100);
	}

	if (!defer)
	{
		lock.unlock();
		return worker();
	}
	else
	{
		fileobj->ops.emplace(worker);
		if (!fileobj->running)
		{
			// Hey, WE will make it running now.
			fileobj->running = true;
			lock.unlock();
			workQueue.enqueue([fileobj] { fileobj->run(); });
		}

		return 0;
	}
}

int cannyfs_add_write(bool defer, const std::string& path, auto fun)
{
	if (options.verbose) fprintf(stderr, "Adding write (A) for %s\n", path.c_str());
	return cannyfs_add_write_inner(defer, path, [path, fun](int eventId)->int {
		cannyfs_writer writer(path, LOCK_WHOLE, eventId);
		return fun(path);
	});
}

int cannyfs_add_write(bool defer, const std::string& path, fuse_file_info* origfi, auto fun)
{
	if (options.verbose) fprintf(stderr, "Adding write (B) for %s\n", path.c_str());
	fuse_file_info fi = *origfi;
	return cannyfs_add_write_inner(defer, path, [path, fun, fi](int eventId)->int {
		cannyfs_writer writer(path, LOCK_WHOLE, eventId);
		return fun(path, &fi);
	});
}

int cannyfs_add_write(bool defer, const std::string& path1, const std::string& path2, auto fun)
{
	if (options.verbose) fprintf(stderr, "Adding write (C) for %s\n", path1.c_str());
	return cannyfs_add_write_inner(defer, path2, [path1, path2, fun](int eventId)->int {
		//cannyfs_writer writer1(path1, LOCK_WHOLE, eventId);

		// TODO: LOCKING MODEL MESSED UP
		cannyfs_reader reader(path1, JUST_BARRIER);
		cannyfs_writer writer2(path2, LOCK_WHOLE, eventId);

		return fun(path1, path2);
	});
}


static int cannyfs_getattr(const char *path, struct stat *stbuf)
{
	bool inaccurate = options.inaccuratestat;
	cannyfs_reader b(path, inaccurate ? (NO_BARRIER | LOCK_WHOLE) : JUST_BARRIER);

	if (inaccurate)
	{
		if (options.cachemissing && b.fileobj && b.fileobj->missing)
		{
			return -ENOENT;
		}

		bool wascreated = b.fileobj && b.fileobj->created;
		b.lock.unlock();

		if (wascreated)
		{
			*stbuf = {};
			stbuf->st_mode = S_IRUSR | S_IWUSR | ((b.fileobj->knowndir) ? S_IFDIR : S_IFREG);

			return 0;
		}

		if (options.assumecreateddirempty)
		{
			cannyfs_reader parentdata(::path(path).parent_path().generic_string(), JUST_BARRIER | LOCK_WHOLE);

			// If the parent dir was missing or is known not to exist, we can safely (?) assume that the subentry does not exist unless WE created it
			if (parentdata.fileobj && (parentdata.fileobj->missing || parentdata.fileobj->created))
			{
				return -ENOENT;
			}
		}
	}
	int res = lstat(path, stbuf);
	if (res == -1)
	{
		int err = errno;
		if (options.cachemissing && err == ENOENT)
		{
			cannyfs_reader b2(path, NO_BARRIER | LOCK_WHOLE);
			b.fileobj->missing = true;
		}
		return -errno;
	}

	return 0;
}

static int cannyfs_fgetattr(const char *path, struct stat *stbuf,
			struct fuse_file_info *fi)
{
	if (options.inaccuratestat)
	{
		return cannyfs_getattr(path, stbuf);
	}
	cannyfs_reader b(path, JUST_BARRIER);

	int res;

	(void) path;

	fprintf(stderr, "Stat %s\n", path);
	res = fstat(getfh(fi), stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_access(const char *path, int mask)
{
	if (options.veryeageraccess) return 0;

	// At least make the writes finish?
	cannyfs_reader b(path, JUST_BARRIER);

	if (options.eageraccess) return 0;

	int res;

	res = access(path, mask);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_readlink(const char *path, char *buf, size_t size)
{
	cannyfs_reader b(path, JUST_BARRIER);

	int res;

	res = readlink(path, buf, size - 1);
	if (res == -1)
		return -errno;

	buf[res] = '\0';
	return 0;
}

struct cannyfs_dirp {
	DIR *dp;
	struct dirent *entry;
	off_t offset;
};

static int cannyfs_opendir(const char *path, struct fuse_file_info *fi)
{
	// With accurate dirs, ALL operations need to finish
	cannyfs_dirreader b(path, JUST_BARRIER);

	int res;
	cannyfs_dirp* d = new cannyfs_dirp;
	if (d == NULL)
		return -ENOMEM;

	d->dp = opendir(path);
	if (d->dp == NULL) {
		res = -errno;
		free(d);
		return res;
	}
	d->offset = 0;
	d->entry = NULL;

	fi->fh = getnewfh() - fhs.begin();
	getcfh(fi->fh)->setfh((unsigned long)d);
	return 0;
}

static inline struct cannyfs_dirp *get_dirp(struct fuse_file_info *fi)
{
	return (struct cannyfs_dirp *) (uintptr_t) getfh(fi);
}

static int cannyfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi
#if FUSE_USE_VERSION >= 30
		       , enum fuse_readdir_flags flags
#endif
)
{
	cannyfs_dirreader b(path, JUST_BARRIER);

	struct cannyfs_dirp *d = get_dirp(fi);

	(void) path;
	if (offset != d->offset) {
		seekdir(d->dp, offset);
		d->entry = NULL;
		d->offset = offset;
	}
	while (1) {
		struct stat st;
		off_t nextoff;
#if FUSE_USE_VERSION >= 30
		enum fuse_fill_dir_flags fill_flags = (fuse_fill_dir_flags) 0;
#endif

		if (!d->entry) {
			d->entry = readdir(d->dp);
			if (!d->entry)
				break;
		}
#if FUSE_USE_VERSION >= 30
#ifdef HAVE_FSTATAT
		if (flags & FUSE_READDIR_PLUS) {
			int res;

			res = fstatat(dirfd(d->dp), d->entry->d_name, &st,
				      AT_SYMLINK_NOFOLLOW);
			if (res != -1)
				fill_flags |= FUSE_FILL_DIR_PLUS;
		}
#endif
		if (!(fill_flags & FUSE_FILL_DIR_PLUS)) {
#endif
			memset(&st, 0, sizeof(st));
			st.st_ino = d->entry->d_ino;
			st.st_mode = d->entry->d_type << 12;
#if FUSE_USE_VERSION >= 30
		}
#endif
		nextoff = telldir(d->dp);
		if (filler(buf, d->entry->d_name, &st, nextoff
#if FUSE_USE_VERSION >= 30
			, fill_flags
#endif
		))
			break;

		d->entry = NULL;
		d->offset = nextoff;
	}

	return 0;
}

static int cannyfs_releasedir(const char *path, struct fuse_file_info *fi)
{
	struct cannyfs_dirp *d = get_dirp(fi);
	(void) path;
	closedir(d->dp);
	free(d);
	return 0;
}

static int cannyfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res;

	if (S_ISFIFO(mode))
		res = mkfifo(path, mode);
	else
		res = mknod(path, mode, rdev);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_mkdir(const char *path, mode_t mode)
{
	int res;
	{
		cannyfs_reader b(path, LOCK_WHOLE);
		b.fileobj->missing = false; 
		b.fileobj->created = true;
		b.fileobj->knowndir = true;
	}

	res = mkdir(path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_unlink(const char *path)
{
	// We should KILL all pending IOs, not let them go through to some corpse. Or, well,
	// we should have a flag to do that.
	// TODO: cannyfs_clear(path);
	int res;

	res = unlink(path);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_rmdir(const char *path)
{
	int res;

	res = rmdir(path);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_symlink(const char *from, const char *to)
{
	// TODO: Barrier here?
	int res;

	res = symlink(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_rename(const char *from, const char *to
#if FUSE_USE_VERSION >= 30
	, unsigned int flags
#endif
)
{
	// TODO: I/O logic at rename, what will the names be
	cannyfs_reader b(from, LOCK_WHOLE);
	int res;

#if FUSE_USE_VERSION >= 30
	/* When we have renameat2() in libc, then we can implement flags */
	if (flags)
		return -EINVAL;
#endif

	res = rename(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_link(const char *cfrom, const char *cto)
{
	return cannyfs_add_write(options.eagerlink, cfrom, cto, [](const std::string& from, const std::string& to) {
		int res;

		res = link(from.c_str(), to.c_str());
		if (res == -1)
			return -errno;

		return 0;
	});
}

static int cannyfs_chmod(const char *cpath, mode_t mode)
{
	return cannyfs_add_write(options.eagerchmod, cpath, [mode](const std::string& path) {
		int res;
		res = chmod(path.c_str(), mode);
		if (res == -1)
			return -errno;

		return 0;
	});
}

static int cannyfs_chown(const char *cpath, uid_t uid, gid_t gid)
{
	return cannyfs_add_write(options.eagerchown, cpath, [uid, gid](const std::string& path) {
		int res;

		res = lchown(path.c_str(), uid, gid);
		if (res == -1)
			return -errno;

		return 0;
	});
}

static int cannyfs_truncate(const char *path, off_t size)
{
	fprintf(stderr, "Truncate1?\n");
	// TODO: FUN STUFF COULD BE DONE HERE TO AVOID WRITES
	int res;

	res = truncate(path, size);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_ftruncate(const char *path, off_t size,
			 struct fuse_file_info *fi)
{
	fprintf(stderr, "Truncate2?\n");
	// TODO: FUN STUFF COULD BE DONE HERE TO AVOID WRITES
	int res;

	(void) path;

	res = ftruncate(getfh(fi), size);
	if (res == -1)
		return -errno;

	return 0;
}

#ifdef HAVE_UTIMENSAT
static int cannyfs_utimens(const char *cpath, const struct timespec ts[2])
{
	return cannyfs_add_write(options.eagerutimens, cpath, [ts](const std::string& path) {
		int res;

		/* don't use utime/utimes since they follow symlinks */
		res = utimensat(0, path.c_str(), ts, AT_SYMLINK_NOFOLLOW);
		if (res == -1)
			return -errno;

		return 0;
	});
}
#endif

static int cannyfs_create(const char *cpath, mode_t mode, struct fuse_file_info *fi)
{
	if (options.verbose) fprintf(stderr, "Going to create %s\n", cpath);
	fi->fh = getnewfh() - fhs.begin();
	{
		cannyfs_reader b(cpath, NO_BARRIER | LOCK_WHOLE);
		b.fileobj->created = true;
		b.fileobj->missing = false;
	}

	return cannyfs_add_write(options.eagercreate, cpath, fi, [mode](const std::string& path, const fuse_file_info* fi)
	{
		int fd = open(path.c_str(), fi->flags, mode);
		if (fd == -1)
			return -errno;

		getcfh(fi->fh)->setfh(fd);
		return 0;
	});
}

static int cannyfs_open(const char *path, struct fuse_file_info *fi)
{
	if (options.verbose) fprintf(stderr, "Going to open %s\n", path);
	int fd;

	fi->fh = getnewfh() - fhs.begin();
	fd = open(path, fi->flags);
	if (fd == -1)
		return -errno;

	getcfh(fi->fh)->setfh(fd);
	return 0;
}

static int cannyfs_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	int res;

	(void) path;
	res = pread(getfh(fi), buf, size, offset);
	if (res == -1)
		res = -errno;

	return res;
}

static int cannyfs_read_buf(const char *path, struct fuse_bufvec **bufp,
			size_t size, off_t offset, struct fuse_file_info *fi)
{
	cannyfs_reader b(path, JUST_BARRIER);
	struct fuse_bufvec *src;

	(void) path;

	src = new fuse_bufvec;
	if (src == NULL)
		return -ENOMEM;

	*src = FUSE_BUFVEC_INIT(size);

	src->buf[0].flags = (fuse_buf_flags) (FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
	src->buf[0].fd = getfh(fi);
	src->buf[0].pos = offset;

	*bufp = src;

	return 0;
}

static int cannyfs_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	// TODO, NO WRITE, JUST WRITE_BUF???
	abort();
	int res;

	(void) path;
	res = pwrite(getfh(fi), buf, size, offset);
	if (res == -1)
		res = -errno;

	return res;
}

static int cannyfs_write_buf(const char *cpath, struct fuse_bufvec *buf,
		     off_t offset, struct fuse_file_info *fi)
{
	cannyfs_filehandle* cfh = getcfh(fi->fh);

	int sz = fuse_buf_size(buf);

	int toret = cannyfs_add_write(true, cpath, fi, [sz, offset](const std::string& path, const fuse_file_info *fi) {

		struct fuse_bufvec dst = FUSE_BUFVEC_INIT(sz);

		dst.buf[0].flags = (fuse_buf_flags) (FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
		dst.buf[0].fd = getfh(fi);
		dst.buf[0].pos = offset;
		
		struct fuse_bufvec newsrc = FUSE_BUFVEC_INIT(sz);
		newsrc.buf[0].fd = getcfh(fi->fh)->getpipefd(0);
		fd_set set;
		FD_ZERO(&set);
		FD_SET(newsrc.buf[0].fd, &set);
		newsrc.buf[0].flags = (fuse_buf_flags)(FUSE_BUF_FD_RETRY | FUSE_BUF_IS_FD);

		int val = 0;

		while (val < sz)
		{
			while (select(FD_SETSIZE, &set, nullptr, nullptr, nullptr) <= 0) {}
			int ret = fuse_buf_copy(&dst, &newsrc, (fuse_buf_copy_flags)0);
			if (ret < 0)
			{
				return ret;
			}

			val += ret;
		}

		return val;
	});

	if (toret < 0)
	{
		return toret;
	}

	struct fuse_bufvec halfdst = FUSE_BUFVEC_INIT(sz);

	halfdst.buf[0].flags = (fuse_buf_flags) (FUSE_BUF_IS_FD | FUSE_BUF_FD_RETRY);
	halfdst.buf[0].fd = getcfh(fi->fh)->getpipefd(1);

	fd_set set;
	FD_ZERO(&set);
	FD_SET(halfdst.buf[0].fd, &set);

	int val = 0;
	while (val < sz)
	{
		while (select(FD_SETSIZE, nullptr, &set, nullptr, nullptr) <= 0) {}
		int ret = fuse_buf_copy(&halfdst, buf, (fuse_buf_copy_flags)0);
		if (ret < 0)
		{
			return ret;
		}

		val += ret;
	}

	return val;
}

static int cannyfs_statfs(const char *path, struct statvfs *stbuf)
{
	cannyfs_reader b(path, JUST_BARRIER);
	int res;

	res = statvfs(path, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_flush(const char *cpath, struct fuse_file_info *fi)
{
	if (options.closeverylate)
	{
		// Just adding it to the close list might lock, if we don't have an fh yet
		return cannyfs_add_write(options.eagerflush, cpath, fi, [](const std::string& path, const fuse_file_info *fi) {
			closes.push_back(dup(getfh(fi)));

			// TODO: Adjust return value if dup fails?
			return 0;
		});
	}

	return cannyfs_add_write(options.eagerflush, cpath, fi, [](const std::string& path, const fuse_file_info *fi) {
		int res;

		/* This is called from every close on an open file, so call the
		   close on the underlying filesystem.	But since flush may be
		   called multiple times for an open file, this must not really
		   close the file.  This is important if used on a network
		   filesystem like NFS which flush the data/metadata on close() */
		res = close(dup(getfh(fi)));
		if (res == -1)
			return -errno;

		return 0;
	});
}

static int cannyfs_release(const char *cpath, struct fuse_file_info *fi)
{
	if (options.closeverylate)
	{
		// Just adding it to the close list might lock, if we don't have an fh yet
		return cannyfs_add_write(options.eagerclose, cpath, fi, [](const std::string& path, const fuse_file_info *fi) {
			closes.push_back(getfh(fi));

			return 0;
		});
	}

	return cannyfs_add_write(options.eagerclose, cpath, fi, [](const std::string& path, const fuse_file_info *fi) {
		int fd = getfh(fi);
		getcfh(fi->fh)->~cannyfs_filehandle();
		// Reset object using default constructor
		new(getcfh(fi->fh)) cannyfs_filehandle();
		freefhs.push(fhs.begin() + fi->fh);

		return close(fd);
	});

	return 0;
}

static int cannyfs_fsync(const char *cpath, int isdatasync,
		     struct fuse_file_info *fi)
{
	if (options.ignorefsync) return 0;

	return cannyfs_add_write(options.eagerfsync, cpath, fi, [isdatasync](const std::string& path, const fuse_file_info *fi) {
		int res;
		(void)path;

#ifndef HAVE_FDATASYNC
		(void) isdatasync;
#else
		if (isdatasync)
			res = fdatasync(getfh(fi));
		else
#endif
			res = fsync(getfh(fi));
		if (res == -1)
			return -errno;

		return 0;
	});
}

#ifdef HAVE_POSIX_FALLOCATE
static int cannyfs_fallocate(const char *cpath, int mode,
			off_t offset, off_t length, struct fuse_file_info *fi)
{
	if (mode)
		return -EOPNOTSUPP;

	return cannyfs_add_write(options.eagerchown, cpath, fi, [mode, offset, length](const std::string& path, struct fuse_file_info *fi) {
		return -posix_fallocate(getfh(fi), offset, length);
	}
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int cannyfs_setxattr(const char *path, const char *cname, const char *cvalue,
			size_t size, int flags)
{
	std::string name = cname;
	std::string value = cvalue;

	return cannyfs_add_write(options.eagerxattr, path, [name, value, size, flags] (const std::string& path)
	{
		int res = lsetxattr(path.c_str(), name.c_str(), value.c_str(), size, flags);
		if (res == -1)
			return -errno;

		return 0;
	});
}

static int cannyfs_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	cannyfs_reader b(path, JUST_BARRIER);

	int res = lgetxattr(path, name, value, size);
	if (res == -1)
		return -errno;
	return res;
}

static int cannyfs_listxattr(const char *path, char *list, size_t size)
{
	cannyfs_reader b(path, JUST_BARRIER);

	int res = llistxattr(path, list, size);
	if (res == -1)
		return -errno;
	return res;
}

static int cannyfs_removexattr(const char *path, const char *cname)
{
	std::string name = cname;
	return cannyfs_add_write(options.eagerxattr, path, [name](const std::string& path)
	{
		int res = lremovexattr(path.c_str(), name.c_str());
		if (res == -1)
			return -errno;

		return 0;
	});
}
#endif /* HAVE_SETXATTR */

#ifdef HAVE_LIBULOCKMGR
static int cannyfs_lock(const char *path, struct fuse_file_info *fi, int cmd,
		    struct flock *lock)
{
	cannyfs_immediatewriter b(path, JUST_BARRIER);

	return ulockmgr_op(getfh(fi), cmd, lock, &fi->lock_owner,
			   sizeof(fi->lock_owner));
}
#endif

static int cannyfs_flock(const char *path, struct fuse_file_info *fi, int op)
{
	// TODO: cannyfs_immediatewriter INSTEAD
	cannyfs_reader b(path, JUST_BARRIER);
	int res;

	res = flock(getfh(fi), op);
	if (res == -1)
		return -errno;

	return 0;
}

static struct fuse_operations cannyfs_oper;



int main(int argc, char *argv[])
{
	global_control c(global_control::max_allowed_parallelism, 128);
	umask(0);
	cannyfs_oper.flag_nopath = 0;
	cannyfs_oper.flag_reserved = 0;
	cannyfs_oper.getattr = cannyfs_getattr;
	cannyfs_oper.readlink = cannyfs_readlink;
	cannyfs_oper.mknod = cannyfs_mknod;
	cannyfs_oper.mkdir = cannyfs_mkdir;
	cannyfs_oper.fgetattr = cannyfs_fgetattr;
	cannyfs_oper.access = cannyfs_access;

	cannyfs_oper.opendir = cannyfs_opendir;
	cannyfs_oper.readdir = cannyfs_readdir;
	cannyfs_oper.releasedir = cannyfs_releasedir;


	cannyfs_oper.symlink = cannyfs_symlink;
	cannyfs_oper.unlink = cannyfs_unlink;
	cannyfs_oper.rmdir = cannyfs_rmdir;
	cannyfs_oper.rename = cannyfs_rename;
	cannyfs_oper.link = cannyfs_link;
	cannyfs_oper.chmod = cannyfs_chmod;
	cannyfs_oper.chown = cannyfs_chown;
	cannyfs_oper.truncate = cannyfs_truncate;
	cannyfs_oper.ftruncate = cannyfs_ftruncate;
#ifdef HAVE_UTIMENSAT
	cannyfs_oper.utimens = cannyfs_utimens;
#endif
	cannyfs_oper.create = cannyfs_create;
	cannyfs_oper.open = cannyfs_open;
	cannyfs_oper.read = cannyfs_read;
	cannyfs_oper.read_buf = cannyfs_read_buf;
	cannyfs_oper.write = cannyfs_write;
	cannyfs_oper.write_buf = cannyfs_write_buf;
	cannyfs_oper.statfs = cannyfs_statfs;
	cannyfs_oper.flush = cannyfs_flush;
	cannyfs_oper.release = cannyfs_release;
	cannyfs_oper.fsync = cannyfs_fsync;
#ifdef HAVE_POSIX_FALLOCATE
	cannyfs_oper.fallocate = cannyfs_fallocate;
#endif
#ifdef HAVE_SETXATTR
	cannyfs_oper.setxattr = cannyfs_setxattr;
	cannyfs_oper.getxattr = cannyfs_getxattr;
	cannyfs_oper.listxattr = cannyfs_listxattr;
	cannyfs_oper.removexattr = cannyfs_removexattr;
#endif
#ifdef HAVE_LIBULOCKMGR
	cannyfs_oper.lock = cannyfs_lock;
#endif
	cannyfs_oper.flock = cannyfs_flock;
	workQueue.initialize();

	int toret = fuse_main(argc, argv, &cannyfs_oper, NULL);
	return toret;
}
