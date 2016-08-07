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

#define FUSE_USE_VERSION 30

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

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
#include <shared_mutex>
#include <mutex>
#include <condition_variable>
#include <map>
#include <set>
#include <string>
#include <functional>
#include <queue>
using namespace std;

struct cannyfs_filedata
{
	mutex lock;
	long long lastEventId = 0;

	// Kill it if we don't have any ops.
	// TODO: What if file is closed with close list?
	// long long numOps;
	condition_variable processed;
};

struct cannyfs_options
{
	bool eagerlink = true;
	bool eagerchmod = true;
	bool veryeageraccess = true;
	bool eageraccess = true;
	bool eagerutimens = true;
	bool eagerchown = true;
	bool eagerclose = true;
	bool closeverylate = true;
	bool restrictivedirs = false;
	bool eagerfsync = true;
	bool ignorefsync = true;
} options;


const int JUST_BARRIER = 0;
const int LOCK_WHOLE = 1;

struct cannyfs_filemap
{
private:
	map<string, cannyfs_filedata> data;
	shared_mutex lock;
public:
	cannyfs_filedata* get(std::string path, bool always, unique_lock<mutex>& lock)
	{
		cannyfs_filedata* result = nullptr;
		{
			shared_lock<shared_mutex> maplock(this->lock);
			auto i = data.find(path);
			if (i != data.end())
			{
				result = &i->second;
				lock = unique_lock<mutex>(result->lock);
			}
		}

		if (always && !result)
		{
			unique_lock<shared_mutex> maplock(this->lock);
			auto i = data.find(path);
			if (i != data.end())
			{
				result = &i->second;
			}
			else
			{
				result = &data[path];
			}
			lock = unique_lock<mutex>(result->lock);
		}

		return result;
	}
} filemap;

set<long long> waitingEvents;

int getfh(const fuse_file_info* fi)
{
	return fi->fh;
}

struct cannyfs_reader
{
private:
	unique_lock<mutex> lock;
public:
	cannyfs_filedata* fileobj;
	cannyfs_reader(std::string path, int flag)
	{
		unique_lock<mutex> locallock;
		fileobj = filemap.get(path, flag == LOCK_WHOLE, locallock);

		if (fileobj)
		{
			long long eventId = fileobj->lastEventId;
			while (waitingEvents.find(eventId) != waitingEvents.end())
			{
				fileobj->processed.wait(locallock);
			}
		}

		if (flag == LOCK_WHOLE)
		{
			swap(lock, locallock);
		}
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
	cannyfs_writer(std::string path, int flag, long long eventId) : eventId(eventId), global(path != "")
	{
		generalwriter = nullptr;
		if (global) waitingEvents.insert(eventId);
		fileobj = filemap.get(path, true, lock);

		fileobj->lastEventId = eventId;

		if (flag != LOCK_WHOLE)
		{
			lock.release();
		}

		if (!global && options.restrictivedirs) generalwriter = new cannyfs_writer("", JUST_BARRIER, eventId);
	}

	~cannyfs_writer()
	{
		if (!global) waitingEvents.erase(eventId);
		if (!lock.owns_lock()) lock.lock();

		fileobj->processed.notify_all();
		if (generalwriter) delete generalwriter;
	}
};

atomic_llong eventId(0);

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

queue<function<int()> > workQueue;

int cannyfs_add_write(bool defer, function<int(int)> fun)
{
	long long eventIdNow = ++::eventId;
	if (!defer)
	{
		return fun(eventIdNow);
	}
	else
	{
		workQueue.emplace([eventIdNow, fun]() { return fun(eventIdNow); });
		return 0;
	}
}

int cannyfs_add_write(bool defer, std::string path, function<int(string)> fun)
{
	return cannyfs_add_write(defer, [path, fun](int eventId)->int {
		cannyfs_writer writer(path, LOCK_WHOLE, eventId);
		return fun(path);
	});
}

int cannyfs_add_write(bool defer, std::string path, fuse_file_info* origfi, function<int(string, const fuse_file_info*)> fun)
{
	fuse_file_info fi = *origfi;
	return cannyfs_add_write(defer, [path, fun, fi](int eventId)->int {
		cannyfs_writer writer(path, LOCK_WHOLE, eventId);
		return fun(path, &fi);
	});
}

int cannyfs_add_write(bool defer, std::string path1, std::string path2, function<int(string, string)> fun)
{
	return cannyfs_add_write(defer, [path1, path2, fun](int eventId)->int {
		cannyfs_writer writer1(path1, LOCK_WHOLE, eventId);
		cannyfs_writer writer2(path2, LOCK_WHOLE, eventId);

		return fun(path1, path2);
	});
}


static int cannyfs_getattr(const char *path, struct stat *stbuf)
{
	cannyfs_reader b(path, JUST_BARRIER);

	int res;

	res = lstat(path, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_fgetattr(const char *path, struct stat *stbuf,
			struct fuse_file_info *fi)
{
	cannyfs_reader b(path, JUST_BARRIER);

	int res;

	(void) path;

	res = fstat(fi->fh, stbuf);
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

	fi->fh = (unsigned long) d;
	return 0;
}

static inline struct cannyfs_dirp *get_dirp(struct fuse_file_info *fi)
{
	return (struct cannyfs_dirp *) (uintptr_t) fi->fh;
}

static int cannyfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi,
		       enum fuse_readdir_flags flags)
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
		enum fuse_fill_dir_flags fill_flags = 0;

		if (!d->entry) {
			d->entry = readdir(d->dp);
			if (!d->entry)
				break;
		}
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
			memset(&st, 0, sizeof(st));
			st.st_ino = d->entry->d_ino;
			st.st_mode = d->entry->d_type << 12;
		}
		nextoff = telldir(d->dp);
		if (filler(buf, d->entry->d_name, &st, nextoff, fill_flags))
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

static int cannyfs_rename(const char *from, const char *to, unsigned int flags)
{
	// TODO: I/O logic at rename, what will the names be
	cannyfs_reader b(from, LOCK_WHOLE);
	int res;

	/* When we have renameat2() in libc, then we can implement flags */
	if (flags)
		return -EINVAL;

	res = rename(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int cannyfs_link(const char *cfrom, const char *cto)
{
	return cannyfs_add_write(options.eagerlink, cfrom, cto, [](std::string from, std::string to) {
		int res;

		res = link(from.c_str(), to.c_str());
		if (res == -1)
			return -errno;

		return 0;
	});
}

static int cannyfs_chmod(const char *cpath, mode_t mode)
{
	return cannyfs_add_write(options.eagerchmod, cpath, [mode](std::string path) {
		int res;
		res = chmod(path.c_str(), mode);
		if (res == -1)
			return -errno;

		return 0;
	});
}

static int cannyfs_chown(const char *cpath, uid_t uid, gid_t gid)
{
	return cannyfs_add_write(options.eagerchown, cpath, [uid, gid](std::string path) {
		int res;

		res = lchown(path.c_str(), uid, gid);
		if (res == -1)
			return -errno;

		return 0;
	});
}

static int cannyfs_truncate(const char *path, off_t size)
{
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
	// TODO: FUN STUFF COULD BE DONE HERE TO AVOID WRITES
	int res;

	(void) path;

	res = ftruncate(fi->fh, size);
	if (res == -1)
		return -errno;

	return 0;
}

#ifdef HAVE_UTIMENSAT
static int cannyfs_utimens(const char *cpath, const struct timespec ts[2])
{
	return cannyfs_add_write(options.eagerutimens, cpath, [ts](std::string path) {
		int res;

		/* don't use utime/utimes since they follow symlinks */
		res = utimensat(0, path, ts, AT_SYMLINK_NOFOLLOW);
		if (res == -1)
			return -errno;

		return 0;
	});
}
#endif

static int cannyfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	int fd;

	fd = open(path, fi->flags, mode);
	if (fd == -1)
		return -errno;

	fi->fh = fd;
	return 0;
}

static int cannyfs_open(const char *path, struct fuse_file_info *fi)
{
	int fd;

	fd = open(path, fi->flags);
	if (fd == -1)
		return -errno;

	fi->fh = fd;
	return 0;
}

static int cannyfs_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	int res;

	(void) path;
	res = pread(fi->fh, buf, size, offset);
	if (res == -1)
		res = -errno;

	return res;
}

static int cannyfs_read_buf(const char *path, struct fuse_bufvec **bufp,
			size_t size, off_t offset, struct fuse_file_info *fi)
{
	struct fuse_bufvec *src;

	(void) path;

	src = new fuse_bufvec;
	if (src == NULL)
		return -ENOMEM;

	*src = FUSE_BUFVEC_INIT(size);

	src->buf[0].flags = (fuse_buf_flags) (FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
	src->buf[0].fd = fi->fh;
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
	res = pwrite(fi->fh, buf, size, offset);
	if (res == -1)
		res = -errno;

	return res;
}

static int cannyfs_write_buf(const char *path, struct fuse_bufvec *buf,
		     off_t offset, struct fuse_file_info *fi)
{
	int res = copy_to_pipe(buf);
	if (res) return res;


	struct fuse_bufvec dst = FUSE_BUFVEC_INIT(fuse_buf_size(buf));

	(void) path;

	dst.buf[0].flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK;
	dst.buf[0].fd = fi->fh;
	dst.buf[0].pos = offset;

	return fuse_buf_copy(&dst, buf, FUSE_BUF_SPLICE_NONBLOCK);
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
		closes.push_back(dup(getfh(fi)));
		return 0;
	}

	return cannyfs_add_write(options.eagerclose, cpath, fi, [](std::string path, const fuse_file_info *fi) {
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
		closes.push_back(getfh(fi));
	}

	return cannyfs_add_write(options.eagerclose, cpath, fi, [](std::string path, const fuse_file_info *fi) {
		return close(getfh(fi));
	});

	return 0;
}

static int cannyfs_fsync(const char *cpath, int isdatasync,
		     struct fuse_file_info *fi)
{
	if (options.ignorefsync) return 0;

	return cannyfs_add_write(options.eagerfsync, cpath, fi, [isdatasync](std::string path, const fuse_file_info *fi) {
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

	return cannyfs_add_write(options.eagerchown, cpath, fi, [mode, offset, length](std::string path, struct fuse_file_info *fi) {
		return -posix_fallocate(getfh(fi), offset, length);
	}
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int cannyfs_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
	cannyfs_immediatewriter b(path, JUST_BARRIER);

	int res = lsetxattr(path, name, value, size, flags);
	if (res == -1)
		return -errno;
	return 0;
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

static int cannyfs_removexattr(const char *path, const char *name)
{
	cannyfs_immediatewriter b(path, JUST_BARRIER);

	int res = lremovexattr(path, name);
	if (res == -1)
		return -errno;
	return 0;
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

static struct fuse_operations cannyfs_oper = {
	.getattr	= cannyfs_getattr,
	.fgetattr	= cannyfs_fgetattr,
	.access		= cannyfs_access,
	.readlink	= cannyfs_readlink,
	.opendir	= cannyfs_opendir,
	.readdir	= cannyfs_readdir,
	.releasedir	= cannyfs_releasedir,
	.mknod		= cannyfs_mknod,
	.mkdir		= cannyfs_mkdir,
	.symlink	= cannyfs_symlink,
	.unlink		= cannyfs_unlink,
	.rmdir		= cannyfs_rmdir,
	.rename		= cannyfs_rename,
	.link		= cannyfs_link,
	.chmod		= cannyfs_chmod,
	.chown		= cannyfs_chown,
	.truncate	= cannyfs_truncate,
	.ftruncate	= cannyfs_ftruncate,
#ifdef HAVE_UTIMENSAT
	.utimens	= cannyfs_utimens,
#endif
	.create		= cannyfs_create,
	.open		= cannyfs_open,
	.read		= cannyfs_read,
	.read_buf	= cannyfs_read_buf,
	.write		= cannyfs_write,
	.write_buf	= cannyfs_write_buf,
	.statfs		= cannyfs_statfs,
	.flush		= cannyfs_flush,
	.release	= cannyfs_release,
	.fsync		= cannyfs_fsync,
#ifdef HAVE_POSIX_FALLOCATE
	.fallocate	= cannyfs_fallocate,
#endif
#ifdef HAVE_SETXATTR
	.setxattr	= cannyfs_setxattr,
	.getxattr	= cannyfs_getxattr,
	.listxattr	= cannyfs_listxattr,
	.removexattr	= cannyfs_removexattr,
#endif
#ifdef HAVE_LIBULOCKMGR
	.lock		= cannyfs_lock,
#endif
	.flock		= cannyfs_flock,
};

int main(int argc, char *argv[])
{
	umask(0);
	return fuse_main(argc, argv, &cannyfs_oper, NULL);
}
