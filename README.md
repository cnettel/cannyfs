# CannyFS

CannyFS is a shim file system. That is, it mirrors an existing file system. This is implemented using FUSE, and the source code is a heavily
modified fork of the https://github.com/libfuse/libfuse example https://github.com/libfuse/libfuse/blob/master/example/passthrough_fh.c (which used to be called fusexmp_fh.c).

What makes CannyFS different is that almost any write operation is completed asynchronously, while the code itself reports back
to the caller that the task completed. This means that the calling process can proceed doing useful work. This is especially important
if your I/O subsystem has high latency (due to lots of I/O or being hosted somewhere else within a network), and/or if your process
makes a very high number of I/O requests, with a lot of flushing or writing to different files. Examples of this can be tasks that walk over
complete directory trees, touching or writing to every file within them.

## Intended usage mode

1. Mount a directory with CannyFS, in non-demon mode to easily see error messages written to stderr.
2. Do your work.
3. Kill the CannyFS process.
4. Check that the CannyFS process gracefully reports no errors.

## Compiling cannyfs
The packages tbb, boost, and fuse are needed, beyond what's typically available in any Linux distro. In e.g. Ubuntu 16.04, this can be enough to get you going:

```bash
apt-get install libtbb-dev libfuse-dev libboost-dev
```

Then compile using at the very least a g++ compiler from the 5.x tree, 6.x highly recommended (C++ 14 support is needed).

```bash
g++ cannyfs.cpp -std=c++14 -O3 -lfuse -ltbb -lpthread -lboost_filesystem -lboost_system -D_FILE_OFFSET_BITS=64 -o cannyfs
```

## Example script
The following script will create a mount that mirrors your local dir, with settings that are suitable for a Linux system
(where default pipe buffers are typically 65536 bytes in length). The zip file archive.zip contains loads of small files and thus takes
quite long to extract, especially if you are doing this over an NFS or CIFS mount. By mounting it in CannyFS, unzip can enqueue I/O operations
to several target files, rather than performing a blocking wait for completion for each file.

cannyfs will need to be in your path, if it's found locally, adjust the command and kill jobspec to ./cannyfs

```bash
#!/usr/bin/bash
mkdir mountpoint
cannyfs -f -o big_writes -o max_write=65536 -omodules=subdir,subdir=$HOME mountpoint &
# More correct way is to check whether the mounting point exists
sleep 5
cd mountpoint
unzip $HOME/archive.zip
kill %cannyfs
rmdir mountpoint
```
