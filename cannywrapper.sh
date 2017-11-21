#!/bin/bash

# Usage: cannywrapper.sh iothrashingcommand
# The command will be run within a chroot where all I/O accesses really go through CannyFS.
# This is rather coarse-grained, but perfectly fine for e.g. large rsyncs.
# (Assuming you are aware of the caveats in trying to resume failed "cannied" rsyncs).
#
# With fakechroot ( https://github.com/dex4er/fakechroot ), the only system administrator/
# root action needed is to allow FUSE mounting.


# Let's use lots of files (actual files as well as kernel pipes)
ulimit -u 10000

CANNY_PATH=`mktemp -d -t cannywrapper.XXXXXXXXXX || exit 1`
echo Temporary mount in $CANNY_PATH
./cannyfs -f -o big_writes -o max_write=65536 $CANNY_PATH &
until mountpoint -q $CANNY_PATH; do sleep 1 && echo "Waiting for mountpoint"; done

# Since we trust ourselves and use chroot for convenience, fakechroot is fine
# i.e. call the full script using fakechroot cannywrapper.sh
chroot $CANNY_PATH bash -c "cd $PWD && $*"

kill %./cannyfs
wait %./cannyfs

rmdir $CANNY_PATH
