#!/bin/bash

# Let's use lots of files
ulimit -u 10000

CANNY_PATH=`mktemp -t cannywrapper.XXXXXXXXXX || exit 1`
cannyfs -f -o big_writes -o max_write=65536 $CANNY_PATH &
until mountpoint -q $CANNY_PATH; do sleep 1 && echo "Waiting for mountpoint"; done

chroot $CANNY_PATH bash -c "cd $PWD && $*"

kill %cannyfs
wait %cannyfs

rmdir $CANNY_PATH
