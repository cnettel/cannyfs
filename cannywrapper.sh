#!/bin/bash
ulimit -u 10000

CANNY_PATH=`mktemp -t cannywrapper.XXXXXXXXXX || exit 1`
cannyfs -f -o big_writes -o max_write=65536 $CANNY_PATH &
chroot $CANNY_PATH bash -c "cd $PWD && $*"

kill %cannyfs
wait %cannyfs

rmdir $CANNY_PATH
