#!/bin/sh

# Check for the RUNNING_PID file and remove
# of the process isn't actually running
RUNNING_PID=$(find . -type f -name 'RUNNING_PID')
if [ $RUNNING_PID ]
then
  PROCID=`cat $RUNNING_PID`
  if [ ! -d "/proc/${PROCID}" ]
  then
    rm $RUNNING_PID
  fi
fi

SCRIPT=$(find . -type f -name address-reputation-ingester)
exec $SCRIPT $HMRC_CONFIG -Dconfig.file=conf/address-reputation-ingester.conf
