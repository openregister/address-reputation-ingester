#!/bin/bash -e
# this is the command executed in CI but without 'validate'
sbt clean scalastyle coverage test it:test coverageOff dist-tgz
