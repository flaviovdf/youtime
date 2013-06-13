#!/bin/bash

findself() {
    SELF=`dirname $0`
}

findself

VODLIBS=/data/users/flavio/youtime/new-parser/vodlibs/src
PYTHONPATH=$PYTHONPATH:$SELF/src:$VODLIBS python $*
