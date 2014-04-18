#!/bin/bash
erl -sname raws_ingest -kernel error_logger '{file,"log/syserr.log"}' -pa ebin deps/*/ebin -s raws_ingest $@

