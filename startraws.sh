#!/bin/bash
erl -sname raws_ingest -pa ebin deps/*/ebin -s raws_ingest $@

