#!/bin/bash

for i in $(cat usp_urls.txt)
do

PRAGMAS="Pragma: akamai-x-get-cache-key, akamai-x-cache-on, akamai-x-cache-remote-on, akamai-x-get-true-cache-key, akamai-x-check-cacheable, akamai-x-get-request-id, akamai-x-serial-no, akamai-x-get-ssl-client-session-id, X-Akamai-CacheTrack, akamai-x-get-client-ip, akamai-x-feo-trace, akamai-x-tapioca-trace , akamai-x-get-extracted-values"

RESPONSE_CODE=$(curl -k -S -s -D - -o /dev/null  -H "$PRAGMAS" "$i"  | egrep "^HTTP" | awk '{print $2}')


echo " $i $RESPONSE_CODE"

done
