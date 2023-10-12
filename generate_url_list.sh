#!/bin/bash

# Assuming your input is in 'input.txt'
sed -n '/\.mp4\"$/{
        s/\"//g
        s/_[^_]*\.mp4$/_,4500k,2500k,1000k,750k,400k,.mp4.csmil\/master.m3u8/p
}' linodePathJson.txt > url-list.txt
