#!/bin/bash

src/Cloud9/etc/hadoop-cluster.sh edu.umd.cloud9.example.simple.DemoWordCount -input bible+shakes.nopunc.gz -output acstrick -numReducers 5
