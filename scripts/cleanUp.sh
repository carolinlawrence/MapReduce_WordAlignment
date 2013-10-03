#!/bin/sh

hadoop fs -rm -R iterations/*
hadoop fs -rm -R iterations
hadoop fs -rm input/*
hadoop fs -rm -R input
rm -rf ../tmp/
