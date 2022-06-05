#!/bin/bash 

protoc -I=./proto/ --python_out=./ ./proto/*.proto