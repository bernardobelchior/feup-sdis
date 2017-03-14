#!/bin/bash

if [ "$#" -lt 2 ]; then
	echo 'Wrong number of arguments. Usage:'
	echo 'sh test.sh <path-to-compiled-module> <operation> <operand1> <operand2>'
	exit 1;
fi

curPath=$(realpath $1)

#rmiregistry
urxvt -e "rmiregistry -J-Djava.rmi.server.codebase=file://$curPath" &

#Servers 
urxvt -e "java server.Server 1.0 1 1 224.0.0.0 4445 224.0.0.1 4446 224.0.0.2 4447" &
urxvt -e "java server.Server 1.0 2 2 224.0.0.0 4445 224.0.0.1 4446 224.0.0.2 4447" &

#Client
urxvt -e "java client.TestApp 1 $2 $3 $4" &

wait
