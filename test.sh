#!/bin/bash

exists()
{
	command -v "$1" >/dev/null 2>&1
}


if [ "$#" -lt 1 ]; then
	echo 'Wrong number of arguments. Usage:'
	echo 'sh test.sh <path-to-compiled-module>'
	exit 1;
fi

os=$(uname)

if [ "$os" = "Linux" ]; then ##Figure out how to know terminal name
	if  exists urxvt ; then
		terminal=$(echo urxvt)
	elif  exists x-terminal-emulator ; then
		terminal=$(echo x-terminal-emulator)
	else
		exit 1
	fi

	terminal=$(echo $terminal -e bash -c)
elif [ "$os" = "Darwin" ]; then
	terminal=$(echo open -a Terminal)
else
	exit 1
fi

modulePath=$(realpath $1)
originalPath=$(realpath .)

cd $1

#rmiregistry
echo "Lanching rmiregistry...."
eval $terminal "\"rmiregistry -J-Djava.rmi.server.codebase=file://$modulePath\" &"

sleep 1 #To be sure that the rmiregistry is running

#Servers 
echo "Launching server 1..."
eval $terminal "\"java server.Server 1.0 1 1 224.0.0.0 4445 224.0.0.1 4446 224.0.0.2 4447; read\" &"

echo "Launching server 2..."
eval $terminal "\"java server.Server 1.0 2 2 224.0.0.0 4445 224.0.0.1 4446 224.0.0.2 4447; read\" &"

echo "Launching server 3..."
eval $terminal "\"java server.Server 1.0 3 3 224.0.0.0 4445 224.0.0.1 4446 224.0.0.2 4447; read\" &"

wait

cd $originalPath
