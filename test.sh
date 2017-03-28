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
protocolVersion=$(echo 1.1)
mcAddr=$(echo 224.0.0.0)
mcPort=$(echo 4445)
mdbAddr=$(echo 224.0.0.1)
mdbPort=$(echo 4446)
mdrAddr=$(echo 224.0.0.2)
mdrPort=$(echo 4447)

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

# Launch Multicast Snooper
eval $terminal "\"java -jar McastSnooper.jar $mcAddr:$mcPort $mdbAddr:$mdbPort $mdrAddr:$mdrPort; read\" &"

cd $1

# Launch rmiregistry
echo "Lanching rmiregistry...."
eval $terminal "\"rmiregistry -J-Djava.rmi.server.codebase=file://$modulePath\" &"

sleep 1 #To be sure that the rmiregistry is running


# Launch Servers 
echo "Launching server 1..."
eval $terminal "\"java server.Server $protocolVersion 1 1 $mcAddr $mcPort $mdbAddr $mdbPort $mdrAddr $mdrPort; read\" &"

echo "Launching server 2..."
eval $terminal "\"java server.Server $protocolVersion 2 2 $mcAddr $mcPort $mdbAddr $mdbPort $mdrAddr $mdrPort; read\" &"

echo "Launching server 3..."
eval $terminal "\"java server.Server $protocolVersion 3 3 $mcAddr $mcPort $mdbAddr $mdbPort $mdrAddr $mdrPort; read\" &"

wait

cd $originalPath
