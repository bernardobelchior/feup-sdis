#!/bin/bash

exists()
{
	command -v "$1" >/dev/null 2>&1
}

launch_server() {
	id=$(echo $1)
	echo "Launching server $id..."
	eval $terminal "\"java server.Server $protocolVersion $id $id $mcAddr $mcPort $mdbAddr $mdbPort $mdrAddr $mdrPort; bash\" &"
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

	modulePath=$(realpath $1)
	originalPath=$(realpath .)
	terminal=$(echo $terminal -e bash -c)
elif [ "$os" = "Darwin" ]; then
	terminal=$(echo open -a Terminal.app)
	originalPath=$(pwd)
	modulePath=$(echo $originalPath/$1)
else
	exit 1
fi


# Launch Multicast Snooper
eval $terminal "\"java -jar McastSnooper.jar $mcAddr:$mcPort $mdbAddr:$mdbPort $mdrAddr:$mdrPort; read\" &"

cd $1

# Launch rmiregistry
echo "Lanching rmiregistry...."
eval $terminal "\"rmiregistry -J-Djava.rmi.server.codebase=file://$modulePath\" &"

sleep 1 #To be sure that the rmiregistry is running

# Launch Servers 
launch_server "1"
launch_server "2"
launch_server "3"
launch_server "4"

wait

cd $originalPath
