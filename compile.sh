#!/bin/bash
mkdir bin &>/dev/null
javac -sourcepath src/ -d bin/ src/client/TestApp.java src/server/Server.java
