# Distributed Systems

## [Distributed Local Area Network Backup System](https://web.fe.up.pt/~pfs/aulas/sd2017/projs/proj1/proj1.html)
A distributed backup service for a local area network (LAN). The idea is to use the free disk space of the computers in a LAN for backing up files in other computers in the same LAN. The service is provided by servers in an environment that is assumed cooperative (rather than hostile). Nevertheless, each server retains control over its own disks and, if needed, may reclaim the space it made available for backing up other computers' files.

### Running

In order to start the `rmiregistry`, use the following command:
```
$ rmiregistry -J-Djava.rmi.server.codebase=file:///<path-to-module>/
```
Where `<path-to-module>` represents the absolute path to the compiled module.

This is a security measure introduced in Java 7, as seen [here](http://docs.oracle.com/javase/7/docs/technotes/guides/rmi/enhancements-7.html).


To start a peer, use the following command as an example:
```
$ java server.Server 1.0 1 1 224.0.0.0 4445 224.0.0.1 4446 224.0.0.2 4447
```

To start the TestApp, use the following command as an example:
```
$ java client.TestApp 1 BACKUP client/TestApp.class 3
```
