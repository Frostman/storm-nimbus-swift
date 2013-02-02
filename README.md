# storm-nimbus-swift

This library provides implementation for Nimbus Storage API that stores data in OpenStack Swift.

## Installation

Copy `storm-nimbus-swift.jar` to `/lib` folder on each Nimbus node.

## Configuration

You should the following properties to `storm.yaml`:

```
nimbus.storage: "storm.SwiftNimbusStorage"
nimbus.storage.swift.auth: "http://openstack:5000/v2.0/" # keystone address
nimbus.storage.swift.identity: "demo:admin"              # tenant:username
nimbus.storage.swift.password: "swordfish"
nimbus.storage.swift.container: "storm-ha"
nimbus.storage.swift.timeout: "10000"                    # in ms
```

## Check

On start, Nimbus will write some info about used storage to logs.

## Maven / Lein

https://clojars.org/storm-nimbus-swift

## License

Apache License Version 2.0

