HadoopMongo
===========
This is test project which use Hadoop, MongoDB and MapReduce

## configure linux security limit(for ubuntu 12.04):
Set up limit of open files. For it add lines to file _/etc/security/limits.conf_

    * soft nofile 64000
    * hard nofile 64000

[more details about linux limit for mongodb](http://docs.mongodb.org/manual/reference/ulimit/)

##mongod servers:

1. 192.168.1.30:20001 (DM)
2. 192.168.1.34:20001 (DV)

## mongo shard server

    mkdir ~/mongo_data/shard_srv -p

     mongod --shardsvr --dbpath ~/mongo_data/shard_srv --port 20001 --logpath ~/mongo_data/log-shard-srv.log --fork

## start one configuration server on DM machine:

    mkdir ~/mongo_data/config_srv -p

    mongod --configsvr --dbpath /home/dzmitry/mongo_data/config_srv --logpath /home/dzmitry/mongo_data/log-config-srv.log --port 30001  --fork


## start mongos server


    mongos --configdb 192.168.1.30:30001 --logpath ~/mongo_data/mongos.log --port 40001 --fork


## configure config server by mongos(only first time)

    mongo 192.168.1.30:40001

    mongos> sh.addShard("192.168.1.30:20001")
    mongos> sh.addShard("192.168.1.34:20001")

#### check addging shard servert to sharding cluster

    db.getSiblingDB("config").shards.find()

#### enabling sharding for database _test_

    sh.enableSharding("test")

#### check adding db to sharding

    db.getSiblingDB("config").databases.find()

#### add sharding collecition _test.in_

    sh.shardCollection("test.in", {country: 1, _id: 1})

#### check sharding collection

    db.getSiblingDB("config").collections.find()


