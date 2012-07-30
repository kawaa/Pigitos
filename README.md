# Pigitos

## About
Pigitos is a set of tiny, but highly useful Java UDFs for Apache Pig.

## Contents

### UDFs for manipulating maps
Pigitos provides UDFs to manipulate maps such as calculating the size of the map or retrieving keys (or values, or key/value pairs) as a bag. Such UDFs are very useful when working with dynamically created column qualifiers (that hold some meaningful information that you want to process) in Apache HBase tables.

It seems that there is no such UDFs in Apache Pig itself or Piggybank library. I have found only UDFs like TOBAG or TOTUPLE, but they do not take a map as an input parameter.

Currently, it contains following UDFs:
* MapSize – takes a map and returns the number of entries in the map
* MapKeysToTuple – takes a map and produces a bag that contains all keys from that map
* MapValuesToTuple -takes a map and produces a bag that contains all values from that map
* MapEntriesToBag – takes a map and produces a bag that contains tuples, where each tuple consists of two field: key and value (each tuple corresponds to one key/value pair from a map)

Here is a quick example:
```
User = LOAD 'hbase://user' USING HBaseStorage('friend:*', '-loadKey true') 
  AS (username:chararray, friendMap:map[]);
UserFriend = FOREACH User
  GENERATE username, FLATTEN(MapKeysToBag(friendsMap)) AS friendUsername;
```

## License
Apache licensed.
