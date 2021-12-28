## USAGE
- This repo is used for removes redundant data from Kafka Connect to get a light message to put it to HDFS
- Example on how to add to your connector:
```
"transforms":"TransformRecord",
"transforms.TransformRecord.type":"com.vtp.datalake.kafka.connect.smt.TransformRecord$Value",
"transforms.TransformRecord.op":"op",
"transforms.TransformRecord.ts_ms":"ts_ms"
```



- Kafka Connect SMT to Transform Record from

```
{
    "schema": {
        "type": "struct",
        "fields": [{
            "type": "struct",
            "fields": [{
                "type": "int32",
                "optional": false,
                "field": "person_id"
            }...],
            "optional": true,
            "name": "mygotest.test123.persons.Value",
            "field": "before"
        }, {
            "type": "struct",
            "fields": [{
                "type": "int32",
                "optional": false,
                "field": "person_id"
            }, ...],
            "optional": true,
            "name": "mygotest.test123.persons.Value",
            "field": "after"
        }, ...],
        "optional": false,
        "name": "mygotest.test123.persons.Envelope"
    },
    "payload": {
        "before": null,
        "after": {
            "person_id": 15,
            "full_name": "Nguyen Ba Son 15",
            "city": "Ha Noi",
            "country": "Viet Nam"
        },
        "source": {
            "version": "1.8.0.Final",
            "connector": "mysql",
            "name": "mygotest",
            "ts_ms": 1640638401000,
            "snapshot": "false",
            "db": "test123",
            "sequence": null,
            "table": "persons",
            "server_id": 223344,
            "gtid": null,
            "file": "mysql-bin.000003",
            "pos": 7091,
            "row": 0,
            "thread": null,
            "query": null
        },
        "op": "c",
        "ts_ms": 1640638401255,
        "transaction": null,
        "uuid": "55233481-682f-47ca-9b75-a77383ea3c6f"
    }
}
```
to 
```
 {
    "schema": {
        "type": "struct",
        "fields": [{
            "type": "int32",
            "optional": false,
            "field": "person_id"
        }, {
            "type": "string",
            "optional": false,
            "field": "full_name"
        }, {
            "type": "string",
            "optional": false,
            "field": "city"
        }, {
            "type": "string",
            "optional": false,
            "field": "country"
        }, {
            "type": "string",
            "optional": false,
            "field": "op"
        }, {
            "type": "int64",
            "optional": false,
            "field": "ts_ms"
        }],
        "optional": false,
        "name": "mygotest.test123.persons.Value"
    },
    "payload": {
        "person_id": 16,
        "full_name": "Nguyen Ba Son 16",
        "city": "Ha Noi",
        "country": "Viet Nam",
        "op": "c",
        "ts_ms": 1640680054988
    }
}
```

##Run

```
mvn install 
```

- copy file .jar to kafka and config path in plugin.path=/

##Ref 
- [confluentinc/kafka-connect-insert-uuid](https://github.com/confluentinc/kafka-connect-insert-uuid)