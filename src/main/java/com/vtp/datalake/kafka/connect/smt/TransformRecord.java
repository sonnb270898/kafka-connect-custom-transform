package com.vtp.datalake.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class TransformRecord<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final String DELETE_OP = "DELETE";
  private static final String BEFORE = "before";
  private static final String AFTER = "after";
  private static final String OP = "op";
  private static final String TS_MS = "ts_ms";

  public static final String OVERVIEW_DOC =  "Transform Kafka";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(OP, ConfigDef.Type.STRING, "op", ConfigDef.Importance.HIGH,
      "Field name for OP")
    .define(TS_MS, ConfigDef.Type.STRING, "ts_ms", ConfigDef.Importance.HIGH,
            "Field name for TS_MS");

  private static final String PURPOSE = "TransformRecord";

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }


  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

    final Map<String, Object> updatedValue = new HashMap<>(value);

    return newRecord(record, null, updatedValue);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

    Struct newValue = null;

    if(value.get(OP).equals("d") || value.get(OP).equals(DELETE_OP)){
      newValue =  ((Struct) value.get(BEFORE));
    }
    else {
      newValue =  ((Struct) value.get(AFTER));
    }

    Schema updatedSchema = schemaUpdateCache.get(newValue.schema());

    if(updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(newValue.schema());
      schemaUpdateCache.put(newValue.schema(), updatedSchema);
    }

    final Struct updatedValue = new Struct(updatedSchema);

    for (Field field : newValue.schema().fields()) {
      updatedValue.put(field.name(), newValue.get(field));
    }
    updatedValue.put(OP, value.get(OP));
    updatedValue.put(TS_MS, value.get(TS_MS));

    return newRecord(record, updatedSchema, updatedValue);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }


  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field: schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field(OP, Schema.STRING_SCHEMA);
    builder.field(TS_MS, Schema.INT64_SCHEMA);

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends TransformRecord<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends TransformRecord<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


