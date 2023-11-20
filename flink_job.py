from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.datastream.execution_mode import ExecutionMode
from pyflink.table.descriptors import Kafka
from pyflink.table import StreamTableDescriptor

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    # Kafka source configuration
    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("input-topic") \
        .set_group_id("my-group") \
        .set_starting_offsets("earliest") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Define the transformation (e.g., word count)
    result = env.from_source(source, watermark_strategy=None, source_name="Kafka Source") \
        .flat_map(lambda x: x.split()) \
        .map(lambda x: (x, 1), output_type=Types.ROW([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda x, y: (x[0], x[1] + y[1]))

    # Kafka sink configuration
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("output-topic")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    result.sink_to(sink)

    # Execute the job
    env.execute("Kafka Source and Sink Example")

if __name__ == "__main__":
    main()