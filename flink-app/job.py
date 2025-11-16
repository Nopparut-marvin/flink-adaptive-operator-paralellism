from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, DeliveryGuarantee
from pyflink.common import WatermarkStrategy
from pyflink.common import Configuration

import json

INPUT_TOPIC = "input-topic"
OUTPUT_TOPIC = "output-topic"
BOOTSTRAP = "kafka1:19092,kafka2:19092,kafka3:19092"

def main():
    config = Configuration()
    print("✅ Configuring pipeline.jars")
    config.set_string("pipeline.jars", "file:///opt/flink/usrlib/flink-sql-connector-kafka-1.18.1.jar")
    print("✅ Pipeline jars: ", config.get_string("pipeline.jars", ""))
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(2)

    # Source: Kafka (ข้อความ JSON แบบบรรทัดละหนึ่ง obj)
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id("pyflink-demo-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

    # Transform: parse JSON แล้ว map เพิ่มฟิลด์
    def enrich(line: str):
        try:
            obj = json.loads(line)
        except Exception:
            return json.dumps({"_error": True, "raw": line})
        # ตัวอย่างแปลง: เพิ่ม field sum = a + b หากมี
        a = obj.get("a", 0)
        b = obj.get("b", 0)
        obj["sum"] = a + b
        return json.dumps(obj)

    out = ds.map(enrich, output_type=Types.STRING())

    # Sink: Kafka
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(SimpleStringSchema())
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_topic(OUTPUT_TOPIC)
        .build()
    )

    out.sink_to(sink)

    env.execute("pyflink-kafka-example")

if __name__ == "__main__":
    main()
