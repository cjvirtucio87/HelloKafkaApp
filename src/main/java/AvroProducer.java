import java.util.Properties;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by cvirtucio on 4/11/2017.
 */
public class AvroProducer {
    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\" }"
            + "]}";

    public static void main(String[] args) throws Exception {
        // create producer
        KafkaProducer<String, byte[]> producer = AvroProducer.createProducer();

        // parse the json string into an Avro schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        // setup Injection function that transforms each GenericRecord in schema to a byte[]
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        AvroProducer.createStream(schema, recordInjection, producer);
    }

    private static void createStream(Schema schema, Injection<GenericRecord, byte[]> injection, KafkaProducer producer) throws Exception {
        for (int i = 0; i < 1000; i++) {
            GenericData.Record avroRecord = AvroProducer.createRecord(schema, i);

            // apply the function
            byte[] bytes = injection.apply(avroRecord);

            // stream the data to the kafka server
            AvroProducer.produce(bytes, producer);

            Thread.sleep(250);
        }
    }

    private static Properties createProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        return props;
    }

    private static GenericData.Record createRecord(Schema schema, int index) {
        // create a record
        GenericData.Record record = new GenericData.Record(schema);
        record.put("str1", "My first string [INDEX: " + index + "]");
        record.put("str2", "My second string [INDEX: " + index + "]");
        record.put("int1", index);

        return record;
    }

    private static void produce(byte[] bytes, KafkaProducer producer) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>("some-new-topic", bytes);
        producer.send(record);
    }

    private static KafkaProducer createProducer() {
        // setup properties for Kafka Producer
        Properties props = AvroProducer.createProps();
        return new KafkaProducer(props);
    }
}
