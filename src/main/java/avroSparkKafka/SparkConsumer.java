package avroSparkKafka;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by VirtucioC on 4/10/2017.
 */

public class SparkConsumer {
    public static Logger log = Logger.getLogger(SparkConsumer.class);

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("hello-kafka")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        JavaPairInputDStream dStream = SparkConsumer.createDStream(ssc);
        SparkConsumer.process(dStream);

        ssc.start();
        ssc.awaitTermination();
    }

    private static  JavaPairInputDStream createDStream(JavaStreamingContext ssc) {
        // kafkaParams: properties of the stream to be consumed
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        // collection of topics; only have one topic in the collection for now
        Set<String> topics = Collections.singleton("some-new-topic");

        return KafkaUtils.createDirectStream(
                ssc,
                String.class,
                byte[].class,
                StringDecoder.class,
                DefaultDecoder.class,
                kafkaParams,
                topics
        );
    }

    private static void process(JavaPairInputDStream dStream) {
        // callback for each record in RDD
        final VoidFunction onEachRdd = new VoidFunction<Tuple2<String, byte[]>>() {
            public void call(Tuple2<String, byte[]> avroRecord) throws Exception {
                Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(AvroProducer.USER_SCHEMA);

                // setup Injection function that transforms each byte[] to a Generic Record
                Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                GenericRecord record = recordInjection.invert(avroRecord._2).get();

                System.out.println("str1= " + record.get("str1")
                        + ", str2= " + record.get("str2")
                        + ", int1=" + record.get("int1"));
            }
        };

        // callback for each RDD in stream
        final VoidFunction onEach = new VoidFunction<JavaPairRDD<String, byte[]>>() {
            public void call(JavaPairRDD<String, byte[]> rdd) throws Exception {
                rdd.foreach(onEachRdd);
            }
        };

        dStream.foreachRDD(onEach);
    }

}
