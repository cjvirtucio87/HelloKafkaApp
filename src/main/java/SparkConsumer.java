
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

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
        kafkaParams.put("metadata.broker.list", "localhost:9090");
        // collection of topics; only have one topic in the collection for now
        Set<String> topics = Collections.singleton("some-new-topic");

        return KafkaUtils.createDirectStream(
                ssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );
    }

    private static void process(JavaPairInputDStream dStream) {
        VoidFunction<?> onEach = new VoidFunction<JavaPairRDD<String, String>>() {
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                System.out.println("--- New RDD with " + rdd.partitions().size() + " partitions and " + rdd.count() + " records ");
            }
        };

        dStream.foreachRDD(onEach);
    }

}
