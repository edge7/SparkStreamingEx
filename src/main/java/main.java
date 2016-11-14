import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class main{

    public static void main(String [] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        final String master = "local[*]";
        final String appName = "Spark&Kafka";
        // Creating Spark streaming context
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        ssc.checkpoint(".");
        // Getting spark Context and loading list of users as an rdd
        // reading from local file
        JavaSparkContext sc = ssc.sparkContext();
        JavaRDD<String> rawUsers = sc.textFile("file:///home/edge7/Desktop/SparkEx/Data/data");
        JavaRDD<String> usersName = rawUsers.map(x -> x.split(",")[0]);
        JavaPairRDD users = rawUsers.mapToPair(x -> new Tuple2(x.split(",")[0], Integer.parseInt(x.split(",")[1])));
        Map map = users.collectAsMap();

        // Printing users
        map.forEach((k, v) -> System.out.println(k + " " + v));

        
        // Init Kafka Connector
        List<String> topics = Arrays.asList("spark");
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "example");
        JavaInputDStream<ConsumerRecord<Object, Object>> directStream;
        directStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));
        JavaPairDStream<String, Integer> received = directStream.mapToPair(x -> new Tuple2<String, Integer>((String) x.value(), 1)).reduceByKey((i1,i2) -> i1+i2);
        //received.print();
        JavaPairDStream<String, Integer> join = received.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
                                                                             @Override
                                                                             public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> input) {
                                                                                 return input.join(users).mapValues(new Function<Tuple2<Integer, Integer>, Integer>() {
                                                                                                                        @Override
                                                                                                                        public Integer call(Tuple2<Integer, Integer> t) throws Exception {
                                                                                                                            return t._1 + t._2;
                                                                                                                        }
                                                                                                                    }

                                                                                 );
                                                                             }
                                                                         });

        //join.print();

        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>) (word, one, state) -> {
                    int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                    Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                    state.update(sum);
                    return output;
                };
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
                received.mapWithState(StateSpec.function(mappingFunc).initialState(users));
        stateDstream.print();
        ssc.start();
        ssc.awaitTermination();
    }

}