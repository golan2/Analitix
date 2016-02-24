
package com.analitix.data.in;

import com.analitix.utils.ConsoleUtils;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by golaniz on 17/02/2016.
 */
public class KafkaStreamToHdfs {
    private static final String APP_NAME = KafkaStreamToHdfs.class.getSimpleName();
    private static final String CLA_KAFKA_TOPIC       = "kafkaTopic";
    private static final String CLA_SPARK_MASTER      = "sparkMaster";
    private static final String CLA_NUMBER_OF_THREADS = "numberOfThreads";
    private static final String CLA_ZK_QUORUM         = "zkQuorum";
    private static final String CLA_CONSUMER_GROUP    = "consumerGroup";

    public static void main(String[] args) throws IOException, ParseException {
//        CmdOpts cmdOpts = new CmdOpts(args);
        CmdOpts cmdOpts = new CmdOpts(args, getParamsMap(), Collections.emptySet());
        JavaStreamingContext jssc = null;

//        why can't I read from the beginning????

        try {
            SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster(cmdOpts.get(CLA_SPARK_MASTER));
            jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

            Map<String, Integer> topicMap = new HashMap<>();
            topicMap.put(cmdOpts.get(CLA_KAFKA_TOPIC), Integer.valueOf(cmdOpts.get(CLA_NUMBER_OF_THREADS)));


            JavaPairReceiverInputDStream<String, String> pairs = KafkaUtils.createStream(jssc, cmdOpts.get(CLA_ZK_QUORUM), cmdOpts.get(CLA_CONSUMER_GROUP), topicMap);
            pairs.foreach(new Function<JavaPairRDD<String, String>, Void>() {
                @Override
                public Void call(JavaPairRDD<String, String> pair) throws Exception {
                    pair.saveAsNewAPIHadoopDataset(createHadoopConfig());
                    return null;
                }
            });


//           TODO: pairs.saveAsNewAPIHadoopFiles("/user/hadoop/logs/logs2", "txt", Integer.class, Integer.class, TextOutputFormat.class, conf);

            jssc.start();
            int read = 0;
            while (read!='Q') {
                read = System.in.read();
            }



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jssc != null) {
                ConsoleUtils.log("stopping...closing...");
                jssc.stop();
                jssc.close();
            }

            ConsoleUtils.log("~~ DONE ~~");
        }
    }

    private static Map<String, String> getParamsMap() {
        HashMap<String, String> result = new HashMap<>();
        result.put(CLA_SPARK_MASTER, "local[*]");
        result.put(CLA_KAFKA_TOPIC, "iris");
        result.put(CLA_NUMBER_OF_THREADS, "1");
        result.put(CLA_ZK_QUORUM, "myd-vm23458.hpswlabs.adapps.hp.com:2181");
        result.put(CLA_CONSUMER_GROUP, "ConsumerGroup_Izik");
        return result;
    }

    private static Configuration createHadoopConfig() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://http://myd-vm22661.hpswlabs.adapps.hp.com:9000");
        conf.set("dfs.replication", "1");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return conf;
    }

    public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
        public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) {
            return new Tuple2(new Text(record._1()), new IntWritable(record._2()));
        }
    }

    private static void lowLevelConsumerApi() {
    }
}
