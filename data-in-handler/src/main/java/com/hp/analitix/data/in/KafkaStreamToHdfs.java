package com.hp.analitix.data.in;

import com.hp.analitix.utils.ConsoleUtils;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by golaniz on 17/02/2016.
 */
public class KafkaStreamToHdfs {
    public static final String APP_NAME = KafkaStreamToHdfs.class.getSimpleName();

    public static void main(String[] args) throws IOException, ParseException {
        CmdOpts cmdOpts = new CmdOpts(args);
        KafkaConsumerHelper helper = new KafkaConsumerHelper(cmdOpts);
        JavaStreamingContext jssc = null;



        why can't I read from the beginning????





        try {
            jssc = helper.createJavaStreamingContext(APP_NAME);
            JavaPairReceiverInputDStream<String, String> rs = helper.createReceiverStream(jssc);
            rs.foreach(new Function<JavaPairRDD<String, String>, Void>() {
                @Override
                public Void call(JavaPairRDD<String, String> pairs) throws Exception {
                    System.out.println("! !pairs=["+pairs.count()+"]");
                    pairs.foreach(new VoidFunction<Tuple2<String, String>>() {
                        @Override
                        public void call(Tuple2<String, String> t) throws Exception {
                            System.out.println("! !\t"+t);
                        }
                    });
                    return null;
                }
            });
//            rs.saveAsNewAPIHadoopFiles("/user/hadoop/logs/logs2", "txt", Integer.class, Integer.class, TextOutputFormat.class, conf);
            helper.startAndWait(jssc);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jssc != null) {
                ConsoleUtils.log("stopping...closing...");
                helper.stopAndClose(jssc);

                System.out.println("~~~~~~~~~~~~~~~~~~~~~~kafkaStream.saveAsHadoopFiles");

            }

            ConsoleUtils.log("~~ DONE ~~");
        }
    }

    private static Configuration createHadoopConfig() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
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
