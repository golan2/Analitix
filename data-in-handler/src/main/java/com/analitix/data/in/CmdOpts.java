package com.analitix.data.in;

import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by golaniz on 24/02/2016.
 */
public class CmdOpts {
    private final Map<String, String> arguments;

    public CmdOpts(String[] args, Map<String, String> params, Set<String> mandatory) throws ParseException {
        Options o = new Options();
        for (String key : params.keySet()) {
            o.addOption(OptionBuilder.hasArgs(1).isRequired(mandatory.contains(key)).create(key));
        }
        CommandLineParser parser = new BasicParser();
        CommandLine line = parser.parse(o, args);

        arguments = new HashMap<>(params.size());
        for (String key : params.keySet()) {
            String value = line.getOptionValue(key, params.get(key));
            arguments.put(key,value);
        }
    }

    public String get(String name) {
        return arguments.get(name);
    }


//    private static final int    NUM_THREADS                = 1;
//    private static final String KAFKA_TOPIC                = "test1";
//    private static final String ZK_QUORUM                  = "myd-vm23458.hpswlabs.adapps.hp.com:2181";
//    private static final String KAFKA_BROKERS_LIST         = "myd-vm23458.hpswlabs.adapps.hp.com";
//    private static final String KAFKA_METADATA_BROKER_LIST = "myd-vm23458.hpswlabs.adapps.hp.com:9092";
//    private static final long   KAFKA_OFFSET               = 0;
//    private static final String CONSUMER_GROUP             = "group_2";
//    private static final String SPARK_MASTER               = "local[*]";
//    private static final String NO_FILE                    = "{console}";
//    private static final String HDFS_OUTPUT_PATH           = "/datasets_izik/irisLibSVM.data";

}
