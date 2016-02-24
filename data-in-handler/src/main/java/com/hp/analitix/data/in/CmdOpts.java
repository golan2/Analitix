package com.hp.analitix.data.in;

import org.apache.commons.cli.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by golaniz on 17/02/2016.
 */
public class CmdOpts implements Serializable {
    private static final int    NUM_THREADS                = 1;
    private static final String TOPIC_NAME                 = "test1";
    private static final String ZK_QUORUM                  = "myd-vm23458.hpswlabs.adapps.hp.com:2181";
    private static final String KAFKA_BROKERS_LIST         = "myd-vm23458.hpswlabs.adapps.hp.com";
    private static final String KAFKA_METADATA_BROKER_LIST = "myd-vm23458.hpswlabs.adapps.hp.com:9092";
    private static final long   KAFKA_OFFSET               = 0;
    private static final String CONSUMER_GROUP             = "group_2";
    private static final String SPARK_MASTER               = "local[*]";
    private static final String NO_FILE                    = "{console}";

    private final int         numberOfThreads;
    private final Set<String> topicNames;
    private final String      zkQuorum;
    private final String      kafkaBrokersList;
    private final long        kafkaOffset;
    private final String      consumerGroup;
    private final String      sparkMaster;
    private final String      outputFile;
    private final String      kafkaMetadataBrokerList;

    @SuppressWarnings("AccessStaticViaInstance")
    public CmdOpts(String[] args) throws ParseException {
        Options o = new Options();
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("numberOfThreads"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("topicNames"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("zkQuorum"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("kafkaBrokersList"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("kafkaMetadataBrokerList"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("kafkaOffset"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("consumerGroup"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("sparkMaster"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("outputFile"));
        CommandLineParser parser = new BasicParser();
        CommandLine line = parser.parse(o, args);

        this.numberOfThreads = Integer.parseInt(line.getOptionValue("numberOfThreads", String.valueOf(NUM_THREADS)));
        this.topicNames = new HashSet<>(Arrays.asList(line.getOptionValue("topicNames", TOPIC_NAME).split(",")).stream().map(String::trim).collect(Collectors.toList()));
        this.zkQuorum = line.getOptionValue("zkQuorum", ZK_QUORUM);
        this.kafkaBrokersList = line.getOptionValue("kafkaBrokersList", KAFKA_BROKERS_LIST);
        this.kafkaMetadataBrokerList = line.getOptionValue("kafkaMetadataBrokerList", KAFKA_METADATA_BROKER_LIST);
        this.kafkaOffset = Integer.parseInt(line.getOptionValue("kafkaOffset", String.valueOf(KAFKA_OFFSET)));
        this.consumerGroup = line.getOptionValue("consumerGroup", CONSUMER_GROUP);
        this.sparkMaster = line.getOptionValue("sparkMaster", SPARK_MASTER);
        this.outputFile = line.getOptionValue("outputFile", NO_FILE);
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public Set<String> getTopicNames() {
        return topicNames;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public String getKafkaBrokersList() {
        return kafkaBrokersList;
    }

    //including the ports (myd-vm23458.hpswlabs.adapps.hp.com:9092)
    public String getKafkaMetadataBrokerList() {
        return kafkaMetadataBrokerList;
    }

    public long getKafkaOffset() {
        return kafkaOffset;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }
}
