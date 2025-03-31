/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicLeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda.AwsConstants;
import com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda.EddaClient;
import com.linkedin.kafka.cruisecontrol.analyzer.netflix.kaas.BrokerInfo;
import com.linkedin.kafka.cruisecontrol.analyzer.netflix.kaas.KaasControllerClient;
import com.linkedin.kafka.cruisecontrol.analyzer.netflix.kaas.TopicInfo;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;

@RunWith(JUnit4.class)
public class RandomClusterLinearDistNewBrokerSingleTest {

    public static Collection<Object[]> data() {
        return RandomClusterTest.data(TestConstants.Distribution.LINEAR);
    }

    private String makeNewTopic(String key) {
        return "T" + key;
    }

    private Replica createReplica(ClusterModel cluster, int destBrokerId, Replica replica, int i) {
        Broker destBroker = cluster.broker(destBrokerId);
        TopicPartition tp = new TopicPartition(makeNewTopic(replica.topicPartition().topic()), replica.topicPartition().partition());
        System.out.printf("Creating new %s replica for %s on %s.%n", i == 0 ? "leader" : "follow", tp, destBrokerId);
        Replica newReplica = cluster.createReplica(destBroker.rack().id(), destBroker.id(), tp, i, i == 0);
        cluster.setReplicaLoad(destBroker.rack().id(), destBrokerId, tp, replica.load().loadByWindows(), Collections.singletonList(1L));
        cluster.removeReplica(replica.broker().id(), replica.topicPartition());
        return newReplica;
    }

    private void displayClusterInfo(ClusterModel cluster, Set<String> topics) {
        cluster.brokers().forEach(b -> {
            final String replicasStr = b.replicas().stream()
                    .filter(r -> !r.isLeader())
                    .filter(r -> topics.contains(r.topicPartition().topic()))
                    .map(r -> r.topicPartition().topic() + "-" + r.topicPartition().partition())
                    .sorted().collect(Collectors.joining(","));
            final String leadersStr = b.leaderReplicas().stream()
                    .filter(r -> topics.contains(r.topicPartition().topic()))
                    .map(r -> r.topicPartition().topic() + "-" + r.topicPartition().partition())
                    .sorted().collect(Collectors.joining(","));
            System.out.println("broker: " + b.id() + ", #replicas: " + b.replicas().size() + " : " + leadersStr + " : " + replicasStr);
        });
    }

    @Test
    public void testProductionRebalance() throws Exception {
        // ads cluster — 645, 646, 647
        // eventsourcing - 592
        Arrays.asList(105, 114, 123, 421, 286, 328, 329, 592, 326, 636, 645, 646, 647).forEach(clusterId -> {
            try {
                Pair<ClusterModel, ClusterModel> pair = makeClusterModelFromKaasCluster(clusterId);
                simulateCruiseControlRebalance("kaas - " + clusterId, pair, null);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testSimpleRebalance() throws Exception {
        int numBrokers = 6;
        var pair = makeSimpleClusterModel(numBrokers);
        simulateCruiseControlRebalance("simple cluster with broker_cnt = " + numBrokers, pair, null);
    }

    private Pair<ClusterModel, ClusterModel> makeSimpleClusterModel(int numBrokers) throws IOException {
        ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
        ClusterModel clusterModelClone = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
        BrokerCapacityInfo brokerCapacity = EddaClient.getBrokerCapacityInfoDefault(AwsConstants.InstanceType.R5_2XL);
        Map<String, AtomicInteger> topicPartitionCnt = new HashMap<>();
        Function<Integer, String> rackIdGetter = bidx -> {
            switch (bidx % 3) {
                case 0:
                    return "useast1a";
                case 1:
                    return "useast1b";
                case 2:
                    return "useast1c";
                default:
                    throw new IllegalArgumentException();
            }
        };
        for (int bidx = 0; bidx < numBrokers; bidx++) {
            String rackId = rackIdGetter.apply(bidx);
            clusterModel.createRack(rackId);
            clusterModelClone.createRack(rackId);
            clusterModel.createBroker(rackId, "broker" + bidx, bidx, brokerCapacity, false);
            clusterModelClone.createBroker(rackId, "broker" + bidx, bidx, brokerCapacity, false);
        }
        for (int bidx = 0; bidx < numBrokers; bidx++) {
            Broker broker = clusterModel.broker(bidx);
            for (int tidx = 0; tidx < 2; tidx++) {
                String topic = "T" + tidx;
                for (int pidx = 0; pidx < 2 * bidx; pidx++) {
                    int offset = topicPartitionCnt.computeIfAbsent(topic, k -> new AtomicInteger()).getAndIncrement();
                    TopicPartition tp = new TopicPartition(topic, offset);
                    clusterModel.createReplica(broker.rack().id(), broker.id(), tp, 0, true);
                    AggregatedMetricValues aggregatedMetricValues = getAggregatedMetricValues(1.0, 10.0, 13.0, 5.0);
                    clusterModel.setReplicaLoad(broker.rack().id(), broker.id(), tp, aggregatedMetricValues, Collections.singletonList(3L));
                    clusterModelClone.createReplica(broker.rack().id(), broker.id(), tp, 0, true);
                    clusterModelClone.setReplicaLoad(broker.rack().id(), broker.id(), tp, aggregatedMetricValues, Collections.singletonList(3L));
                    final int nextBrokerId = (broker.id() + 1) % numBrokers;
                    final String followerRackId = rackIdGetter.apply(nextBrokerId);
                    clusterModel.createReplica(followerRackId, nextBrokerId, tp, 1, false);
                    clusterModel.setReplicaLoad(followerRackId, nextBrokerId, tp, aggregatedMetricValues, Collections.singletonList(3L));
                    clusterModelClone.createReplica(followerRackId, nextBrokerId, tp, 1, false);
                    clusterModelClone.setReplicaLoad(followerRackId, nextBrokerId, tp, aggregatedMetricValues, Collections.singletonList(3L));
                }
            }
        }
        return Pair.create(clusterModel, clusterModelClone);
    }

    private Pair<ClusterModel, ClusterModel> makeClusterModelFromKaasCluster(int clusterId) throws Exception {
        KaasControllerClient kaas = new KaasControllerClient();
        List<BrokerInfo> brokers = kaas.getBrokers(clusterId);
        List<TopicInfo> topics = kaas.getTopics(clusterId);
        BrokerInfo fbroker = brokers.get(0);
        TopicInfo ftopic = topics.get(0);
        BrokerCapacityInfo brokerCapacity = EddaClient.getBrokerCapacityInfo(fbroker.getName(), ftopic.getAccount(), ftopic.getRegion());
        ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
        ClusterModel clusterModelClone = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
        Map<TopicPartition, AtomicInteger> topicReplicaIndex = new HashMap<>();
        for (BrokerInfo b : brokers) {
            String rackId = b.getZone();
            clusterModel.createRack(rackId);
            clusterModelClone.createRack(rackId);
            Broker broker = clusterModel.createBroker(rackId, b.getName(), b.getId(), brokerCapacity, false);
            clusterModelClone.createBroker(rackId, b.getName(), b.getId(), brokerCapacity, false);
            b.getBrokerDetail().getTopicList()
                    .stream()
                    .filter(t -> !t.isCompacted())
                    //.filter(t -> "ads_playback_problem_report".equals(t.getTopicName()))
                    .flatMap(t -> t.getPartitions().stream().map(tp -> Pair.create(t, tp)))
                    .forEach(t -> {
                        final TopicPartition tp = new TopicPartition(t.getFirst().getTopicName(), t.getSecond().getId());
                        t.getSecond().getReplicas().stream()
                                .filter(r -> r.getBrokerId() == b.getId())
                                .forEach(r -> {
                                    int replicaIndex = topicReplicaIndex.computeIfAbsent(tp, k -> new AtomicInteger()).getAndIncrement();
                                    AggregatedMetricValues aggregatedMetricValues = getAggregatedMetricValues(0.1, 1.0, 1.3, .5);
                                    clusterModel.createReplica(rackId, broker.id(), tp, replicaIndex, r.isLeader());
                                    clusterModel.setReplicaLoad(rackId, broker.id(), tp, aggregatedMetricValues, Collections.singletonList(3L));
                                    clusterModelClone.createReplica(rackId, broker.id(), tp, replicaIndex, r.isLeader());
                                    clusterModelClone.setReplicaLoad(rackId, broker.id(), tp, aggregatedMetricValues, Collections.singletonList(3L));
                                });
                    });
        }
        return Pair.create(clusterModel, clusterModelClone);
    }

    private void simulateCruiseControlRebalance(
            String clusterDescription, Pair<ClusterModel, ClusterModel> clusterPair, List<String> goalNameByPriorityArg) throws Exception {

        var clusterModel = clusterPair.getSecond();
        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        props.setProperty(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG, "0");
        props.setProperty(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG, "0");
        props.setProperty(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG, "1");
        props.setProperty(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG, "2");
        props.setProperty(AnalyzerConfig.LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, "1.05");
        BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
        final List<String> goalNameByPriority;
        if (goalNameByPriorityArg == null || goalNameByPriorityArg.isEmpty()) {
            goalNameByPriority = Arrays.asList(
                    TopicLeaderReplicaDistributionGoal.class.getName(),
                    RackAwareDistributionGoal.class.getName(),
                    TopicReplicaDistributionGoal.class.getName(),
                    LeaderReplicaDistributionGoal.class.getName()
            );
        } else {
            goalNameByPriority = goalNameByPriorityArg;
        }
        GoalOptimizer goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(balancingConstraint.setProps(props)),
                null,
                new SystemTime(),
                new MetricRegistry(),
                EasyMock.mock(Executor.class),
                EasyMock.mock(AdminClient.class));
        List<Goal> goalsByPriority = new ArrayList<>(goalNameByPriority.size());
        for (String goalClassName : goalNameByPriority) {
            Class<? extends Goal> goalClass = (Class<? extends Goal>) Class.forName(goalClassName);
            try {
                Constructor<? extends Goal> constructor = goalClass.getDeclaredConstructor(BalancingConstraint.class);
                constructor.setAccessible(true);
                goalsByPriority.add(constructor.newInstance(balancingConstraint));
            } catch (NoSuchMethodException badConstructor) {
                //Try default constructor
                goalsByPriority.add(goalClass.newInstance());
            }
        }
        OptimizerResult result = goalOptimizer.optimizations(clusterModel, goalsByPriority, new OperationProgress());
        writeToFile(clusterDescription, clusterPair);

        System.out.println("clusterModel for " + clusterDescription + ": " + clusterModel);
        System.out.println("result: " + result.getProposalSummary());
        System.out.println("result: " + result.goalResultDescription(TopicLeaderReplicaDistributionGoal.class.getSimpleName()));
        System.out.println("violated goals - " + StringUtils.join(result.violatedGoalsAfterOptimization(), ","));
        System.out.println("end " + clusterDescription);
        System.out.println("===========\n");
    }

    private void writeToFile(String clusterDescription, Pair<ClusterModel, ClusterModel> pair) throws IOException {
        Path dirPath = Paths.get(System.getenv("HOME"), "/cc_rebalance/");
        if (!Files.isDirectory(dirPath)) {
            Files.createDirectory(dirPath);
        }
        ClusterModel beforeCluster = pair.getFirst();
        ClusterModel clusterModel = pair.getSecond();
        String fname = clusterDescription.replaceAll("[- =_]", "");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String topic : clusterModel.topics()) {
            final DescriptiveStatistics afterLeaderStats = new DescriptiveStatistics();
            final DescriptiveStatistics afterReplicaStats = new DescriptiveStatistics();
            for (Broker broker : clusterModel.brokers()) {
                afterLeaderStats.addValue(broker.numLeadersFor(topic));
                afterReplicaStats.addValue(broker.numReplicasOfTopicInBroker(topic));
            }

            final DescriptiveStatistics beforeLeaderStats = new DescriptiveStatistics();
            final DescriptiveStatistics beforeReplicaStats = new DescriptiveStatistics();
            for (Broker broker : beforeCluster.brokers()) {
                beforeLeaderStats.addValue(broker.numLeadersFor(topic));
                beforeReplicaStats.addValue(broker.numReplicasOfTopicInBroker(topic));
            }

            Map<String, Object> rowMap = new HashMap<>();
            rowMap.put("topicName", topic);
            rowMap.put("numPartitions", afterLeaderStats.getSum());
            rowMap.put("stats", afterLeaderStats.toString());
            rowMap.put("beforeStatsHtml", beforeLeaderStats.toString().replaceAll("\n", " <br/> "));
            rowMap.put("afterStatsHtml", afterLeaderStats.toString().replaceAll("\n", " <br/> "));
            rowMap.put("beforeValues", StringUtils.join(beforeLeaderStats.getValues(), ','));
            rowMap.put("afterValues", StringUtils.join(afterLeaderStats.getValues(), ','));
            rowMap.put("beforeReplicaValues", StringUtils.join(beforeReplicaStats.getValues(), ','));
            rowMap.put("afterReplicaValues", StringUtils.join(afterReplicaStats.getValues(), ','));
            rows.add(rowMap);
        }
        List<Map<String, Object>> clusterModelBrokers = new ArrayList<>();
        for (Broker broker : beforeCluster.brokers()) {
            Map<String, Object> clusterRowMap = new HashMap<>();
            clusterRowMap.put("id", broker.id());
            clusterRowMap.put("rack", broker.rack().id());
            clusterRowMap.put("beforeLeaderReplicas", beforeCluster.broker(broker.id()).leaderReplicas());
            clusterRowMap.put("beforeReplicas", beforeCluster.broker(broker.id()).replicas());
            clusterRowMap.put("leaderReplicas", clusterModel.broker(broker.id()).leaderReplicas());
            clusterRowMap.put("replicas", clusterModel.broker(broker.id()).replicas());
            clusterModelBrokers.add(clusterRowMap);
        }

        rows.sort(Comparator.comparingInt(x -> ((Double) x.get("numPartitions")).intValue()));
        String htmlTemplate = readResourceFile("cluster_model_histogram.html");
        MustacheFactory mf = new DefaultMustacheFactory();
        Mustache mustache = mf.compile(new StringReader(htmlTemplate), "cluster_model_histogram");
        Map<String, Object> dataModel = new HashMap<>();
        dataModel.put("clusterDescription", clusterDescription);
        dataModel.put("beforeClusterModel", beforeCluster);
        dataModel.put("clusterModelBrokers", clusterModelBrokers);
        dataModel.put("clusterModel", clusterModel);
        dataModel.put("beforeBrokerLeaderReplicas", beforeCluster.brokers().stream()
                .map(b -> String.valueOf(b.leaderReplicas().size())).collect(Collectors.joining(",")));
        dataModel.put("beforeBrokerReplicas", beforeCluster.brokers().stream()
                .map(b -> String.valueOf(b.replicas().size())).collect(Collectors.joining(",")));
        dataModel.put("afterBrokerLeaderReplicas", clusterModel.brokers().stream()
                .map(b -> String.valueOf(b.leaderReplicas().size())).collect(Collectors.joining(",")));
        dataModel.put("afterBrokerReplicas", clusterModel.brokers().stream()
                .map(b -> String.valueOf(b.replicas().size())).collect(Collectors.joining(",")));
        dataModel.put("rows", rows);
        StringWriter writer = new StringWriter();
        mustache.execute(writer, dataModel).flush();

        File htmlFile = Paths.get(dirPath.toString(), "dynamic_histogram_" + fname + ".html").toFile();
        try (FileWriter fileWriter = new FileWriter(htmlFile, Charset.defaultCharset())) {
            fileWriter.write(writer.toString());
            System.out.println("HTML file created: dynamic_histogram.html");
        }
    }

    private String readResourceFile(String resourceName) throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(resourceName);
             InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {

            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
            return content.toString();
        }
    }
}
