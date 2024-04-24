package com.netflix.dyno.queues.demo;

import com.netflix.dyno.demo.redis.DynoJedisDemo;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.redis.RedisQueues;
import com.netflix.dyno.queues.redis.v2.QueueBuilder;
import com.netflix.dyno.queues.shard.ConsistentAWSDynoShardSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DynoQueueDemo extends DynoJedisDemo {

    private static final Logger logger = LoggerFactory.getLogger(DynoQueue.class);

    public DynoQueueDemo(String clusterName, String localRack) {
        super(clusterName, localRack);
    }

    public DynoQueueDemo(String primaryCluster, String shadowCluster, String localRack) {
        super(primaryCluster, shadowCluster, localRack);
    }

    /**
     * Provide the cluster name to connect to as an argument to the function.
     * throws java.lang.RuntimeException: java.net.ConnectException: Connection timed out (Connection timed out)
     * if the cluster is not reachable.
     *
     * @param args: cluster-name version
     *              <p>
     *              cluster-name: Name of cluster to run demo against
     *              version: Possible values = 1 or 2; (for V1 or V2)
     */
    public static void main(String[] args) throws IOException {
        final String clusterName = args[0];

        if (args.length < 2) {
            throw new IllegalArgumentException("Need to pass in cluster-name and version of dyno-queues to run as arguments");
        }

        int version = Integer.parseInt(args[1]);
        final DynoQueueDemo demo = new DynoQueueDemo(clusterName, "us-east-1e");
        Properties props = new Properties();
        props.load(DynoQueueDemo.class.getResourceAsStream("/demo.properties"));
        for (String name : props.stringPropertyNames()) {
            System.setProperty(name, props.getProperty(name));
        }

        try {
            demo.initWithRemoteClusterFromEurekaUrl(args[0], 8102, false);

            if (version == 1) {
                demo.runSimpleV1Demo(demo.client);
            } else if (version == 2) {
                demo.runSimpleV2QueueDemo(demo.client);
            }
            Thread.sleep(10000);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            demo.stop();
            logger.info("Done");
        }
    }


    private void runSimpleV1Demo(DynoJedisClient dyno) throws IOException {
        String region = System.getProperty("LOCAL_DATACENTER");
        String localRack = System.getProperty("LOCAL_RACK");

        String prefix = "dynoQueue_";

        ConsistentAWSDynoShardSupplier ss = new ConsistentAWSDynoShardSupplier(dyno.getConnPool().getConfiguration().getHostSupplier(), region, localRack);

        RedisQueues queues = new RedisQueues(dyno, dyno, prefix, ss, 50_000, 50_000);

        List<Message> payloads = new ArrayList<>();
        payloads.add(new Message("id1", "searchable payload123"));
        payloads.add(new Message("id2", "payload 2"));
        payloads.add(new Message("id3", "payload 3"));
        payloads.add(new Message("id4", "payload 4"));
        payloads.add(new Message("id5", "payload 5"));
        payloads.add(new Message("id6", "payload 6"));
        payloads.add(new Message("id7", "payload 7"));
        payloads.add(new Message("id8", "payload 8"));
        payloads.add(new Message("id9", "payload 9"));
        payloads.add(new Message("id10", "payload 10"));
        payloads.add(new Message("id11", "payload 11"));
        payloads.add(new Message("id12", "payload 12"));
        payloads.add(new Message("id13", "payload 13"));
        payloads.add(new Message("id14", "payload 14"));
        payloads.add(new Message("id15", "payload 15"));

        DynoQueue v1Queue = queues.get("simpleQueue");

        // Clear the queue in case the server already has the above key.
        v1Queue.clear();

        // Test push() API
        List pushedMsgs = v1Queue.push(payloads);

        // Test ensure() API
        Message msg1 = payloads.get(0);
        logger.info("Does Message with ID '" + msg1.getId() + "' already exist? -> " + !v1Queue.ensure(msg1));

        // Test containsPredicate() API
        logger.info("Does the predicate 'searchable' exist in  the queue? -> " + v1Queue.containsPredicate("searchable"));

        // Test getMsgWithPredicate() API
        logger.info("Get MSG ID that contains 'searchable' in the queue -> " + v1Queue.getMsgWithPredicate("searchable pay*"));

        // Test getMsgWithPredicate(predicate, localShardOnly=true) API
        // NOTE: This only works on single ring sized Dynomite clusters.
        logger.info("Get MSG ID that contains 'searchable' in the queue -> " + v1Queue.getMsgWithPredicate("searchable pay*", true));
        logger.info("Get MSG ID that contains '3' in the queue -> " + v1Queue.getMsgWithPredicate("3", true));

        Message poppedWithPredicate = v1Queue.popMsgWithPredicate("searchable pay*", false);
        v1Queue.ack(poppedWithPredicate.getId());

        List<Message> specificPops = new ArrayList<>();
        // We'd only be able to pop from the local shard with popWithMsgId(), so try to pop the first payload ID we see in the local shard.
        // Until then pop all messages not in the local shard with unsafePopWithMsgIdAllShards().
        for (int i = 1; i < payloads.size(); ++i) {
            Message popWithMsgId = v1Queue.popWithMsgId(payloads.get(i).getId());
            if (popWithMsgId != null) {
                specificPops.add(popWithMsgId);
                break;
            } else {
                // If we were unable to pop using popWithMsgId(), that means the message ID does not exist in the local shard.
                // Ensure that we can pop with unsafePopWithMsgIdAllShards().
                Message unsafeSpecificPop = v1Queue.unsafePopWithMsgIdAllShards(payloads.get(i).getId());
                assert(unsafeSpecificPop != null);
                boolean ack = v1Queue.ack(unsafeSpecificPop.getId());
                assertack;
            }
        }

        // Test ack()
        boolean ackSuccessful = v1Queue.ack(specificPops.get(0).getId());
        assertackSuccessful;

        // Test remove()
        // Note: This checks for "id9" specifically as it implicitly expects every 3rd element we push to be in our
        // local shard.
        boolean removed = v1Queue.remove("id9");
        assertremoved;

        // Test pop(). Even though we try to pop 3 messages, there will only be one remaining message in our local shard.
        List<Message> poppedMsgs = v1Queue.pop(1, 1000, TimeUnit.MILLISECONDS);
        v1Queue.ack(poppedMsgs.get(0).getId());

        // Test unsafePeekAllShards()
        List<Message> peekAllMsgs = v1Queue.unsafePeekAllShards(5);
        for (Message msg : peekAllMsgs) {
            logger.info("Message peeked (ID : payload) -> " + msg.getId() + " : " + msg.getPayload());
        }

        // Test unsafePopAllShards()
        List<Message> popAllMsgs = v1Queue.unsafePopAllShards(7, 1000, TimeUnit.MILLISECONDS);
        for (Message msg : popAllMsgs) {
            logger.info("Message popped (ID : payload) -> " + msg.getId() + " : " + msg.getPayload());
            boolean ack = v1Queue.ack(msg.getId());
            assertack;
        }

        v1Queue.clear();
        v1Queue.close();
    }

    private void runSimpleV2QueueDemo(DynoJedisClient dyno) throws IOException {
        String prefix = "dynoQueue_";

        DynoQueue queue = new QueueBuilder()
                .setQueueName("test")
                .setRedisKeyPrefix(prefix)
                .useDynomite(dyno, dyno)
                .setUnackTime(50_000)
                .build();

        Message msg = new Message("id1", "message payload");
        queue.push(Arrays.asList(msg));

        int count = 10;
        List<Message> polled = queue.pop(count, 1, TimeUnit.SECONDS);
        logger.info(polled.toString());

        queue.ack("id1");
        queue.close();
    }
}
