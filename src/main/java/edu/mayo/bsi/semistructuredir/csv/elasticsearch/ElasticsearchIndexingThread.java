package edu.mayo.bsi.semistructuredir.csv.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;

import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElasticsearchIndexingThread extends Thread {

    private static final AtomicBoolean INIT_START_LOCK = new AtomicBoolean(false);
    private static final AtomicBoolean INIT_END_LOCK = new AtomicBoolean(false);
    private static final BlockingDeque<Job> JOBS = new LinkedBlockingDeque<>(1000);
    private static final AtomicBoolean SHUTDOWN = new AtomicBoolean(false);

    private static String HOST = "localhost";
    private static int HTTP_PORT = 9210;
    private static int PORT = 9310;
    private static String INDEX = "index";
    private static Client ES_CLIENT;

    public ElasticsearchIndexingThread() {
        super("SemiStructuredIR-Elasticsearch-Indexer-Thread");
        initES();
    }

    /**
     * Initializes the ElasticSearch index, adding mappings as necessary. Assumes that index already exists and this is
     * run after the OMOP indexer.
     */
    private void initES() {
        if (!INIT_START_LOCK.getAndSet(true)) { // Thread safety: only init from config once, rest should wait
            // Construct mapping information for index
            Map<String, JSONObject> mapping = new LinkedHashMap<>();
            mapping.put("Person",
                    new JSONObject()
                            .put("properties", new JSONObject()));
            mapping.put("Procedure",
                    new JSONObject()
                            .put("_parent", new JSONObject().put("type", "Person")));
            mapping.put("Diagnosis",
                    new JSONObject()
                            .put("_parent", new JSONObject().put("type", "Person")));
            mapping.put("LabTest",
                    new JSONObject()
                            .put("_parent", new JSONObject().put("type", "Person")));
            // Wrapper object sent to ES
            for (Map.Entry<String, JSONObject> e : mapping.entrySet()) {
                try {
                    String base = "http://" + HOST + ":" + HTTP_PORT + "/" + INDEX + "/_mapping/" + e.getKey();
                    URL indexURL = new URL(base);
                    HttpURLConnection conn = (HttpURLConnection) indexURL.openConnection();
                    System.out.println(base);
                    System.out.println(e.getValue().toString());
                    conn.setRequestMethod("PUT");
                    conn.setDoOutput(true);
                    conn.setRequestProperty("Content-Type", "application/json");
                    conn.setRequestProperty("Accept", "application/json");
                    OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
                    osw.write(e.getValue().toString());
                    osw.flush();
                    osw.close();
                    conn.getResponseCode(); // Force update
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            Settings s = Settings.builder()
                    .put("cluster.name", "elasticsearch").put("client.transport.sniff", true).put().build();
            try {
                ES_CLIENT = new PreBuiltTransportClient(s).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Could not find host using supplied settings", e);
            }
            synchronized (INIT_END_LOCK) {
                INIT_END_LOCK.set(true);
                INIT_END_LOCK.notifyAll();
            }
        } else {
            synchronized (INIT_END_LOCK) {
                while (!INIT_END_LOCK.get()) {
                    try {
                        INIT_END_LOCK.wait(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        Collection<Job> jobs = new ArrayList<>(100);
        while (!SHUTDOWN.get() || !JOBS.isEmpty()) {
            JOBS.drainTo(jobs, 100);
            if (jobs.isEmpty()) {
                synchronized (JOBS) {
                    try {
                        JOBS.wait(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                BulkRequestBuilder bulkBuilder = ES_CLIENT.prepareBulk();
                for (Job j : jobs) {
                    bulkBuilder.add(index(j.source, j.type, j.parent, j.routing));
                }
                jobs.clear();
                try {
                    bulkBuilder.execute().actionGet();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private IndexRequestBuilder index(JSONObject obj, String type, String parentID, String routingID) {
        IndexRequestBuilder indexRequest = ES_CLIENT.prepareIndex(INDEX, type).setSource(obj.toString(), XContentType.JSON);
        if (parentID != null) {
            indexRequest.setParent(parentID);
        } else { // Is a root element/person
            indexRequest.setId(obj.getString("person_id")).setVersionType(VersionType.EXTERNAL).setVersion(Long.MAX_VALUE); // Prevent overwrites
        }
        if (routingID != null) {
            indexRequest.setRouting(routingID);
        }
        return indexRequest;
    }

    public static void schedule(JSONObject source, String dataType, String parentID, String routingID) {
        if (SHUTDOWN.get()) {
            throw new IllegalStateException("Attempted to schedule indexing but is shut down");
        }
        try {
            JOBS.offer(new Job(source, dataType, parentID, routingID), 1, TimeUnit.DAYS);
            synchronized (JOBS) {
                JOBS.notifyAll();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("An error occured during indexing", e);
        }
    }

    public static void shutdown() {
        if (SHUTDOWN.getAndSet(true)) {
            throw new IllegalStateException("Attempted to shut down but already shut down!");
        }

    }

    private static class Job {
        JSONObject source;
        String type;
        String parent;
        String routing;

        public Job(JSONObject source, String type, String parent, String routing) {
            this.source = source;
            this.type = type;
            this.parent = parent;
            this.routing = routing;
        }
    }

}
