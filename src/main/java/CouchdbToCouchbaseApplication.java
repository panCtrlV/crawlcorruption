/*
This script helps to move documents in a CouchDB database to a CouchBase
data bucket. Details can be found at:

    http://blog.couchbase.com/2015/august/moving-couch

I haven't fully understand it and am not sure how to use it. But for now,
we can just stick with CouchDB.

Couchbase is better suited for a distributed environment than CouchDB.
It also provides a Spark connector which enables accessing Couchbase
from within Spark. An introduction can be found at:

    http://blog.couchbase.com/introducing-the-couchbase-spark-connector

A getting-started tutorial can be found at:

    http://developer.couchbase.com/documentation/server/4.0/connectors/spark-1.0/getting-started.html
 */

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.util.retry.RetryBuilder;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.util.Assert;
import rx.Observable;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.time.Delay.fixed;

@SpringBootApplication
public class CouchdbToCouchbaseApplication {

    public static Logger log = Logger.getLogger(CouchdbToCouchbaseApplication.class);

    public static final String TOTAL_ROWS_PROPERTY = "total_rows";

    public static final String OFFSET_PROPERTY = "offset";

    @Value("${couchdb.downloadURL:http://127.0.0.1:5984/database_export/_all_docs?include_docs=true}")
    String couchDBRequest;

    @Value("${import.error.log:errors.out}")
    String errorLogFilename;

    @Value("${import.success.log:success.out}")
    String successLogFilename;

    public static void main(String[] args) {
        SpringApplication.run(CouchdbToCouchbaseApplication.class, args);
    }


    @Bean
    public CommandLineRunner runner() {
        return new CommandLineRunner() {
            @Override
            public void run(String... args) throws Exception {
                long start = System.currentTimeMillis();
                CouchbaseCluster cc = CouchbaseCluster.create();
                AsyncBucket asyncBucket = cc.openBucket().async();

                URL url = new URL(couchDBRequest);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setRequestProperty("Accept", "application/json");
                //assume this is going to be a big file...
                conn.setReadTimeout(0);
                if (conn.getResponseCode() != 200) {
                    throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
                }

                ObjectMapper om = new ObjectMapper();
                BufferedReader inp2 = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                final long[] totalRows = new long[2];
                int count = Observable.from(inp2.lines()::iterator).flatMap(s -> {
                    JsonNode node = null;
                    if (s.endsWith("\"rows\":[")) {
                        // first line, find total rows, offset
                        s = s.concat("]}");
                        try {
                            node = om.readTree(s.toString());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        totalRows[0] = node.get(TOTAL_ROWS_PROPERTY).asLong();
                        totalRows[1] = node.get(OFFSET_PROPERTY).asLong();
                        log.info(String.format("Query starting at offset %d for a total of %d rows.", totalRows[1], totalRows[0]));
                        writeToSuccessLog(String.format("Query starting at offset %d for a total of %d rows.", totalRows[1], totalRows[0]));
                        return Observable.empty();
                    } else if (s.length() == 2) {
                        // last line, do nothing
                        writeToSuccessLog("end of the feed.");
                        return Observable.empty();
                    } else {
                        try {
                            if (s.endsWith(",")) {
                                node = om.readTree(s.substring(0, s.length() - 1).toString());
                            } else {
                                node = om.readTree(s.toString());
                            }
                            return Observable.just(node);
                        } catch (IOException e) {
                            return Observable.error(e);
                        }
                    }

                }).flatMap(jsonNode -> {
                            String key = jsonNode.get("key").asText();
                            String jsonDoc = jsonNode.get("doc").toString();
                            RawJsonDocument rjd = RawJsonDocument.create(key, jsonDoc);
                            log.debug("Importing " + key);
                            return asyncBucket.upsert(rjd)
                                    .timeout(500, TimeUnit.MILLISECONDS)
                                    .retryWhen(RetryBuilder
                                            .anyOf(RequestCancelledException.class)
                                            .delay(fixed(31, TimeUnit.SECONDS)).max(100).build())
                                    .retryWhen(RetryBuilder
                                            .anyOf(TemporaryFailureException.class, BackpressureException.class)
                                            .delay(fixed(100, TimeUnit.MILLISECONDS))
                                            .max(100)
                                            .build())
                                    .doOnError(t -> writeToErrorLog(key))
                                    .doOnNext(jd -> writeToSuccessLog(key))
                                    .onErrorResumeNext(throwable -> {
                                        log.error(String.format("Could not import document ", key));
                                        log.error(throwable.getMessage());
                                        return Observable.just(null);
                                    });
                        }
                ).count().toBlocking().single();
                long stop = System.currentTimeMillis();
                writeToSuccessLog(String.format("End of the import in %dms.", stop - start));
                Assert.isTrue(count == totalRows[0], String.format("Something went wrong during the import, expected" +
                        " %d but got %d", totalRows[0], count));
            }

        };

    }

    public void writeToSuccessLog(String text){
        writeToFile(successLogFilename, text);
    }

    public void writeToErrorLog(String text){
        writeToFile(errorLogFilename, text);
    }

    public void writeToFile(String filename, String text) {
        try (FileWriter fw = new FileWriter(filename, true);){
            fw.write(text + System.getProperty("line.separator"));
            fw.close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
