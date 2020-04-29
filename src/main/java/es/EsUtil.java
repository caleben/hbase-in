package es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.FileUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static tool.Constant.BULK_ACTIONS;
import static tool.Constant.ES_HOST;

/**
 * @author wenci 2020/4/22
 */
public class EsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(EsUtil.class);
    private static RestHighLevelClient client = getHighLevelClient();

    private static RestHighLevelClient getHighLevelClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ES_HOST, 9200, "http")
                )
        );
    }

    public static void loadData(String indexName, String fileName) throws IOException, InterruptedException {
        createTemplateIfAbsent("my-template");
        createIndexIfAbsent(indexName);
        bulkLoad(indexName, FileUtil.readFromFile(fileName));
        closeResource();
    }

    public static void closeResource() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean isTemplateExist(String name) throws IOException {
        IndexTemplatesExistRequest request = new IndexTemplatesExistRequest(name);
        request.setLocal(true);
        request.setMasterNodeTimeout(TimeValue.timeValueSeconds(30));
        return client.indices().existsTemplate(request, RequestOptions.DEFAULT);
    }

    private static void createTemplateIfAbsent(String name) throws IOException {
        if (isTemplateExist(name)) {
            LOG.info(">>> template [{}] exists,do nothing", name);
            return;
        }
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(name)
                .patterns(Collections.singletonList("course_*"))
                .order(0)
                .settings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 1)
                        .put("index.refresh_interval", "20s"))
                .mapping(
                        "{\n" +
                                "\"properties\": {\n" +
                                "          \"course\": {\n" +
                                "              \"index\": true,\n" +
                                "              \"type\": \"keyword\"\n" +
                                "            }\n" + ",\n" +
                                "      \"track_id\": {\n" +
                                "        \"type\": \"keyword\",\n" +
                                "        \"doc_values\": true\n" +
                                "      }" +
                                "  }\n" +
                                "}",
                        XContentType.JSON);

        request.create(false);
        AcknowledgedResponse re = client.indices().putTemplate(request, RequestOptions.DEFAULT);
        LOG.info(">>> create template {} {}", name, re.isAcknowledged());
    }

    private static boolean isIndexExist(String name) throws IOException {
        GetIndexRequest request = new GetIndexRequest(name)
                .local(false)
                .humanReadable(true)
                .includeDefaults(false);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    private static void createIndexIfAbsent(String indexName) throws IOException {
        if (isIndexExist(indexName)) {
            LOG.info(">>> index [{}] already exists,do nothing", indexName);
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        CreateIndexResponse re = client.indices().create(request, RequestOptions.DEFAULT);
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info(">>> create index {} {}", indexName, re.isAcknowledged());
    }

    private static void bulkLoad(String indexName, List<String> stringList) throws InterruptedException {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numOfActions = request.numberOfActions();
                LOG.info(">>> begin bulk [{}] with [{}]...", executionId, numOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    LOG.error(">>> bulk [{}] executed with failures!", executionId);
                } else {
                    LOG.info(">>> bulk complete with success in {}ms!", response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                LOG.info(">>> bulk failed!", failure);
            }
        };

        BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener)
                .setBulkActions(BULK_ACTIONS)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setConcurrentRequests(4)
                .setFlushInterval(TimeValue.timeValueSeconds(10L))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1), 3))
                .build();

        for (String line : stringList) {
            String[] n = line.split("\t");
            String rowKey = n[0];
            String src = n[1];
            bulkProcessor.add(new IndexRequest(indexName)
                    .id(rowKey)
                    .source(XContentType.JSON, "course", src, "track_id", rowKey)
            );
        }

        bulkProcessor.awaitClose(20L, TimeUnit.SECONDS);

    }

    public static RestHighLevelClient getClient() {
        return client;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        LOG.info(">>>>>>>> start >>>>>>>");
        String indexName = "course_es";
        createTemplateIfAbsent("my-template");
        createIndexIfAbsent(indexName);
        bulkLoad(indexName, FileUtil.readFromFile("course_info.txt"));
        closeResource();
    }
}
