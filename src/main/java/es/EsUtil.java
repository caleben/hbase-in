package es;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.Constant;
import tool.FileUtil;
import tool.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static tool.Constant.BULK_ACTIONS;
import static tool.Constant.ES_HBASE_HOST;
import static tool.Constant.ES_PORT;

/**
 * @author wenci 2020/4/22
 */
public class EsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(EsUtil.class);
    private static RestHighLevelClient client = getHighLevelClient(ES_HBASE_HOST, ES_PORT);

    private static RestHighLevelClient getHighLevelClient(String ip, int port) {
        RestClientBuilder builder;
        List<HttpHost> httpHosts = new ArrayList<>();

        for (String s : ip.split(",")) {
            httpHosts.add(new HttpHost(s.trim(), port));
        }

        builder = RestClient.builder(httpHosts.toArray(new HttpHost[0]));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        //ES认证默认关闭
        if (false) {
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elasticsearch", "xxx"));
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        return client;
    }

    public static void loadData(String indexName, String fileName) throws IOException, InterruptedException {
        createTemplateIfAbsent("my-template", true, true,
                indexName.substring(0, indexName.indexOf(Tool.LINE)) + "*");
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

    private static void createTemplateIfAbsent(String name, boolean hot, boolean switchOn, String patterns) throws IOException {
        if (isTemplateExist(name)) {
            LOG.info(">>> template [{}] exists,do nothing", name);
            return;
        }
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(name)
                .patterns(Collections.singletonList(patterns))
                .order(0)
                .settings(genSettings(hot, switchOn))
                .mapping(genFieldMapping(Constant.COLUMNS));

        // false 表示如果存在则update; true 表示只能create，如果存在则报错
        request.create(false);
        AcknowledgedResponse re = client.indices().putTemplate(request, RequestOptions.DEFAULT);
        LOG.info(">>> create template {} {}", name, re.isAcknowledged());
    }

    private static Settings.Builder genSettings(boolean hot, boolean switchHotWarm) {
        Settings.Builder settings = Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "20s");
        return switchHotWarm ?
                hot ? settings.put("index.routing.allocation.require.temperature", "hot") :
                        settings.put("index.routing.allocation.require.temperature", "warm")
                : settings;
    }

    private static XContentBuilder genFieldMapping(String[] columns) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject().startObject("properties");
        for (String column : columns) {
            builder.startObject(column)
                    .field("type", "keyword")
                    .endObject();
        }
        return builder.endObject().endObject();
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

            Object[] objects = new Object[2 * Constant.COLUMNS.length];

            for (int i = 0; i < Constant.COLUMNS.length; i++) {
                objects[2 * i] = Constant.COLUMNS[i];
                objects[2 * i + 1] = n[i + 1];
            }
            bulkProcessor.add(new IndexRequest(indexName)
                    .id(n[0])
                    .source(XContentType.JSON, objects)
            );
        }

        bulkProcessor.awaitClose(20L, TimeUnit.SECONDS);

    }

    public static RestHighLevelClient getClient() {
        return client;
    }

}
