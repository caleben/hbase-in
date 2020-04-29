package es.query;

import es.EsUtil;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * @author wenci 2020/4/23
 */
public class EsQuery {
    private static final Logger LOG = LoggerFactory.getLogger(EsQuery.class);
    private static ConcurrentLinkedQueue<String> result = new ConcurrentLinkedQueue<>();

    public QueryBuilder genQueryBuilder(String field, String value) {
        BoolQueryBuilder bool = QueryBuilders.boolQuery();

        bool.should().add(new MatchQueryBuilder(field, "Chemistry_A"));
//        bool.should().add(new MatchQueryBuilder(field, "Chemistry_b"));
//        bool.should().add(new MatchQueryBuilder(field, "Chemistry_c"));
//        bool.should().add(new MatchQueryBuilder(field, "Chemistry_d"));
//        bool.should().add(new MatchQueryBuilder(field, "Chinese_o"));

        return bool;
    }

    public void getResult(String indexName, String field, String value) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        QueryBuilder queryBuilder = genQueryBuilder(field, value);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder)
                .fetchSource(false)
                .size(1000);

        searchRequest.source(searchSourceBuilder)
                .preference("_local")
                .indices(indexName);

        SearchResponse response = EsUtil.getClient().search(searchRequest, RequestOptions.DEFAULT);

        TotalHits totalHits = response.getHits().getTotalHits();
        SearchHit[] hits = response.getHits().getHits();
        List<String> collect = Arrays.stream(response.getHits().getHits()).map(SearchHit::getId).sorted().peek(LOG::info).collect(Collectors.toList());

        LOG.info(">>> query done, size [{}]", totalHits);
    }

    public void getResultBySearchAfter(String indexName, String field, String value, int each) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        QueryBuilder queryBuilder = genQueryBuilder(field, value);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder)
                .fetchSource(false)
                .size(each)
                .sort("course", SortOrder.ASC)
                .sort("_id", SortOrder.ASC);

        searchRequest.source(searchSourceBuilder)
                .preference("_local")
                .indices(indexName);

        SearchResponse response = EsUtil.getClient().search(searchRequest, RequestOptions.DEFAULT);

        long totalHits = response.getHits().getTotalHits().value;
        SearchHit[] hits = response.getHits().getHits();
        int i = 0;
        for (int j = 0; j < totalHits/each+1; j++) {
            LOG.info(">>> fetch size [{}], [batch {}], [queue size {}]", hits.length, i++, result.size());
            List<String> collect = Arrays.stream(response.getHits().getHits()).map(SearchHit::getId).peek(LOG::info).collect(Collectors.toList());
            result.addAll(collect);
            searchSourceBuilder = new SearchSourceBuilder()
                    .query(queryBuilder)
                    .fetchSource(false)
                    .size(each)
                    .sort("course", SortOrder.ASC)
                    .sort("_id", SortOrder.ASC)
                    .searchAfter(hits[hits.length - 1].getSortValues());

            searchRequest.source(searchSourceBuilder)
                    .preference("_local")
                    .indices(indexName);
            response = EsUtil.getClient().search(searchRequest, RequestOptions.DEFAULT);
            totalHits = response.getHits().getTotalHits().value;
            hits = response.getHits().getHits();
        }
        LOG.info(">>> search after Done, queue size [{}]", result.size());

    }

    public static void main(String[] args) throws IOException {
        String indexName = "course_es";
        String field = "course";
        String value = "Chinese_o";
        EsQuery esQuery = new EsQuery();
//        esQuery.getResult(indexName, field, value);
        esQuery.getResultBySearchAfter(indexName, field, value, 10);
        EsUtil.closeResource();

    }
}
