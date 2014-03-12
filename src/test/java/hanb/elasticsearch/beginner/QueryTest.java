package hanb.elasticsearch.beginner;

import java.util.Arrays;
import java.util.HashMap;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.statistical.StatisticalFacetBuilder;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTest {
	private static final Logger log = LoggerFactory.getLogger(QueryTest.class);
	
	@Ignore
	public void termQuery() throws Exception {
		TermQueryBuilder termQueryBuilder = new TermQueryBuilder("unified_search", "tv");
		String result = executeQuery(termQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void termsQuery() throws Exception {
		TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("unified_search", Arrays.asList("삼정","알지"));
		termsQueryBuilder.minimumMatch(1);
		
		String result = executeQuery(termsQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void matchQuery() throws Exception {
		MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("unified_search", "led tv");
		matchQueryBuilder.operator(Operator.OR);
		
		String result = executeQuery(matchQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void multiMatchQuery() throws Exception {
		MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("카논 렌즈", "unified_search", "item_name");
		String result = executeQuery(multiMatchQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void queryStringQuery() throws Exception {
		QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder("LED AND 삼정 OR TV");
		queryStringQueryBuilder.defaultField("unified_search");
		
		String result = executeQuery(queryStringQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void idsQuery() throws Exception {
		IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder("market");
		idsQueryBuilder.addIds("1", "3", "5", "18");
		
		String result = executeQuery(idsQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void prefixQuery() throws Exception {
		PrefixQueryBuilder prefixQueryBuilder = new PrefixQueryBuilder("cat_third_id", "2_1");
		String result = executeQuery(prefixQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void matchAllQuery() throws Exception {
		MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
		String result = executeQuery(matchAllQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void rangeQuery() throws Exception {
		RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("item_sales_price");
		rangeQueryBuilder.gte(1000000)
			.lte(1500000);
		
		String result = executeQuery(rangeQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void boolQuery() throws Exception {
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("unified_search", Arrays.asList("삼정", "tv"));
		termsQueryBuilder = termsQueryBuilder.minimumMatch(2);
		boolQueryBuilder.must(termsQueryBuilder);
		
		String result = executeQuery(boolQueryBuilder);
		
		log.debug(result);
		
		boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.should(new TermQueryBuilder("unified_search", "삼정"))
			.should(new TermQueryBuilder("unified_search", "tv"))
			.minimumNumberShouldMatch(2);
		
		result = executeQuery(boolQueryBuilder);
		
		log.debug(result);
		
		boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.mustNot(new TermQueryBuilder("unified_search", "알지"));
		
		result = executeQuery(boolQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void pagineQuery() throws Exception {
		Settings settings;
		Client client;
		
		settings = ImmutableSettings
				.settingsBuilder()
				.build();
		
		client = buildClient(settings);
		
		SearchResponse searchResponse;
		MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
		
		searchResponse = client.prepareSearch("open_market")
				.setQuery(matchAllQueryBuilder)
				.setFrom(0)
				.setSize(5)
				.execute()
				.actionGet();
		
		client.close();
		
		log.debug(searchResponse.toString());
	}
	
	@Ignore
	public void filterQuery() throws Exception {
		Settings settings;
		Client client;
		
		settings = ImmutableSettings
				.settingsBuilder()
				.build();
		
		client = buildClient(settings);
		
		SearchResponse searchResponse;
		MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
		BoolFilterBuilder boolFilterBuilder = new BoolFilterBuilder();
		boolFilterBuilder.mustNot(new TermFilterBuilder("item_name", "카논"));
		
		searchResponse = client.prepareSearch("open_market")
				.setQuery(matchAllQueryBuilder)
				.setPostFilter(boolFilterBuilder)
				.execute()
				.actionGet();
		
		client.close();
		
		log.debug(searchResponse.toString());
	}
	
	@Ignore
	public void sortQuery() throws Exception {
		Settings settings;
		Client client;
		
		settings = ImmutableSettings
				.settingsBuilder()
				.build();
		
		client = buildClient(settings);
		
		SearchResponse searchResponse;
		MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
		FieldSortBuilder fieldSortBuilderDesc = SortBuilders.fieldSort("item_regdate").order(SortOrder.DESC);
		FieldSortBuilder fieldSortBuilderAsc = SortBuilders.fieldSort("item_sales_volume").order(SortOrder.ASC);
		
		searchResponse = client.prepareSearch("open_market")
				.setQuery(matchAllQueryBuilder)
				.addSort(fieldSortBuilderDesc)
				.addSort(fieldSortBuilderAsc)
				.setTrackScores(true)
				.execute()
				.actionGet();
		
		client.close();
		
		log.debug(searchResponse.toString());
	}
	
	@Ignore
	public void termsFacetQuery() throws Exception {
		Settings settings;
		Client client;
		
		settings = ImmutableSettings
				.settingsBuilder()
				.build();
		
		client = buildClient(settings);
		
		SearchResponse searchResponse;
		MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
		TermsFacetBuilder termsFacetBuilder = FacetBuilders.termsFacet("category_sum").fields("cat_first_id","cat_second_id","cat_third_id").size(100);
				
		searchResponse = client.prepareSearch("open_market")
				.setQuery(matchAllQueryBuilder)
				.addFacet(termsFacetBuilder)
				.execute()
				.actionGet();
		
		client.close();
		
		log.debug(searchResponse.toString());
	}
	
	@Ignore
	public void termsStatsQuery() throws Exception {
		//nested field 에 대한 facet 기능으로 field mapping 은 아래와 같은 구조를 만족해야 한다.
		// 각 key field 명으로 grouping 을 수행 하고 value field 에 값을 가지고 sum 연산을 한다.
		/*
		"FIELD_NAME" : {"type" : "nested",
			"properties" : {
                "KEY_FIELD_NAME" : {"type" : "string", "store" : "no", "index" : "not_analyzed", "omit_norms" : true, "index_options" : "docs", "include_in_all" : false},
                "VALUE_FIELD_NAME" : {"type" : "integer", "store" : "no", "index" : "not_analyzed", "omit_norms" : true, "index_options" : "docs", "ignore_malformed" : true, "include_in_all" : false}
        	}
        },
		*/
	}
	
	@Ignore
	public void statisticalFacetQuery() throws Exception {
		Settings settings;
		Client client;
		
		settings = ImmutableSettings
				.settingsBuilder()
				.build();
		
		client = buildClient(settings);
		
		SearchResponse searchResponse;
		MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
		StatisticalFacetBuilder statisticalFacetBuilder = FacetBuilders.statisticalFacet("price_sum").field("item_sales_price");
				
		searchResponse = client.prepareSearch("open_market")
				.setQuery(matchAllQueryBuilder)
				.addFacet(statisticalFacetBuilder)
				.execute()
				.actionGet();
		
		client.close();
		
		log.debug(searchResponse.toString());
	}
	
	@Ignore
	public void highlightQuery() throws Exception {
		Settings settings;
		Client client;
		
		settings = ImmutableSettings
				.settingsBuilder()
				.build();
		
		client = buildClient(settings);
		
		SearchResponse searchResponse;
		TermQueryBuilder termQueryBuilder = new TermQueryBuilder("unified_search", "tv");
				
		searchResponse = client.prepareSearch("open_market")
				.setQuery(termQueryBuilder)
				.addHighlightedField("item_name")
				.addHighlightedField("cat_second_name")
				.setHighlighterPreTags("<strong>")
				.setHighlighterPostTags("</strong>")
				.execute()
				.actionGet();
		
		client.close();
		
		log.debug(searchResponse.toString());
	}
	
	@Ignore
	public void termBoostingQuery() throws Exception {
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder("카논^2 and 니쿤");
		queryStringQueryBuilder = queryStringQueryBuilder.defaultField("unified_search");
		
		boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.should(new MatchAllQueryBuilder())
			.should(queryStringQueryBuilder);
		
		String result = executeQuery(boolQueryBuilder);
		
		log.debug(result);
	}
	
	@Ignore
	public void fieldBoostingQuery() throws Exception {
		TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("unified_search", Arrays.asList("카논","니쿤"));
		termsQueryBuilder.minimumMatch(1);
		TermsQueryBuilder termsQueryBuilderBoost = new TermsQueryBuilder("cat_third_id", Arrays.asList("2_1_1","2_2_1"));
		termsQueryBuilderBoost.boost(2.0f);
		
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.should(termsQueryBuilder)
			.should(termsQueryBuilderBoost);
		
		String result = executeQuery(boolQueryBuilder);
		
		log.debug(result);
	}
	
	/**
	 * 검색 질의를 위한 공용 함수.
	 * 
	 * @param queryBuilder
	 * @return
	 * @throws Exception
	 */
	protected String executeQuery(QueryBuilder queryBuilder) throws Exception {
		Settings settings;
		Client client;
		
		settings = ImmutableSettings
				.settingsBuilder()
				.build();
		
		client = buildClient(settings);
		
		SearchResponse searchResponse;
		
		searchResponse = client.prepareSearch("open_market")
			.setQuery(queryBuilder)
			.execute()
			.actionGet();
		
		client.close();
		
		return searchResponse.toString();
	}

	/**
     * Reference : https://github.com/dadoonet/spring-elasticsearch
     * 
     * elasticsearch client TransportAddress 등록.<br>
     * 	cluster.node.list<br>
     * 
     * @param settings		client settings 정보.
     * @throws Exception
     */
	protected Client buildClient(Settings settings) throws Exception {
		TransportClient client = new TransportClient(settings);
		String nodes = "localhost:9300";
		String[] nodeList = nodes.split(",");
		int nodeSize = nodeList.length;

		for (int i = 0; i < nodeSize; i++) {
			client.addTransportAddress(toAddress(nodeList[i]));
		}

		return client;
	}
	
	/**
     * Reference : https://github.com/dadoonet/spring-elasticsearch
     * 
     * InetSocketTransportAddress 등록.<br>
     * 
     * @param address		node 들의 ip:port 정보.
     * @return InetSocketTransportAddress
     * @throws Exception
     */
	private InetSocketTransportAddress toAddress(String address) {
		if (address == null) return null;
		
		String[] splitted = address.split(":");
		int port = 9300;
		
		if (splitted.length > 1) {
			port = Integer.parseInt(splitted[1]);
		}
		
		return new InetSocketTransportAddress(splitted[0], port);
	}
}
