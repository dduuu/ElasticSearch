import java.net.InetSocketAddress;
















import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.engine.Engine.Get;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;


public class Deletedata {
	 private static TransportClient instance ;
	public static void initializeES() {
		   final Settings settings = Settings.builder()
		     .put("cluster.name", "elasticsearch")
		               
		                 .build();
		   try{
		  TransportClient client = new PreBuiltTransportClient(settings).
		    addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 9300)));
		  System.out.println("es init ..................!! ");
		   }catch(Exception e){
		    e.printStackTrace();
		   }
		 }
	public static void deletedata(){
		 QueryBuilder query = QueryBuilders.boolQuery()
	                .must(QueryBuilders.rangeQuery("im_posttimestamp").lte("now").gte("now-1d"))
	                .must(termQuery("site_name_not_analyzed", "State Of Pennsylvania"));
		 Long count = count("opps", "opps", query);
	        System.out.println("Count of opps : " + count);
	      //  SearchResponse response = search(query, count);
	}

//	private static SearchResponse search(QueryBuilder query, Long count) {
//		// TODO Auto-generated method stub
//		return getRequestBuilder()
//                .setQuery(query)
//                .setSize((int) count)
//                .get(); 
//	}
	private static Object getRequestBuilder() {
		// TODO Auto-generated method stub
		return instance.prepareSearch("opps")
                .setTypes("opps");
	}
	private static Long count(String indexname, String indextype, QueryBuilder query) {
		
		// TODO Auto-generated method stub
		return instance.prepareSearch(indexname)
                .setTypes(indextype)
                .setSource(new SearchSourceBuilder().size(0))
                .setQuery(query)
                .get()
                .getHits()
                .getTotalHits();
	}
	

		UpdateByQueryRequestBuilder ubqrb = UpdateByQueryAction.INSTANCE
		    .newRequestBuilder(client);

		Script script = new Script("ctx._source.List = [\"Item 1\",\"Item 2\"]");

		//termQuery is not recognised by the program
		BulkItemResponse r = ubqrb.source("twitter").script(script)
		    .filter(termQuery("user", "kimchy")).execute().get();
	public static void main(String[] args) {
	 initializeES();
	 deletedata();
	  
	}

	
}
