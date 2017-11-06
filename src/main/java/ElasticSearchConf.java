import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponseContentListener;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchConf{
	
	public static TransportClient client = null;
	public static void initializeES() {
		 final Settings settings = Settings.builder()
				 .put("cluster.name", "elasticsearch")
	              
	                .build();
		 try{
		client = new PreBuiltTransportClient(settings).
				addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 9300)));
		System.out.println("es init ..................!! Thanks");
		 }catch(Exception e){
			 e.printStackTrace();
		 }
	}
	public static void searchES() {
//		SearchResponse responce = client.prepareSearch()
//				.setQuery(QueryBuilder)
		SearchResponse responce = client.prepareSearch()
				.setQuery(QueryBuilders.boolQuery()
						.must(QueryBuilders.termQuery("site_name_not_analyzed", "City of Kissimmee"))
						//.must(QueryBuilders.termQuery("bidtype_not_analyzed", "Bid Notification"))
						)
						.setSize(31).execute().actionGet();
						
			String output = responce.toString();
			System.out.println(responce);
	}
	public static void deleteES(){
		 BulkItemResponse responce = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.boolQuery()
						.must(QueryBuilders.termQuery("site_name_not_analyzed", "City of Kissimmee"))
								.must(QueryBuilders.termQuery("bidtype_not_analyzed","Bid Notification")))
								.source()
								.get();
		 System.out.println(responce);
		 
				
	}
	public static void main(String[] args) {
		ElasticSearchConf esconf = new ElasticSearchConf();
		esconf.initializeES();
		esconf.searchES();
	}

}

