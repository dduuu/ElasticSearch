



import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.queryparser.xml.builders.TermQueryBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponseContentListener;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class Test{

    private static TransportClient instance ;
 public static TransportClient client = null;
 public static void initializeES() {
   final Settings settings = Settings.builder()
     .put("cluster.name", "elasticsearch")
               
                 .build();
   try{
  client = new PreBuiltTransportClient(settings).
    addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 9300)));
  System.out.println("es init ..................!! ");
   }catch(Exception e){
    e.printStackTrace();
   }
 }
 public static void searchES(final String indexname,final String sourcename) {
  SearchResponse responce = client.prepareSearch("opps")
		  .setTypes("opps")
    .setQuery(QueryBuilders.boolQuery()
      .must(QueryBuilders.termQuery(indexname, sourcename))
      //.must(QueryBuilders.termQuery("bidtype_not_analyzed", "Bid Notifixcation"))
      )
      .setSize(100).execute().actionGet();
      
   String output = responce.toString();
   System.out.println(output);
   
   Long countopps = count("opps", "opps", termQueryBuilder(indexname, sourcename) );
   System.out.println(countopps);
 }
  public static QueryBuilder termQueryBuilder(String fieldName, Object searchTerm) {
         return QueryBuilders.termQuery(fieldName, searchTerm);
     }
  
     public static  long count(String indexname , String indextype , QueryBuilder query){
   return client.prepareSearch(indexname)
     .setTypes(indextype)
     .setQuery(query)
     .get()
     .getHits()
     .getTotalHits();
   
   
    }
     
     public static void deletedata(final String indexname,final String sourcename){
    	 System.out.println("deletedata started....................!!");
    	 searchES(indexname, sourcename);
    	 // BulkRequestBuilder builder = getBulkBuilder();
    	  BulkByScrollResponse response =
    			    DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
    			        .filter(QueryBuilders.matchQuery(indexname, sourcename)) 
    			        .source("opps")                                  
    			        .get();                                             

    			long deleted = response.getDeleted(); 
    			System.out.println(deleted);
     }
  
     private void update(final String indexname,final String sourcename, final String changevalue) {
 		// TODO Auto-generated method stub
 	 System.out.println("update data started....................!!");
 	 SearchResponse responce = client.prepareSearch("opps")
 			  .setTypes("opps")
 	    .setQuery(QueryBuilders.boolQuery()
 	      .must(QueryBuilders.termQuery(indexname, sourcename))
 	      //.must(QueryBuilders.termQuery("bidtype_not_analyzed", "Bid Notifixcation"))
 	      )
 	      .setSize(100).execute().actionGet();
 	      
 	   String output = responce.toString();
 	   System.out.println(output);
 	
 	 BulkRequestBuilder builder = client.prepareBulk();
 			 //getBulkBuilder();
     for (SearchHit hit :responce.getHits().getHits()){
    	 System.out.println("hit data");
         Map<String, Object> source = hit.getSource();
         source.put("site_name_not_analyzed", changevalue);
         if (!indexname.equals("site_name_not_analyzed")) {
             source.put("site_name", changevalue);
         }

         builder.add(client.prepareIndex("opps","opps")
                 .setSource(source)
                 .setId(hit.getId())
                 .request());
     }
     BulkResponse bulkResponse = builder.numberOfActions() > 0 ? builder.get() : null;
     System.out.println(bulkResponse);
 	}
     public static IndexRequestBuilder getIndexBuilder() {
         return instance.prepareIndex("opps", "opps");
     }
  
  
 
 private BulkRequestBuilder getBulkBuilder() {
		// TODO Auto-generated method stub
	 return instance.prepareBulk();
	}
public static void deleteES(){
//   BulkItemResponse responce = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
//    .filter(QueryBuilders.boolQuery()
//      .must(QueryBuilders.termQuery("site_name_not_analyzed", "City of Kissimmee"))
//        .must(QuerykBuilders.termQuery("bidtype_not_analyzed","Bid Notification")))
//        .source()
//        .get();
//   System.out.println(responce);
   /*
  DeleteByQueryAction.INSTANCE.newRequestBuilder(client).filter(QueryBuilders.matchQuery("site_name_not_analyzed", "City of Kissimmee"))
        .source("").execute(new ActionListener<BulkByScrollResponse>() {
         
         public void onResponse(BulkByScrollResponse responce) {
          // TODO Auto-generated method stub
          long delete = responce.getDeleted();
          System.out.println("data deleted..................!!");
         }
         
         public void onFailure(Exception arg0) {
          // TODO Auto-generated method stub
          
         }
        });
        */
   
 }

 

  
  
  
  
  
  



public static void main(String[] args) {
  Test esconf = new Test();
  esconf.initializeES();
 esconf.searchES("site_name_not_analyzed", "City of North Port");
  //esconf.deletedata("site_name_not_analyzed", "City of Kissimmee");
  esconf.update("site_name_not_analyzed", "City of North Port","North Port");
  
 }


}