package com.dant.webservices;

import com.dant.entity.Index;
import com.dant.entity.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Path("/api/index")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class IndexWS {

    public void init(){
     //   createIndex("table1", new String[]{"c1"});
    }
    private IndexWS(){init();}
    private static IndexWS INSTANCE;
    public static IndexWS getInstance(){
        if(INSTANCE==null)INSTANCE = new IndexWS();
        return INSTANCE;
    }

    private Gson gson = new GsonBuilder().serializeNulls().create();


    /**
     * A web service to create an index in a table from column names
     * @param tableName the table where we want to create an index
     * @param type the type of index we want to use
     *             ("BPTree_Full_Disk" or "BPTree_Semi_Disk" or "BPTree_Full_Ram" or "Hash_Table_Full_Ram"), by default
     *             value is "BPTree_Semi_Disk"
     * @param distribute Boolean to specify if we want to distribute the index creation, true by default
     * @param columnNames the names of concerned columns
     * @return
     * */
    @POST
    @Path("/create")
    public Response createIndex(@QueryParam("tableName") final String tableName,
                                @QueryParam("type") @DefaultValue("BPTree_Semi_Disk") final String type,
                                @QueryParam("distribute") @DefaultValue("true") final Boolean distribute,
                                final String[] columnNames) throws Exception {
        if(tableName==null||tableName.isEmpty())throw new IllegalArgumentException("Cannot get index without table name");
        if(columnNames==null||columnNames.length==0)throw new IllegalArgumentException("Cannot get an index without columns names");
        ExecutorService pool = Executors.newFixedThreadPool(Utils.NB_THREADS);

        Table table = TableWS.getInstance().getOneTable(tableName);
        pool.execute(()->{
            try {
                table.createIndex(columnNames, type);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        List<String> nodes = NodeWS.getInstance().getInstancesURL();
        String myInstance = NodeWS.getInstance().getMyInstance();
        if (distribute) {
            for(String instance : nodes){
                if(!myInstance.equals(instance)){
                    pool.execute(()->{
                        MultivaluedMap<String, Object> queryParams = new MultivaluedMapImpl<>();
                        queryParams.add("tableName",tableName);
                        queryParams.add("type", type);
                        queryParams.add("distribute", false);
                        NodeWS.getInstance().doBasicRequestPost(
                                instance+"/api/index/create",
                                gson.toJson(columnNames),
                                MediaType.APPLICATION_JSON,
                                queryParams);
                    });
                }
            }
        }
        pool.shutdown();
        pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        return Response.ok(table).build();
    }


    /**
     * A web service to get all index of a table
     * @param tableName concerned table name
     * @return the list of all index of this table
     * */
    @GET
    @Path("/getall")
    public List<Index> getAllIndexFromTableName(@QueryParam("tableName") final String tableName){
        if(tableName==null || tableName.isEmpty())throw new IllegalArgumentException("Cannot get index without table name");
        Table table = TableWS.getInstance().getOneTable(tableName);
        return table.getIndexes();
    }

    /**
     * A web service to get only an index of a table
     * @param tableName concerned table name
     * @param columnNames names of columns of the index
     * @return the requested index
     * */
    @POST
    @Path("/getone")
    public Index getOneIndexFromTableNameAndColumnNames(@QueryParam("tableName")final String tableName, String[] columnNames){
        if(tableName==null || tableName.isEmpty())throw new  IllegalArgumentException("Cannot get index without table name");
        if(columnNames==null || columnNames.length==0)throw new IllegalArgumentException("Cannot get an index without columns names");
        Table table = TableWS.getInstance().getOneTable(tableName);
        Index index = table.getIndex(columnNames);
        return index;
    }

}

