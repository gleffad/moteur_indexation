package com.dant.webservices;

import com.dant.entity.QueryInsert;
import com.dant.entity.QuerySelect;
import com.dant.entity.Result;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

@Path("/api/request")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryWS {
    public void init(){

    }

    private QueryWS(){
        init();
    }
    private static QueryWS INSTANCE;
    public static QueryWS getInstance(){
        if(INSTANCE==null)INSTANCE = new QueryWS();
        return INSTANCE;
    }

    private Gson gson = new GsonBuilder().serializeNulls().create();


    @POST
    @Path("/select")
    @Consumes(MediaType.TEXT_PLAIN)
    public Result doQuerySelect(final String query,
                                @QueryParam("limit")@DefaultValue(Utils.DEFAULT_LIMIT_RESULTS_STR) final Integer limit,
                                @QueryParam("distribute") @DefaultValue("false")final Boolean distribute ){
        boolean isDistribute=false;
        if(distribute!=null){
            isDistribute = distribute;
        }

        if(isDistribute){
            List<String> nodes = NodeWS.getInstance().getInstancesURL();
            String myInstance = NodeWS.getInstance().getMyInstance();
            Result finalResult = null;
            for(String instance:nodes){
                Result currentResult=null;
                if(!instance.equals(myInstance)){
                    MultivaluedMap<String, Object> queryParams = new MultivaluedMapImpl<>();
                    queryParams.add("distribute", "false");
                    queryParams.add("limit", String.valueOf(limit));
                    currentResult = gson.fromJson(
                            NodeWS.getInstance().doBasicRequestPost(instance+"/api/request/select", query,
                                    MediaType.TEXT_PLAIN,
                                    queryParams),
                            Result.class);
                }else{
                    currentResult = this.doQuerySelect(query, limit,false);
                }
                if(finalResult==null){
                    finalResult = currentResult;
                }else{
                    finalResult.setCount(finalResult.getCount() + currentResult.getCount());
                    finalResult.getLines().addAll(currentResult.getLines());
                }
            }
            if(finalResult==null){
                return Result.builder().rawQuery(query).lines(new ArrayList<>()).build();
            }else{
                return finalResult;
            }
        }else{
            try{
                QuerySelect querySelect = new QuerySelect(query, limit);
                querySelect.execute();

                return querySelect.getResult();
            }catch (Exception e){
                System.err.println("Exception :"+e.getMessage());
                return Result.builder().rawQuery(query).lines(new ArrayList<>()).build();
            }
        }
    }

    @POST
    @Path("/insert")
    @Consumes(MediaType.TEXT_PLAIN)
    public Response doQueryInsert(final String query){
        try{
            QueryInsert queryInsert = new QueryInsert(query);
            queryInsert.execute();
            return Response.ok("insertion ok").build();
        }catch(IllegalArgumentException e){
            throw new RuntimeException("Erreur lors de la création de la query, de type IllegalArgumentException : "+
                    e.getMessage());
        }catch(Exception e){
            throw new RuntimeException("Erreur lors de la création de la query, de type IllegalArgumentException : "+
                    e.getMessage());
        }
    }


}
