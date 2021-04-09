package com.dant.webservices;


import com.dant.entity.Table;
import lombok.Getter;
import org.apache.http.client.utils.URIBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.*;
import java.io.*;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;

@Path("/api/node")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NodeWS {

    @Getter
    private List<String> instancesURL = new ArrayList<>();

    @Getter
    private String myInstance="";

    private String URL = "test";

    private void init(){
        //myInstance = "test";
    }
    private NodeWS(){
        init();
    }
    private static  NodeWS INSTANCE;
    public static NodeWS getInstance(){
        if(INSTANCE==null)INSTANCE =new NodeWS();
        return INSTANCE;
    }

    @GET
    @Path("/getall")
    public Response getAllNode() {
        try{
            String retour = readSeeting();
            return Response.ok(retour).build();
        }catch (IOException e){
            System.out.println("ERROR" + e);
        }
        return Response.status(400).build();
    }

    public String getMyInstanceName(){
        if(myInstance==null || myInstance.isEmpty())return "";
        String result = myInstance.replace("http://","").replace("https://","")
                .replace(":","");
        return result;
    }

    private void writeSeeting(String txt) throws IOException {
        File file = new File("src/main/resources/Node");
        RandomAccessFile in = null;
        in = new RandomAccessFile(file, "rw");
        FileLock lock = in.getChannel().lock();
        FileOutputStream out = new FileOutputStream(file,true);
        lock.release();
        String val = txt+'\n';
        out.write(val.getBytes());
    }


    private String readSeeting()throws IOException{

        File file = new File("src/main/resources/Node");
        RandomAccessFile in = null;
        in = new RandomAccessFile(file, "rw");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            String retour = "";
            FileLock lock = in.getChannel().lock();
            while ((line = br.readLine()) != null) {
                // process the line.
                System.out.println(line);
                retour += line+'\n';
            }
            lock.release();
            return retour;
        }



    }

    public String doBasicRequestGet(String url){
        return doBasicRequestGet(url, new MultivaluedMapImpl<>());
    }
    public String doBasicRequestGet(String url,MultivaluedMap<String, Object>queryParams){
        ResteasyClientBuilder c = new ResteasyClientBuilder();
        ResteasyClient client = c.build();
        ResteasyWebTarget target = client.target(UriBuilder.fromPath(url)).queryParams(queryParams);
        return get(target);
    }
    private String get(final ResteasyWebTarget target){
        Response response;
        response = target.request().get();
        String entity = response.readEntity(String.class);
        if(response.getStatus()!=200)throw new RuntimeException(response.getStatusInfo()+" "+response.getHeaders()+" "+response.getEntity());
        //Object entity = response.getEntity();
        response.close();
        return entity;
    }


    public String doBasicRequestPost(String url, Object body){
        return doBasicRequestPost(url, body, MediaType.APPLICATION_JSON);
    }
    public String doBasicRequestPost(String url, Object body, String mediaTypeSent){
        return doBasicRequestPost(url, body, mediaTypeSent, new MultivaluedMapImpl<>());
    }
    public String doBasicRequestPost(String url, Object body, String mediaTypeSent, MultivaluedMap<String,Object>queryParams){
        ResteasyClientBuilder c = new ResteasyClientBuilder();
        ResteasyClient client = c.build();
        ResteasyWebTarget target = client.target(UriBuilder.fromPath(url)).queryParams(queryParams);
        return post(body, mediaTypeSent, target);
    }
    private String post(Object body, String mediaTypeSent, ResteasyWebTarget target) {
        Response response=null;
        response = target.request().post(Entity.entity(body, mediaTypeSent));
        String entity=null;
        if(response!=null){
            if(response.getStatus()/100!=2 )throw new RuntimeException(response.getStatusInfo()+" "+response.getHeaders()+" "+response.getEntity());

            entity = response.readEntity(String.class);
            response.close();
        }
        return entity;
    }




    @GET
    @Path("/init")
    public Response initialize(@Context UriInfo uriInfo){
        myInstance = uriInfo.getAbsolutePath().getScheme()+"://"+uriInfo.getAbsolutePath().getAuthority();
        if(!instancesURL.contains(myInstance)){
            instancesURL.add(myInstance);
        }
        return Response.ok(myInstance).build();
    }

    @GET
    @Path("/get/infos")
    public Response getInfos(@Context UriInfo uriInfo){
        if(myInstance==null || myInstance.isEmpty()){
            initialize(uriInfo);
        }
        return Response.ok(myInstance).build();
    }

    @POST
    @Path("/add/instance/dist")
    public Response addNewInstanceDist(final String url, @Context UriInfo uriInfo){
        //TODO Ajouter une instance à partir de son URL
        addNewInstance(url);
        String me=(String)getInfos(uriInfo).getEntity();
        for(String node:instancesURL){
            if(!me.equals(node)){
                String path = node+"/api/node/add/instance";
                ResteasyClientBuilder c = new ResteasyClientBuilder();
                ResteasyClient client = c.build();
                ResteasyWebTarget target = client.target(UriBuilder.fromPath(path));
                Response response = target.request().post(Entity.json(url));
                String value = response.readEntity(String.class);
                response.close();
            }
        }

        return Response.ok(instancesURL).build();
    }
    @POST
    @Path("/add/instance")
    public Response addNewInstance(final String url){
        //TODO Ajouter une instance à partir de son URL
        if(!instancesURL.contains(url)){
            instancesURL.add(url);
        }
        return Response.ok(instancesURL).build();
    }


    @GET
    @Path("/get/all")
    public Response getAllCurrentInstances(){
        //TODO Récupérer toutes les instances actuelles
        return Response.ok(instancesURL).build();
    }

    @POST
    @Path("/delete/instance/dist")
    public Response deleteInstanceDist(final String url, @Context UriInfo uriInfo){
        //TODO supprimer une instance
        deleteInstance(url);
        String me=(String)getInfos(uriInfo).getEntity();
        for(String node : instancesURL){
            if(!node.equals(me)){
                Client client = ClientBuilder.newBuilder().build();
                WebTarget target = client.target(node+"/delete/instance");
                Response response = target.request().post(Entity.json(url));
                String value = response.readEntity(String.class);
                response.close();  // You should close connections!
            }
        }
        return Response.ok(instancesURL).build();
    }

    @POST
    @Path("/delete/instance")
    public Response deleteInstance(final String url){
        //TODO supprimer une instance
        if(instancesURL.contains(url)){
            if(myInstance!=null && !myInstance.equals(url)){
                instancesURL.remove(url);
            }
        }
        return Response.ok(instancesURL).build();
    }

}
