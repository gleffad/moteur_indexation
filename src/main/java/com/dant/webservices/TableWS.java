package com.dant.webservices;


import com.dant.entity.Column;
import com.dant.entity.Table;

import javax.annotation.PostConstruct;
import javax.ws.rs.*;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.Setter;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

@Path("/api/table")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class TableWS {

    public void init(){
        //createTable("table1", new Column[]{
        //        Column.builder().name("c1").type("int").build(),
        //        Column.builder().name("c2").type("float").build()});
    }

    private TableWS(){
        init();
    }
    private static TableWS INSTANCE;

    public static TableWS getInstance(){
        if(INSTANCE==null)INSTANCE = new TableWS();
        return INSTANCE;

    }

    @Getter
    @Setter
    private ArrayList<Table> tables=new ArrayList<>();

    private Gson gson = new GsonBuilder().serializeNulls().create();


    /**A method to create a table from its name and column names
     * @param tableName name of the table
     * @param distribute boolean to distribute table creation
     * @param columnNames names of the columns, we suppose that all types are any
     * @return Created table
     * */
    public Response createTableWithColumnNames(String tableName, final Boolean distribute, String[] columnNames){
        Column[] columns=null;
        if(columnNames!=null){
            columns = new Column[columnNames.length];
            for (int i = 0 ; i < columnNames.length ; i++){
                columns[i] =  Column.builder().name(columnNames[i].toUpperCase()).type("STRING").build();
            }
        }
        if(tableName!=null)tableName=tableName.toUpperCase();
        return createTable(tableName, distribute, columns);

    }


    /**
     * A web service to create a table from its name (passed as Query param), and its columns (passed in Body)
     * @param name the name of table
     * @param distribute boolean to precise if we want to distribute this creation to all nodes, default value is true
     * @param columns the columns of table
     * @return
     * */
    @POST
    @Path("/createtable/json")
    public Response createTable(@QueryParam("name")String name,
                                @QueryParam("distribute")@DefaultValue("true")final Boolean distribute,
                                final Column[] columns){
        if(name==null || name.isEmpty()){
            throw new IllegalArgumentException("Cannot create a table without a specified name");
        }
        if(columns==null || columns.length==0){
            throw new IllegalArgumentException("Cannot create a table without columns");
        }
        HashSet<Column> columnHashSet=new HashSet<>();
        for(Column c:columns){
            if(c.getName()==null || c.getName().isEmpty())throw new IllegalArgumentException("Cannot create a column without name");
            if(c.getType()==null || c.getType().isEmpty())throw new IllegalArgumentException("Cannot create a column without type");
            c.setName(c.getName().toUpperCase());
            c.setType(c.getType().toUpperCase());
            columnHashSet.add(c);
        }
        if(columnHashSet.size()>columns.length)throw new IllegalArgumentException("Cannot create columns in the same table with duplicate names");

        name = name.toUpperCase();

        for(Table t:tables){
            if(name.equals(t.getName()))throw new IllegalArgumentException("A table with a similar name is already existing");
        }

        if(distribute){
            List<String> nodes = NodeWS.getInstance().getInstancesURL();
            String myInstance = NodeWS.getInstance().getMyInstance();
            for(final String instance : nodes ){
                if(!myInstance.equals(instance)){
                    MultivaluedMap<String, Object> queryParams = new MultivaluedMapImpl<>();
                    queryParams.add("name",name);
                    queryParams.add("distribute", "false");
                    NodeWS.getInstance().doBasicRequestPost(
                            instance+"/api/table/createtable/json",
                            gson.toJson(columns),
                            MediaType.APPLICATION_JSON,
                            queryParams
                    );
                }
            }
        }

        try{
            Table t = new Table(name, columns);
            tables.add(t);
            return Response.ok(t).build();

        }catch (Exception e){
            System.err.println("Exception when trying to create table with message "+e.getMessage());
            return null;
        }
    }


    /**
     * A web service to create a table from its name (passed as Query param), and its columns (passed through a
     * JSON file)
     * @param name the name of table
     * @param distribute boolean to precise if we want to distribute this creation to all nodes, default value is true
     * @param jsonColumns the JSON file containing columns informations
     * */
    @POST
    @Path("/createtable/json/file")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response createTable(@QueryParam("name")final String name,
                                @QueryParam("distribute")@DefaultValue("true")final Boolean distribute,
                                final MultipartFormDataInput jsonColumns){
        Gson gson = new Gson();
        Column[] columns;
        try{
            columns = gson.fromJson(jsonColumns.getParts().get(0).getBodyAsString(), Column[].class);
            for(Column c:columns){
                c.setName(c.getName().toUpperCase());
                c.setType(c.getType().toUpperCase());
            }

        }catch (Exception e){
            throw new RuntimeException("Error while parsing json file, please enter a valid json file");
        }
        return createTable(name, distribute, columns);
    }


    /**
     * A web service to create a table from a table template, directly the object Table
     * @param distribute boolean to precise if we want to distribute this creation to all nodes, default value is true
     * @param tableTemplate the table template we want to create
     * */
    @POST
    @Path("/createtable/fromTemplate")
    public void createTable(Table tableTemplate,
                            @QueryParam("distribute")@DefaultValue("true")final Boolean distribute){
        if(tableTemplate!=null){
            if(tableTemplate.getName()!=null){
                tableTemplate.setName(tableTemplate.getName().toUpperCase());
            }
            if(tableTemplate.getColumns()!=null){
                for(Column c:tableTemplate.getColumns()){
                    c.setName(c.getName().toUpperCase());
                    c.setType(c.getType().toUpperCase());
                }
            }

            Table created =(Table) createTable(tableTemplate.getName(), distribute, tableTemplate.getColumns()).getEntity();
            created.setIndexes(tableTemplate.getIndexes());
        }
    }


    /**
     * A web service to get all the tables
     * @return The list of all tables
     * */
    @GET
    @Path("/getall")
    public List<Table> getAllTables(){
        if(tables==null)return new ArrayList<>();
        return tables;
    }


    /**
     * A web service to get one table
     * @param name name of the table
     * @return The requested table
     * */
    @GET
    @Path("/getone")
    public Table getOneTable(@QueryParam("name")String name){
        if(name==null || name.isEmpty())return null;
        if(name!=null){
            name = name.toUpperCase();
            for(Table table:tables){
                if(name.equals(table.getName())){
                    return table;
                }
            }
        }
        throw new RuntimeException("Table "+name+" non trouvée");
    }


    /**
     * A web service to get the columns informations of a table
     * @param name name of the table
     * @return the columns of the table
     * */
    @GET
    @Path("/getone/informations")
    public Table getOneTableInformations(@QueryParam("name") String name){
        if(name==null || name.isEmpty())return null;
        if(name!=null){
            for(Table table:tables){
                name = name.toUpperCase();
                if(name.equals(table.getName())){
                    return Table.builder().columns(table.getColumns()).name(table.getName()).build();
                }
            }
        }
        throw new RuntimeException("Table "+name+" non trouvée");
    }


    /**
     * A web service to check whether a table is existant or not
     * @param tableName the name of searched table
     * @return true if and only if the table exists
     * */
    @GET
    @Path("/isExistant")
    public Response isAvailable(@QueryParam("tableName") String tableName){
        Boolean result=false;
        if(tableName!=null){
            for(Table table:tables){
                tableName = tableName.toUpperCase();
                if(tableName.equals(table.getName())){
                    result=true;
                    break;
                }
            }
        }
        return Response.ok().entity(result).build();
    }


    /**A web service to add columns in a table*/
    @POST
    @Path("/columns/add")
    public Response addColumns(@QueryParam("tableName") String tableName,
                               @QueryParam("distribute")@DefaultValue("true")final Boolean distribute,
                               final Column[] columns){
        if(columns==null || columns.length==0 || tableName==null || tableName.trim().isEmpty()){
            return Response.ok().entity("Request OK, but no table influed").build();
        }else{
            //On ajoute la colonne au niveau de notre table
            final Table theTable = TableWS.getInstance().getOneTable(tableName);
            int originalLength = theTable.getColumns().length;
            theTable.setColumns(Arrays.copyOf(theTable.getColumns(), theTable.getColumns().length+columns.length));
            for(Column column:columns){
                theTable.getColumns()[originalLength++] = column;
            }
            if(distribute){
                //On distribue la requête aux autres noeuds
                List<String> nodes = NodeWS.getInstance().getInstancesURL();
                String myInstance = NodeWS.getInstance().getMyInstance();
                for(String instance :nodes){
                    if(!myInstance.equals(instance)){
                        MultivaluedMap<String, Object> queryParams = new MultivaluedMapImpl<>();
                        queryParams.add("tableName", tableName);
                        queryParams.add("distribute", "false");
                        boolean theTableExists = gson.fromJson(
                                NodeWS.getInstance().doBasicRequestGet(instance+"/api/table/isExistant",queryParams),
                                Boolean.class
                        );
                        if(theTableExists){
                            NodeWS.getInstance().doBasicRequestPost(instance+"/api/table/columns/add",
                                    gson.toJson(columns),
                                    MediaType.APPLICATION_JSON,
                                    queryParams);
                        }
                    }
                }
            }
            return Response.ok().entity(theTable).build();
        }
    }
}
