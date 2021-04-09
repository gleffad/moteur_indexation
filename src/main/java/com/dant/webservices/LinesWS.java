package com.dant.webservices;

import com.dant.entity.Column;
import com.dant.entity.QuerySelect;
import com.dant.entity.Table;
import com.dant.entity.combinedbeans.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.Value;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.resteasy.util.GenericType;

import javax.annotation.PostConstruct;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Path("/api/line")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LinesWS {

    private LinesWS(){}
    private static LinesWS INSTANCE;
    public static LinesWS getInstance(){
        if(INSTANCE==null)INSTANCE = new LinesWS();
        return INSTANCE;
    }

    public static final long DISTRIBUTE_LINES_INTERVAL=Utils.DEFAULT_CHUNK_SIZE;
    private Gson gson = new GsonBuilder().serializeNulls().create();

    /**
     * A Web service to insert and create table from CSV file
     * @param tableName the name of concerned table
     * @param create a boolean specifiying if we need to create the table,
     */

    @POST
    @Path("/createAndInsert/csv/file")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response createTableAndInsertLinesAsCSVFile(@QueryParam("tableName")final String tableName,
                                                       @QueryParam("create")final Boolean create,
                                                       @QueryParam("fileWithHeaders")final Boolean fileWithHeaders,
                                                       @QueryParam("distribute") @DefaultValue("false") final Boolean distribute,
                                                       final MultipartFormDataInput CSVLines)throws Exception{
        Table latable = null;
        if(tableName==null || tableName.isEmpty()){
            throw new IllegalArgumentException("Cannot create or insert in an uknown table");
        }
        boolean isCreate,isWithHeaders, isDistribute;
        if(create==null){
            isCreate = false;
        }else{
            isCreate = create;
        }
        if(fileWithHeaders==null){
            isWithHeaders=false;
        }else{
            isWithHeaders = fileWithHeaders;
        }
        if(distribute==null){
            isDistribute=false;
        }else{
            isDistribute = distribute;
        }
        if(isCreate && !isWithHeaders){
            throw new IllegalArgumentException("Cannote create a table from csv file without headers");
        }

        InputPart ip;
        InputStream is=null;
        BufferedReader br=null;
        CSVReader csvReader=null;
        try{

            if(CSVLines != null){
                ip = CSVLines.getParts().get(0);
                is = ip.getBody(InputStream.class, null);
                br = new BufferedReader(new InputStreamReader(is));
                CSVParser parser = new CSVParserBuilder()
                        .withEscapeChar('\\')
                        .withSeparator(',')
                        .withIgnoreQuotations(true)
                        .withQuoteChar('"')
                        .build();
                CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(br).withCSVParser(parser);
                csvReader = csvReaderBuilder.build();
                String[] colms=null;
                if(isWithHeaders){
                    colms= csvReader.readNext();
                }
                String line;
                if(isCreate){
                    latable = (Table)TableWS.getInstance().createTableWithColumnNames(tableName, false, colms)
                            .getEntity();
                }else{
                    try{
                        latable = TableWS.getInstance().getOneTable(tableName);
                    }catch(Exception e){
                        csvReader.close();
                        br.close();
                        is.close();
                        throw new IllegalArgumentException("Table "+tableName+" not found");
                    }
                }

                List<String> nodes = NodeWS.getInstance().getInstancesURL();
                String myInstance = NodeWS.getInstance().getMyInstance();
                if(isDistribute){
                    //In case we want to distribute, we prepare the other instances to receive data :)
                    ExecutorService pool = Executors.newFixedThreadPool(Utils.NB_THREADS);
                    for(String instance:nodes){
                        if(!myInstance.equals(instance)){
                            final Table finalLatable = latable;
                            Runnable runnable = () -> {
                                MultivaluedMap<String, Object> queryParams = new MultivaluedMapImpl<>();
                                queryParams.add("tableName",tableName);
                                boolean theTableExists = gson.fromJson(
                                        NodeWS.getInstance().doBasicRequestGet(instance+"/api/table/isExistant",queryParams),
                                        Boolean.class
                                );
                                if(!theTableExists){
                                    MultivaluedMap<String, Object> queryParams2 = new MultivaluedMapImpl<>();
                                    queryParams2.add("distribute", false);

                                    NodeWS.getInstance().doBasicRequestPost(instance+"/api/table/createtable/fromTemplate",
                                            gson.toJson( Table.builder()
                                                    .name(finalLatable.getName())
                                                    .columns(finalLatable.getColumns())
                                                    .indexes(finalLatable.getIndexes())
                                                    //On n'envoie pas les lignes
                                                    .build()),
                                            //On envoie le body en format json
                                            MediaType.APPLICATION_JSON,
                                            queryParams2);
                                }
                            };
                            pool.execute(runnable);
                        }
                    }
                    pool.shutdown();
                    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                }

                //int numberOfNodes = nodes.size();
                boolean end=false;
                int loop=(int)DISTRIBUTE_LINES_INTERVAL;
                if(isDistribute){
                    String[] linesOfLines = new String[loop];
                    while(!end){
                        for (int i = 0; i < loop; i++) {
                            if((line=br.readLine())==null){
                                end=true;
                                break;
                            }
                            if(!line.isEmpty()){
                                insertOneLineInTableAsCsv(latable, line);
                            }
                        }
                        if(!end){
                            for(String node : nodes){
                                if(!myInstance.equals(node)){

                                    int i;
                                    for (i = 0; i < loop ; i++) {
                                        if((linesOfLines[i]=br.readLine())==null){
                                            end=true;
                                            break;
                                        }
                                    }

                                    if(i>0){
                                        Table finalLatable1 = latable;
                                        Runnable runnable = () -> {
                                            NodeWS.getInstance().doBasicRequestPost(
                                                    node+"/api/line/insert/csv/aLotOf/line/withtablename",
                                                    gson.toJson(StringAndStringArray.builder()
                                                            .string(finalLatable1.getName())
                                                            .stringArray(linesOfLines)
                                                            .build()),
                                                    MediaType.APPLICATION_JSON
                                            );
                                        };
                                        new Thread(runnable).start();
                                    }

                                }

                                if(end){
                                    break;
                                }
                            }
                        }
                    }
                }
                else{
                    while(((line = br.readLine()) != null)){
                        if(!line.isEmpty()){
                            insertOneLineInTableAsCsv(latable, line);
                        }
                    }
                    //latable.getFileManager().closeFile();
                }


                csvReader.close();
                br.close();
                is.close();


            }else{
                throw new IllegalArgumentException("CSV file null");
            }

        }catch (Exception e){
            try{
                if(csvReader!=null)csvReader.close();
                if(br!=null)br.close();
                if(is!=null)is.close();
            }catch(Exception f){
                throw new RuntimeException("Error while parsing csv file, please enter a valid csv file "+e.getMessage()+"\nIn addition to that, cannot close input file. The error message :"+f.getMessage());
            }
            if(latable!=null && isCreate){
                TableWS.getInstance().getAllTables().remove(latable);
            }
            e.printStackTrace();
            throw new RuntimeException("Error while parsing csv file, please enter a valid csv file "+e.getMessage());
        }
        return Response.ok("Insertion CSV OK").build();
    }





    /**
     * A web service to insert lines from JSON file
     * */
    @POST
    @Path("/insert/json/file")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response insertLinesInTableAsJsonFile(@QueryParam("tableName")final String tableName,
                                                 final MultipartFormDataInput jsonLines)throws Exception{
        Gson gson = new Gson();
        Object lines;
        try{
            lines = gson.fromJson(jsonLines.getParts().get(0).getBodyAsString(), Object.class);

        }catch (Exception e){
            throw new RuntimeException("Error while parsing json file, please enter a valid json file");
        }
        return insertLinesInTableAsJson(tableName, lines);
    }


    /**
     * A web service to insert line or lines in a table in JSON format from Body of http request
     * @param tableName the name of the table whereto insert lines
     * @param lines the lines or the line to be inserted
     * @return
     * */
    @POST
    @Path("/insert/json")
    public Response insertLinesInTableAsJson(@QueryParam("tableName")final String tableName, Object lines)throws Exception{
        if(tableName==null || tableName.isEmpty())throw new IllegalArgumentException("Cannot insert a line without a table name");
        if(lines!=null){
            Table table = TableWS.getInstance().getOneTable(tableName);
            byte i=0,j;
            if(lines instanceof ArrayList){
                //Trying to insert a lot of lines
                List listOfLines = ((List)lines);
                for(Object element : listOfLines){
                    Object[] lineToAdd = new Object[table.getColumns().length];
                    LinkedTreeMap<String, String> line = ((LinkedTreeMap)element);
                    insertOneLineInTableAsJson(table, line);
                }
            }else if(lines instanceof LinkedTreeMap){
                //Trying to insert only one line
                insertOneLineInTableAsJson(table, (LinkedTreeMap<String, String>) lines);
            }
        }
        return Response.ok("Insertion done").build();
    }



    public void insertOneLineInTableAsJson(final Table table,final LinkedTreeMap<String, String> line){
        byte i=0,j;
        String[] lineToAdd = new String[table.getColumns().length];
        for(LinkedTreeMap.Entry<String,String> entry : line.entrySet()){
            if(i<table.getColumns().length && table.getColumns()[i].getName().equals(entry.getKey())){
                lineToAdd[i]=entry.getValue();
            }else{
                j=table.getColumnPosition(entry.getKey());
                if(j!=-1)lineToAdd[j] = entry.getValue();
            }
            i++;
        }
        try{
            table.insert(lineToAdd);
        }catch (Exception e){
            System.err.println("Error while inserting line " + line + " in table of name : " + table.getName());
        }
    }


    /**
     * A method to insert one line in table from CSV input, the line is a supposed to be an array of String
     * @param tableAndLineStringArray A combined bean containing a table and a String[], to pass the 2 objects through the body of a single post request.
     *                                the table is where we insert the line (we just need its columns, other attributes can be null).
     *                                And the lineString a string[] containing the data of the line to insert
     * */
    @POST
    @Path("/insert/csv/one/line/withtable")
    public void insertOneLineInTableAsCsv(final TableAndString tableAndLineStringArray){
        if(tableAndLineStringArray!=null){
            Table table = tableAndLineStringArray.getTable();
            String lineString = tableAndLineStringArray.getString();
            insertOneLineInTableAsCsv(table, lineString);
        }
    }

    @POST
    @Path("/insert/csv/one/line/withtablename")
    public void insertOneLineInTableAsCsv(final StringAndString tableNameAndLineString){
        if(tableNameAndLineString!=null){
            String tableName = tableNameAndLineString.getString1();
            String line = tableNameAndLineString.getString2();
            Table table = TableWS.getInstance().getOneTable(tableName);
            insertOneLineInTableAsCsv(table, line);
        }
    }


    @POST
    @Path("/insert/csv/aLotOf/line/withtablename")
    public void insertALotOfLinesWithTableName(final StringAndStringArray stringAndStringArray){
        if(stringAndStringArray!=null){
            String tableName = stringAndStringArray.getString();
            String[] linesString = stringAndStringArray.getStringArray();
            Table table = TableWS.getInstance().getOneTable(tableName);
            insertALotOfLinesInTableAsCsv(table, linesString);
        }
    }
/*
    public void insertALotOfLinesWithTableName(final StringAndArrayOfStringArray stringAndArrayOfStringArray){
        if(stringAndArrayOfStringArray!=null){
            String tableName = stringAndArrayOfStringArray.getString();
            String[][] linesString = stringAndArrayOfStringArray.getArrayOfStringArray();
            Table table = TableWS.getInstance().getOneTable(tableName);
            insertALotOfLinesInTableAsCsv(table, linesString);
        }
    }
*/

    public void insertOneLineInTableAsCsv(Table table, String lineString){
        if(table != null && lineString!=null){
            try{
                table.insert(lineString);
            }catch (Exception e){
                System.err.println("Exception while trying to insert a line :" + lineString + " in table of name "
                        + table.getName());
            }
        }
    }
    public void insertALotOfLinesInTableAsCsv(Table table, String[] linesString){
        if(table != null && linesString!=null){
            for(String lineString:linesString){
                if(lineString!=null){
                    insertOneLineInTableAsCsv(table, lineString);
                }
            }
        }
    }


}
