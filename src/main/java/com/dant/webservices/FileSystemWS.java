package com.dant.webservices;

import com.dant.entity.Index;
import com.dant.entity.Table;
import lombok.Getter;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Path("/api/fs")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class FileSystemWS {

    public static String APP_DIR_NAME = ".moteur_indexation";
    public static String FILE_SEP = System.getProperty("file.separator");
    public static String HOME = System.getProperty("user.home");
    public static String INDEX_DIR_NAME = "indexes";

    public void init(){}

    private FileSystemWS(){init();}
    private static FileSystemWS INSTANCE;
    public static FileSystemWS getInstance(){
        if(INSTANCE==null)INSTANCE = new FileSystemWS();
        return INSTANCE;
    }

    @GET
    @Path("/load")
    public Response loadTablesObject(@QueryParam("instanceName")final String instanceName){
        final String appRootDirPath = HOME+ FILE_SEP+ APP_DIR_NAME+
                FILE_SEP+instanceName;
        File appRootDir = new File(appRootDirPath);
        if(appRootDir.isDirectory()){
            try{
                ObjectInputStream tablesFile = new ObjectInputStream( new FileInputStream(appRootDirPath+
                        FILE_SEP+ "tables.bin"
                ));
                ArrayList<Table> tables =(ArrayList<Table>) tablesFile.readObject();
                TableWS.getInstance().setTables(tables);
                tablesFile.close();
                return Response.ok("Data well loaded").build();
            }catch(Exception ex){
                ex.printStackTrace();
                throw new RuntimeException("Error in load function with message "+ex.getMessage());
            }
        }else{
            throw new RuntimeException("There is no "+appRootDirPath+" directory");
        }
    }

    @GET
    @Path("/save")
    public Response saveInstanceTablesObject(@QueryParam("instanceName")final String instanceName){

        final String filePath = System.getProperty("user.home")+FILE_SEP+APP_DIR_NAME+
                FILE_SEP+((instanceName==null||instanceName.isEmpty())?"":(instanceName+FILE_SEP))+"tables.bin";
        try{
            ArrayList<Table> tables = TableWS.getInstance().getTables();
            final ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(new File(filePath)));
            output.writeObject(tables);
            output.close();
            return Response.ok("Saved instance ok "+instanceName).build();
        }catch(Exception e){
            throw new RuntimeException("Error in save function with message "+e.getMessage());
        }

    }

    @Deprecated
    public void prepareTableDir(final String tableName,final String instance)throws Exception{
        if(tableName==null)return;
        try{
            Table table = TableWS.getInstance().getOneTable(tableName);
            prepareTableDir(table, instance);
        }catch(Exception e){
            String appRootDirPath = System.getProperty("user.home")+FILE_SEP+APP_DIR_NAME;
            File appRootDir = new File(appRootDirPath);
            if(!appRootDir.isDirectory()) {
                appRootDir.mkdir();
            }
            if(instance!=null && !instance.isEmpty()){
                appRootDirPath += FILE_SEP+instance;
                appRootDir = new File(appRootDirPath);
                if(!appRootDir.isDirectory()){
                    appRootDir.mkdir();
                }
            }
            final String tableDirPath = appRootDirPath+FILE_SEP+tableName;
            final File tableDir = new File(tableDirPath);
            if(tableDir.isDirectory()){
                throw new RuntimeException("A directory already exists for the table "+tableName);
            }else{
                tableDir.mkdir();
                final String srcDataFilePath = tableDirPath+FILE_SEP+"data.csv";
                final File srcDataFile = new File(srcDataFilePath);
                srcDataFile.createNewFile();
            }
        }
    }

    @Deprecated
    public void prepareTableDir(final Table table, final String instance )throws Exception{
        String appRootDirPath = System.getProperty("user.home")+FILE_SEP+APP_DIR_NAME;
        File appRootDir = new File(appRootDirPath);
        if(!appRootDir.isDirectory()) {
            appRootDir.mkdir();
        }
        if(instance!=null && !instance.isEmpty()){
            appRootDirPath += FILE_SEP+instance;
            appRootDir = new File(appRootDirPath);
            if(!appRootDir.isDirectory()){
                appRootDir.mkdir();
            }
        }
        final String tableDirPath = appRootDirPath+FILE_SEP+table.getName();
        final File tableDir = new File(tableDirPath);
        if(tableDir.isDirectory()){
            throw new RuntimeException("A directory already exists for the table "+table.getName());
        }else{
            tableDir.mkdir();
            final String indexDirPath = tableDirPath+FILE_SEP+"index";
            final File indexDir = new File(indexDirPath);
            indexDir.mkdir();
            final String srcDataFilePath = tableDirPath+FILE_SEP+"data.csv";
            final File srcDataFile = new File(srcDataFilePath);
            srcDataFile.createNewFile();
            List<Index> indexes;
            if((indexes=table.getIndexes())!=null){
                String indexFilePath;
                StringBuffer indexFileName;
                File indexFile;
                for(Index index : indexes){
                    indexFileName = new StringBuffer();
                    for(byte indexPos : index.getColumnsPositions()){
                        indexFileName.append(indexPos).append("_");
                    }
                    indexFileName.append("index.bin");
                    indexFilePath = indexDirPath+FILE_SEP+indexFileName.toString();
                }
            }
        }
    }

    @Deprecated
    public void prepareTableDir(final Table table)throws Exception{
        prepareTableDir(table, NodeWS.getInstance().getMyInstanceName());
    }

    @Deprecated
    public void prepareTableDir(final String tableName)throws Exception{
        prepareTableDir(tableName, NodeWS.getInstance().getMyInstanceName());
    }

    public String getDirTable(final Table table){
        if(table==null)return "";
        return getDirTable(table.getName());
    }
    public String getDirTable(final Table table, final String instance){
        if(table==null)return "";
        return getDirTable(table.getName(), instance);
    }
    public String getDirTable(final String tableName){
        return getDirTable(tableName, NodeWS.getInstance().getMyInstanceName());
    }
    public String getDirTable(final String tableName, final String instance){
        if(tableName==null)return  "";
        StringBuilder base= new StringBuilder(System.getProperty("user.home")+FILE_SEP+APP_DIR_NAME+FILE_SEP);
        if(instance!=null && !instance.isEmpty()){
            base.append(instance+FILE_SEP);
        }
        base.append(tableName);
        return base.toString();
    }

}
