package com.dant.entity;

import com.dant.entity.datastruct.*;
import com.dant.webservices.FileSystemWS;
import com.dant.webservices.Utils;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import lombok.*;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Data
@AllArgsConstructor
@Builder
@EqualsAndHashCode
@ToString
public class Table implements Serializable, Iterable<Object[]> {

    private String name;
    private Column[] columns;
    private List<Index> indexes;
    transient private List<FileManagerTable> fileManagers;
    transient private int curFileManager;
    transient private CSVParser csvParser;


    public Table(Table table){
        this(table.name, table.columns, table.indexes, table.fileManagers, table.curFileManager, table.csvParser);
    }
    public Table(final String name, final Column[] columns, final List<Index> indexes) throws Exception {
        this.name = name;
        this.columns = columns;
        this.indexes = indexes;
        fileManagers = new ArrayList<>();
        fileManagers.add(new FileManagerTableImplBasic(name, 0));
        curFileManager = 0;
        csvParser = new CSVParserBuilder()
                .withEscapeChar('\\')
                .withSeparator(',')
                .withIgnoreQuotations(true)
                .withQuoteChar('"')
                .build();

    }
    public Table(final String name, final Column[] columns) throws Exception {
        this(name, columns, new ArrayList<>());
    }

    /**
     * A function to get column names
     *@return the list of column names
     *  */
    public List<String> getColumnNames(){
        List<String> result = new ArrayList<>();
        for(Column c:columns){
            result.add(c.getName());
        }
        return result;
    }

    public String getName(){
        if(name==null)return null;
        return name.toUpperCase();
    }

    /**
     * A function to get column names at specified positions
     * @param positions a byte array of positions of wanted columns
     * @return the list of column names
     * */
    public List<String> getColumnNames(byte[] positions){
        List<String> result = new ArrayList<>();
        for(byte p:positions){
            result.add(columns[p].getName());
        }
        return result;
    }

    /**
     * A function to find the position of a column in the columns array of this table.
     * @param column the concerned column
     * @return the position of the column, -1 if the column is not found
     * */
    public byte getColumnPosition(final Column column){
        if(column==null)return -1;
        for(byte i=0; i<columns.length; i++){
            if(columns[i].getName().equals(column.getName()))return i;
        }
        return -1;
    }
    public byte getColumnPosition(String columnName){
        if(columnName==null)return -1;
        if(columns==null || columns.length==0)return -1;

        columnName=columnName.toUpperCase();

        for(byte i=0; i<columns.length; i++){
            if(columns[i].getName().equals(columnName))return i;
        }
        return -1;
    }

    /**
     * A function to find the positions of columns in the columns array of this table.
     * @param columns the concerned columns
     * @return the positions of the columns, -1 if a column is not found
     * */
    public byte[] getColumnsPositions(final Column[] columns){
        if(columns==null || columns.length==0)return null;
        byte[] result = new byte[columns.length];
        for(byte i=0; i<columns.length; i++){
            result[i] = getColumnPosition(columns[i]);
        }
        return result;
    }
    public byte[] getColumnsPositions(final String[] columnsNames) {
        if(columnsNames==null || columnsNames.length==0)return null;
        byte[] result = new byte[columnsNames.length];
        for(byte i=0; i<columnsNames.length; i++){
            result[i] = getColumnPosition(columnsNames[i]);
        }
        return result;
    }


    /**A function to create an index for specified columns*/
    public void createIndex(final Column[] columns, final String type) throws Exception {
        createIndex(getColumnsPositions(columns), type);
    }
    public void createIndex(final String[] columnsNames, final String type) throws Exception {
        createIndex(getColumnsPositions(columnsNames), type);
    }
    public void createIndex(final byte[] columnsPositions, final String type) throws Exception {
        for(int i=0; i<columnsPositions.length;i++){
            if(columnsPositions[i]==-1)throw new IllegalArgumentException("Column not found at position "+i);
        }
        StringBuilder filename = new StringBuilder();
        filename.append(name);
        for (int i = 0; i < columnsPositions.length; i++) {
            filename.append(columnsPositions[i]);
            filename.append("_");
        }
        final IndexDataStructure indexDataStructure;
        switch (type){
            case "BPTree_Full_Disk":
                indexDataStructure = new BPTreeImplFullDiskStored(
                        FileSystemWS.getInstance().getDirTable(this)+
                                FileSystemWS.FILE_SEP + FileSystemWS.INDEX_DIR_NAME ,
                        filename.toString()+"data.bin",
                        filename.toString()+"nodes.bin"
                );
                break;
            case "BPTree_Semi_Disk":
                indexDataStructure = new BPTreeImplSemiDiskStored(
                        FileSystemWS.getInstance().getDirTable(this)+
                                FileSystemWS.FILE_SEP + FileSystemWS.INDEX_DIR_NAME ,
                        filename.toString()+"data.bin"
                );
                break;
            case "BPTree_Full_Ram":
                indexDataStructure = new BPTreeImpl();
                break;
            case "Hash_Table_Full_Ram":
                indexDataStructure = new HashTableIndexRAM();
                break;
            default:
                indexDataStructure = new BPTreeImplSemiDiskStored(
                        FileSystemWS.getInstance().getDirTable(this)+
                                FileSystemWS.FILE_SEP + FileSystemWS.INDEX_DIR_NAME ,
                        filename.toString()+"data.bin"
                );
        }

        final Index index = new Index(indexDataStructure, this, columnsPositions);
        index.doIndexation();
        if(indexes==null){
            indexes=new ArrayList<>();
        }
        indexes.add(index);
    }

    /**A function to compute all the index of this table*/
    public void doIndexes(){
        for(Index index:indexes)index.doIndexation();
    }

    /**A function to get (if exists) index from columns names
     * @return Requested index, null if not found
     * */
    public Index getIndex(final String[] columnsNames){
        return getIndex(getColumnsPositions(columnsNames));
    }
    public Index getIndex(final Column[] columns){
        return getIndex(getColumnsPositions(columns));
    }
    public Index getIndex(final byte[] columnPositions){
        for(Index index:indexes){
            if(Arrays.equals(columnPositions,index.getColumnsPositions())){
                return index;
            }
        }
        return null;
    }

    /**
     * A function to insert a line in the table, and also insert it in all the indexes
     * @param line the line to insert
     * */
    public void insert(final String[] line)throws Exception{
        //if(lines==null)lines = new ArrayList<>();
        boolean thereIsNoNull=false;
        int j=0;
        while(!thereIsNoNull && j<line.length){
            if(line[j]!=null)thereIsNoNull=true;
            else j++;
        }

        if(thereIsNoNull){
            if(fileManagers.get(fileManagers.size()-1).isFull()){
                if(fileManagers.get(fileManagers.size()-1).isLoaded()){
                    fileManagers.get(fileManagers.size()-1).save();
                }
                fileManagers.add(new FileManagerTableImplBasic(name, fileManagers.size()));
            }
            int writtenPosition = fileManagers.get(fileManagers.size()-1)
                    .writeLine(Utils.getBytesFast(String.join(",",line)));

            addLineInIndexes(line, writtenPosition);
        }
    }
    public void insert(final String line)throws Exception{
        if(line == null || line.isEmpty())return;
        if(fileManagers.get(fileManagers.size()-1).isFull()){
            if(fileManagers.get(fileManagers.size()-1).isLoaded()){
                fileManagers.get(fileManagers.size()-1).save();

            }
            fileManagers.add(new FileManagerTableImplBasic(name, fileManagers.size()));
        }
        int writtenPosition = fileManagers.get(fileManagers.size()-1).writeLine(Utils.getBytesFast(line));

        if(indexes!=null){
            String[] parsedLine = csvParser.parseLine(line);
            addLineInIndexes(parsedLine, writtenPosition);
        }
    }

    public void addLineInIndexes(final String[] line, final int writtenPosition)throws Exception{
        if(indexes!=null){
            for(Index index:indexes){
                Object[] key = new Object[index.getColumnsPositions().length];
                int i;
                for(i=0;i<key.length;i++){
                    key[i]=Column.convertToType(
                            columns[index.getColumnsPositions()[i]].getType(),
                            line[index.getColumnsPositions()[i]]
                    );
                }
                index.insert(key, writtenPosition);
            }
        }
    }

    public Object[] getAtPosition(int position){
        try{
            int whichFileManager = position /(int) Utils.DEFAULT_CHUNK_SIZE;
            int offset = position %(int) Utils.DEFAULT_CHUNK_SIZE;
            return byteArrayToLine(fileManagers.get(whichFileManager).readLineAtPosition(offset));
        }catch (Exception e){
            System.err.println("Error while trying to read from table "+name+
                    " at position "+position);
            return null;
        }
    }

    public Object[] getNext(){
        //TODO à modifier
        try{
            if(fileManagers.get(curFileManager).isEndOfFile()){
                if(curFileManager == fileManagers.size()-1){
                    return null;
                }
                fileManagers.get(curFileManager).unLoad();
                curFileManager++;
                if(!fileManagers.get(curFileManager).isLoaded()){
                    fileManagers.get(curFileManager).loadAll();
                }
                fileManagers.get(curFileManager).resetPos();
            }
            return byteArrayToLine(fileManagers.get(curFileManager).readNextLine());
        }catch(Exception e){
            System.err.println("Exception while reading next line in table "+name+" with message : "+e.getMessage());
            return null;
        }
    }

    public List<Object[]> getAll(){
        List<Object[]> result = new ArrayList<>();
        try{
            startIterator();
            while( result.size() <= Utils.DEFAULT_LIMIT_RESULTS && !isEndOfFile()){
                result.add(getNext());
            }

        }catch(Exception e){
            System.err.println("Exception while trying to get all lines from table "+name+" with message : "+
                    e.getMessage());
        }
        return result;
    }

    public boolean isEndOfFile(){
        return curFileManager==fileManagers.size()-1 && fileManagers.get(fileManagers.size()-1).isEndOfFile();
    }
    public void startIterator(){
        try{
            curFileManager = 0;
            if(!fileManagers.get(0).isLoaded()){
                fileManagers.get(0).loadAll();
            }
            fileManagers.get(0).resetPos();
        }catch(Exception e){
            System.err.println("Exception while start iter in file of table "+name+" with message : "+e.getMessage());
        }
    }


    @Override
    public Iterator<Object[]> iterator() {
        return new Iterator<Object[]>() {
            @Override
            public boolean hasNext() {
                return !isEndOfFile();
            }

            @Override
            public Object[] next() {
                return getNext();
            }
        };
    }

    @Override
    public void forEach(Consumer<? super Object[]> action) {
        List<Object[]> all = getAll();
        all.forEach(action);
    }

    @Override
    @Deprecated
    public Spliterator<Object[]> spliterator() {
        return null;
    }

    public Object[] byteArrayToLine(byte[] byteArray){
        String[] splittedLine;
        Object[] result = new Object[columns.length];
        try{
            splittedLine = csvParser.parseLine(new String(byteArray));
        }catch (Exception e){
            System.err.println("Error while parsing line from byte[]");
            return result;
        }
        int j=0;
        for (int i = 0; i < columns.length;i++) {
            if(i<splittedLine.length){
                result[i] = Column.convertToType(columns[i].getType(), splittedLine[i]);
            }else{
                result[i] = null;
            }
        }
        return result;
    }




}
