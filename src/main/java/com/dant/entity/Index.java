package com.dant.entity;

import com.dant.entity.datastruct.BPTreeImplFullDiskStored;
import com.dant.entity.datastruct.BPTreeImplSemiDiskStored;
import com.dant.entity.datastruct.IndexDataStructure;
import com.dant.webservices.FileSystemWS;
import lombok.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
@EqualsAndHashCode
@Data
public class Index implements Serializable {
    private int numberOfValues;
    private transient IndexDataStructure index;
    final private byte[] columnsPositions;
    private transient  Table table;

    public Index(final IndexDataStructure indexDataStructure, final Table table, final byte[] columnsPositions){
        this.columnsPositions = columnsPositions;
        this.index = indexDataStructure;
        this.table = table;
        this.numberOfValues = 0;
    }

    /**A function that is doing the indexation*/
    public void doIndexation(){
        doIndexation(columnsPositions);
    }
    private void doIndexation(final byte[] columnPositions){
        int i=0;
        //if(table.getLines()==null)return;
        if(columnPositions==null||columnPositions.length==0)return;
        //for(Object[] o: table.getLines()){
        table.startIterator();
        while(!table.isEndOfFile()){
            Object[] o = table.getNext();
            Object[] keys=new Object[columnPositions.length];
            for(int j=0; j<columnPositions.length;j++){
                keys[j]=o[columnPositions[j]];
            }
            try{
                insert(keys, i);
            }catch (Exception e){
                System.err.println("Error while insertion of line n°"+i+" during indexation");
            }
            i++;
        }

        //table.getFileManagers().parallelStream().forEach();

    }


    /**
     * A function to insert a line into index.
     * @param key An array of object specifiying the value of the indexed column
     * @param position the position of the indexed line inside the list of lines in the table of this index
     * */
    synchronized public void insert(final Object[] key,final int position)throws Exception {
        if(key==null)return;
        int hashKey = hash(key);
        index.insert(hashKey, position);
        numberOfValues++;
    }

    /**
     * A function to get a value from the index.
     * @param key key An array of object specifiying the value of the indexed column
     * @return the corresponding list of integers representing the positions of the lines in the table containing the key.
     * in the case of not found associated value, we return null
     *
     * */
    public List<Integer> get(final Object[] key){
        if (index==null){
            return new ArrayList();
        }
        return index.get(hash(key));
    }

    public List<Integer> get(final Where where){
        if(index == null){
            return new ArrayList();
        }else{
            return index.get(hash(new Object[]{where.getValue()}), where.getOperator());
        }
    }

    @Override
    public String toString() {
        return "Index{" +
                "numberOfValues=" + numberOfValues +
                ", columnsPositions=" + Arrays.toString(columnsPositions) +
                ", table=" + table.getName() +
                '}';
    }


    /**A function to hash the keys of the hashtable, it hash the values of an
     * Object[]
     * @param key the Object[] to hash
     * @return the result of our hash function
     * */
    public int hash(Object[] key){
        if(key==null){
            return 0;
        }
        else{
            int i=0;
            for(Object o:key){
                i+= o.hashCode();
            }
            return i;
        }
    }
}
