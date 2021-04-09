package com.dant.entity.datastruct;

import com.dant.webservices.FileSystemWS;
import com.dant.webservices.Utils;
import lombok.Getter;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BPTreeImplSemiDiskStored extends BPTreeImpl {

    @Getter
    private final String basePath;
    @Getter
    private final String listsFilename;
    @Getter
    private RandomAccessFile listsFile;

    private byte[] buffer;
    private ByteBuffer bufferForInt;

    public BPTreeImplSemiDiskStored(final String basePath, final String listsFilename)throws Exception{
        super();
        allowDuplicates = false;
        this.basePath = basePath;
        File base = new File(basePath);
        base.mkdirs();
        this.listsFilename = listsFilename;
        try{
            openListsFile();
        }catch (Exception e){
            System.err.println("Exception in BPTreeImplSemiDiskStored creation with message : "+e.getMessage());
            throw e;
        }
    }



    @Override public void treatDuplicateKey(final int position, final int value, final Node leaf){
        try{
            int newHeadList = saveValue(value, leaf.childs[position]);
            leaf.childs[position] = newHeadList;
            saveNode(leaf);
        }catch (Exception e){
            System.err.println("Error while saving value "+value+" in index of filename "+ listsFilename +" with message :"
                    +e.getMessage());
        }
    }
    @Override public List<Integer> getAllValuesInPos(final Node leaf, final int pos){
        List<Integer> result = new ArrayList<>();
        if(leaf==null){
            return result;
        }
        boolean stillRemains = true;
        this.buffer = new byte[Integer.BYTES];
        try{
            long curPosition = leaf.childs[pos] * (2 * Integer.BYTES);
            while(stillRemains){
                listsFile.seek(curPosition);
                listsFile.read(buffer);
                result.add(Utils.bytesToInt(buffer));
                listsFile.read(buffer);
                int next = Utils.bytesToInt(buffer);
                if(next == -1){
                    stillRemains = false;
                }else{
                    curPosition = next * (2 * Integer.BYTES);
                }
            }
        }catch (Exception e){
            System.err.println("Exception while get all values for key "+leaf.keys[pos]);
        }
        return result;
    }

    public int saveValue(final int value, final int next){
        try{
            long position = listsFile.length();
            listsFile.seek(position);

            listsFile.write(Utils.intToBytes(value));
            listsFile.write(Utils.intToBytes(next));

            return (int)(position/(2 * Integer.BYTES));
        }catch(Exception e){
            System.err.println("Error while saving value "+value+" in index of filename "+ listsFilename +" with message :"+
                    e.getMessage());
            return -1;
        }
    }
    @Override public int saveValue(int value){
        return saveValue(value, -1);
    }

    public boolean isListsFileOpen(){
        try{
            listsFile.length();
            return true;
        }catch(Exception e){
            return false;
        }
    }
    public void openListsFile()throws Exception{
        listsFile = new RandomAccessFile(basePath + FileSystemWS.FILE_SEP + listsFilename, "rw");
    }
    public void closeListsFile()throws Exception{
        listsFile.close();
    }



}
