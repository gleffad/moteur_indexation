package com.dant.entity.datastruct;

import com.dant.webservices.FileSystemWS;
import com.dant.webservices.Utils;
import lombok.Getter;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BPTreeImplFullDiskStored extends BPTreeImplSemiDiskStored{
    @Getter
    private final String filenameNodes;
    @Getter
    private RandomAccessFile nodesFile;

    private ByteBuffer bufferForInt;
    private byte[] bufferInt = new byte[Integer.BYTES];

    public BPTreeImplFullDiskStored(String basePath, String filename, String filenameNodes) throws Exception {
        super(basePath, filename);
        this.filenameNodes = filenameNodes;
        try{
            openNodesFile();
        }catch (Exception e){
            System.err.println("Exception in BPTreeImplFullDiskStored creation with message : "+e.getMessage());
            throw e;
        }
    }

    @Override
    public  Node getNodeAt(int position){
        if(position >= nbNodes){
            return null;
        }
        try{
            long pos = fromLogicalToPhysicalPosition(position);
            nodesFile.seek(pos);
            return read();
        }catch (Exception e){
            System.err.println("Error while trying to getNodeAt("+position+"), with message : "+e.getMessage());
            return null;
        }
    }
    @Override
    public  int appendNode(Node node){
        try{
            int res = nbNodes;
            nodesFile.seek(nodesFile.length());
            write(node);
            nbNodes++;
            return res;
        }catch (Exception e){
            System.err.println("Error while trying to appendNode("+node+"), with message : "+e.getMessage());
            return -1;
        }
    }
    @Override
    public  void saveNode(Node node, int position){
        if(position >= nbNodes){
            appendNode(node);
        }else{
            try{
                nodesFile.seek(fromLogicalToPhysicalPosition(position));
                write(node);
            }catch (Exception e){
                System.err.println("Exception while trying to saveNode("+node+", "+position+"), with message : "
                        +e.getMessage());
            }
        }
    }

    private void write(Node node) throws Exception{
        nodesFile.write(Utils.intToBytes(node.id));
        nodesFile.write(Utils.intToBytes(node.n));
        nodesFile.write(Utils.intToBytes(node.next));
        for (int i = 0; i < BPTreeImpl.M; i++) {
            nodesFile.write(Utils.intToBytes(node.keys[i]));
        }
        for (int i = 0; i < BPTreeImpl.M+1; i++) {
            nodesFile.write(Utils.intToBytes(node.childs[i]));
        }
    }
    private Node read() throws Exception{
        nodesFile.read(bufferInt);
        int id = Utils.bytesToInt(bufferInt);
        Node result = new Node(id);
        nodesFile.read(bufferInt);
        int n = Utils.bytesToInt(bufferInt);
        result.n = n;
        nodesFile.read(bufferInt);
        int next = Utils.bytesToInt(bufferInt);
        result.next = next;
        for (int i = 0; i < BPTreeImpl.M; i++) {
            nodesFile.read(bufferInt);
            result.keys[i] = Utils.bytesToInt(bufferInt);
        }
        for (int i = 0; i < BPTreeImpl.M+1; i++) {
            nodesFile.read(bufferInt);
            result.childs[i] = Utils.bytesToInt(bufferInt);
        }
        return result;
    }

    public boolean isNodesFileOpen(){
        try{
            nodesFile.length();
            return true;
        }catch(Exception e){
            return false;
        }
    }
    public void openNodesFile()throws Exception{
        nodesFile = new RandomAccessFile(getBasePath() + FileSystemWS.FILE_SEP + filenameNodes, "rw");
    }
    public void closeNodesFile()throws Exception{
        nodesFile.close();
    }

    private long fromLogicalToPhysicalPosition(int logicalPosition){
        return logicalPosition * Node.BYTES_SIZE;
    }




}