package com.dant.entity;

import com.dant.webservices.FileSystemWS;
import com.dant.webservices.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.dant.webservices.Utils.bytesToLong;

@AllArgsConstructor
@EqualsAndHashCode
@Data
public class FileManagerTableImplBasic implements FileManagerTable {

    private int id;
    private int numberOfLines;
    private String tableName;
    private String basePath;

    private boolean isOpen;
    private RandomAccessFile fileRaw;
    private RandomAccessFile filePrimaryIndex;
    private int currentLogicalPosition = 0;

    private boolean isLoaded;
    private List<byte[]> rawData;
    private int totalRawSize;


    //Besoins techniques uniquement
    private ByteBuffer bufferForInt = ByteBuffer.allocate(Integer.BYTES);
    byte[] buffer;



    public FileManagerTableImplBasic(final String tableName, final int id){
        this.tableName = tableName;
        this.id = id;
        this.numberOfLines = 0;
        this.isLoaded = true;
        this.rawData = new ArrayList<>();

        isOpen = false;

        try{
            this.basePath = FileSystemWS.getInstance().getDirTable(tableName);
            File base = new File(basePath);
            base.mkdirs();
        }catch (Exception e){
            System.out.println(e.getMessage());
            throw e;
        }


    }

    @Override
    public boolean isOpen(){
        return isOpen;
    }

    @Override
    public void openIfIsNot()throws Exception{
        if(!isOpen())openFile();
    }

    @Override
    public void openFile()throws Exception {
        try{
            fileRaw = new RandomAccessFile(basePath + FileSystemWS.FILE_SEP + id +"_" +
                    Utils.RAW_DATA_FILENAME_SUFFIX, "rw");
            filePrimaryIndex = new RandomAccessFile(basePath + FileSystemWS.FILE_SEP + id + "_" +
                    Utils.PRIMARY_INDEX_FILENAME_SUFFIX, "rw");
            isOpen = true;
        }catch(Exception e){
            System.err.println("Exception while creating FileManagerTable, when opening file with message : "+
                    e.getMessage());
            throw e;
        }
    }

    @Override
    public void closeFile()throws Exception{
        try{
            isOpen = false;
            fileRaw.close();
            filePrimaryIndex.close();
        }catch(Exception e){
            System.err.println("Exception while closing file with message : "+e.getMessage());
            throw e;
        }
    }

    @Override
    public void resetPos()throws Exception{
        openIfIsNot();
        currentLogicalPosition = 0;
        filePrimaryIndex.seek(0);
        fileRaw.seek(0);
    }

    @Override
    public boolean isEndOfFile(){
        if(isLoaded){
            return currentLogicalPosition >= rawData.size();
        }else{
            if(!isOpen())return true;
            try{
                return fileRaw.getFilePointer() == fileRaw.length();
            }catch(Exception e){
                return true;
            }
        }
    }


    /**
     * Function to write a line in raw file at the end
     * @param line the line to write
     * @return  the position where the line was written, -1 in case of error
     * */
    @Override
    synchronized public int writeLine(final byte[] line){
        try{
            if(isFull()){
                numberOfLines ++;
                throw new Exception("FileManager is full");
            }else{
                numberOfLines +=1;
            }
            int logicalPosition=-1;
            if(isLoaded){
                logicalPosition = numberOfLines-1;
                totalRawSize+=line.length;
                rawData.add(line);
            }else{
                openIfIsNot();
                long position = fileRaw.length();
                long positionPrimaryIndex = filePrimaryIndex.length();
                logicalPosition = (int) (positionPrimaryIndex/Integer.BYTES);

                fileRaw.seek(position);
                fileRaw.write(line);

                filePrimaryIndex.seek(positionPrimaryIndex);
                filePrimaryIndex.write(Utils.intToBytes((int)position, bufferForInt));
            }
            return logicalPosition;
        }catch(Exception e){
            numberOfLines --;
            System.err.println("Error while writing line "+line+" in file "+ fileRaw +" , with message :"
                +e.getMessage());
            return -1;
        }
    }

    @Override
    synchronized public int writeLine(byte[][] line){
        try{
            if(isFull()){
                numberOfLines++;
                throw new Exception("FileManager is full");
            }else{
                numberOfLines ++;
            }
            int logicalPosition = -1;
            if(isLoaded){
                logicalPosition = numberOfLines - 1;
                int size=0;
                for (int i = 0; i < line.length; i++) {
                    if(line[i]!=null){
                        size+=line[i].length;
                    }
                }
                byte[] lineToAdd = new byte[size];
                totalRawSize+=size;
                int pos=0;
                for (int i = 0; i < line.length; i++) {
                    if(line[i]!=null){
                        for (int j = 0; j < line[i].length; j++) {
                            lineToAdd[pos+j] = line[i][j];
                        }
                        pos+=line[i].length;
                    }
                }
                rawData.add(lineToAdd);
            }else{
                openIfIsNot();
                long position = fileRaw.length();
                long positionPrimaryIndex = filePrimaryIndex.length();
                logicalPosition = (int) (positionPrimaryIndex/Integer.BYTES);

                fileRaw.seek(position);
                for (int i = 0; i < line.length; i++) {
                    fileRaw.write(line[i]);
                }

                filePrimaryIndex.seek(positionPrimaryIndex);
                filePrimaryIndex.write(Utils.intToBytes((int)position, bufferForInt));
            }

            return logicalPosition;
        }catch(Exception e){
            numberOfLines--;
            System.err.println("Error while writing line "+line+" in file "+ fileRaw +" , with message :"
                    +e.getMessage());
            return -1;
        }
    }

    /**Function to read a line
     * @param logicalPosition the logical position of the line in the table
     * */
    @Override
    public byte[] readLineAtPosition(final int logicalPosition){
        try{
            if(isLoaded){
                return rawData.get(logicalPosition);
            }else{
                openIfIsNot();
                long position = getRawPositionFromLogicalPosition(logicalPosition);
                int sizeOfLine = getRawSizeFromLogicalPosition(logicalPosition);

                fileRaw.seek(position);
                byte[] buffer = new byte[sizeOfLine];

                //System.out.println("Read "+ fileRaw.read(buffer, 0, sizeOfLine));
                fileRaw.read(buffer, 0, sizeOfLine);

                return buffer;
            }

        }catch(Exception e){
            System.err.println("Error while reading line at logical position "+logicalPosition+" in file "+ fileRaw +" ," +
                    " with message : "+e.getMessage());
            return null;
        }
    }

    @Override
    public byte[] readNextLine(){
        return readLineAtPosition(currentLogicalPosition++);
    }

    @Override
    public void save() throws Exception{
        Runnable runnable = ()->{
            byte[] raw = new byte[totalRawSize];
            byte[] rawPos = new byte[numberOfLines*Integer.BYTES];
            int pos = 0;
            int posPos=0;
            byte[] cur;
            int curSize=0;
            byte[] curPos;
            while(!rawData.isEmpty()){
                cur = rawData.get(0);
                curPos = Utils.intToBytes(curSize);
                curSize += cur.length;
                for (int i = 0; i < Integer.BYTES; i++) {
                    rawPos[posPos++] = curPos[i];
                }
                for (int i = 0; i < cur.length; i++) {
                    raw[pos++] = cur[i];
                }
                rawData.remove(cur);
            }
            try {
                openIfIsNot();
                fileRaw.seek(0);
                filePrimaryIndex.seek(0);
                fileRaw.write(raw);
                fileRaw.setLength(raw.length);
                filePrimaryIndex.write(rawPos);
                filePrimaryIndex.setLength(rawPos.length);
                closeFile();
                unLoad();
            } catch (Exception e) {
                e.printStackTrace();
            }

        };
        Thread thread = new Thread(runnable);
        thread.start();
    }

    @Override
    public void loadAll()throws Exception{
        openIfIsNot();
        rawData = new ArrayList<>();
        byte[] raw = new byte[(int)fileRaw.length()];
        totalRawSize = raw.length;
        fileRaw.seek(0);
        fileRaw.read(raw);

        byte[] rawPos = new byte[(int)filePrimaryIndex.length()];
        filePrimaryIndex.seek(0);
        filePrimaryIndex.read(rawPos);
        int from = 0,to=0;
        for (int i = 1; i < numberOfLines; i++) {
            to = Utils.bytesToInt(rawPos, i*Integer.BYTES, bufferForInt);

            rawData.add(Arrays.copyOfRange(raw, from, to));
            from = to;
        }
        to = raw.length;
        rawData.add(Arrays.copyOfRange(raw, from, to));
        closeFile();
        isLoaded = true;
    }

    @Override
    public void unLoad() {
        rawData.clear();
        rawData = null;
        isLoaded = false;
    }

    @Override
    public boolean isFull() {
        try{
            return numberOfLines >= Utils.DEFAULT_CHUNK_SIZE;
        }catch (Exception e){
            return false;
        }
    }

    /**
     * @return returns the position in the raw file of a line from given logical position, and -1 in case we go
     *          beyond the size of the file
     * */
    private long getRawPositionFromLogicalPosition(final int logicalPosition)throws Exception{
        if(logicalPosition >= numberOfLines)return -1;
        byte[] buffer = new byte[Integer.BYTES];
        filePrimaryIndex.seek(Integer.BYTES * logicalPosition);
        filePrimaryIndex.read(buffer, 0, Integer.BYTES);
        return Utils.bytesToInt(buffer, 0, bufferForInt);
    }

    private int getRawSizeFromLogicalPosition(final int logicalPosition)throws Exception{
        long beginningPosition = getRawPositionFromLogicalPosition(logicalPosition);
        if(beginningPosition==-1)return -1;
        long endPosition = getRawPositionFromLogicalPosition(logicalPosition+1);
        if(endPosition == -1){
            return (int)(fileRaw.length() - beginningPosition);
        }else{
            return (int)(endPosition - beginningPosition);
        }
    }




}
