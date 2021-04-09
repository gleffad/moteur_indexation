package com.dant.webservices;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Utils {

    public static final int NB_THREADS = 3000;
    public static final long DEFAULT_LIMIT_RESULTS = 10000;
    public static final String DEFAULT_LIMIT_RESULTS_STR = "10000";
    public static final long DEFAULT_CHUNK_SIZE = 10000;
    public static final long DEFAULT_LINE_SIZE = 200;
    public static final int DEFAULT_BUF_FILE_MANAGER_SIZE = (int)(Utils.DEFAULT_CHUNK_SIZE * Utils.DEFAULT_LINE_SIZE);


    public final static String RAW_DATA_FILENAME_SUFFIX = "data.bin";
    public final static String PRIMARY_INDEX_FILENAME_SUFFIX = "prim_index.bin";


    public static String[] format(String query) {
        if (query == null) return null;
        query = query.replaceAll("\\(", " ( ")
                .replaceAll("\\)", " ) ")
                .replaceAll(",", " , ")
                .replaceAll(":", " : ")
                .replaceAll("!", " ! ")
                .replaceAll("="," = ")
                .replaceAll("<", " < ")
                .replaceAll(">", " > ");

        String[] result = query.trim().split("\\s+");
        //for(int i=0;i<result.length;i++)result[i] = result[i].toUpperCase();
        return result;
    }

    public static String[] keywords = new String[]{
            "SELECT",
            "INSERT",
            "AND",
            "OR",
            "(",
            ":",
            ",",
            ")",
            "=",
            "<",
            ">",
            "!",
            "INTO",
            "VALUES",
            "WHERE",
            "FROM"
    };

    public static String[] whereOperators = new String[]{
            "=",
            "<",
            ">",
            "!"
    };

    public static String[] booleanOperators = new String[]{
            "AND",
            "OR"
    };

    public static boolean isKeyword(final String string) {
        if (string == null) return false;
        return Arrays.stream(keywords).anyMatch(string::equals);
    }

    public static boolean isWhereOperator(final String string) {
        if (string == null)return false;
        return Arrays.stream(whereOperators).anyMatch(string::equals);
    }

    public static boolean isBooleanOperator(final String string){
        if(string == null )return false;
        return Arrays.stream(booleanOperators).anyMatch(string::equals);
    }

    @Deprecated
    public static boolean matchArrays(final byte[] wheresEqualsPos, final  byte[] indexPos){
        if(indexPos.length>wheresEqualsPos.length){
            return false;
        }else{
            int i,j;
            for (i = 0; i < indexPos.length; i++) {
                for (j = 0; j < wheresEqualsPos.length; j++) {
                    if(indexPos[i]==wheresEqualsPos[j])break;
                }
                if(j==wheresEqualsPos.length)return false;
            }
            return true;
        }
    }

    public static boolean equalsArrays(final byte[] wheresEqualsPos, final byte[] indexPos){
        if(wheresEqualsPos==indexPos)return true;
        if(wheresEqualsPos==null || indexPos==null)return false;
        byte[] wheresEqP = Arrays.copyOf(wheresEqualsPos, wheresEqualsPos.length);
        byte[] indexP = Arrays.copyOf(indexPos, indexPos.length);
        Arrays.sort(wheresEqP);
        Arrays.sort(indexP);
        return Arrays.equals(wheresEqP,indexP);
    }

    public static <T> List<T> intersection(List<T> list1, List<T> list2) {
        List<T> list = new ArrayList<>();

        for (T t : list1) {
            if (list2.contains(t)) {
                list.add(t);
            }
        }

        return list;
    }

    private static ByteBuffer bufferForInt = ByteBuffer.allocate(Integer.BYTES);
    synchronized public static byte[] intToBytes(int x){
        return intToBytes(x, bufferForInt);
    }
    public static byte[] intToBytes(int x, ByteBuffer bufferForInt){
        bufferForInt.clear();
        bufferForInt.putInt(0, x);
        return bufferForInt.array().clone();
    }
    synchronized public static int bytesToInt(byte[] bytes){
        return bytesToInt(bytes, 0);
    }
    synchronized public static int bytesToInt(byte[] bytes, int offset){
        return bytesToInt(bytes, offset, bufferForInt);
    }
    public static int bytesToInt(byte[] bytes, int offset, ByteBuffer bufferForInt){
        bufferForInt.clear();
        bufferForInt.put(bytes, offset, Integer.BYTES);
        bufferForInt.clear();
        return bufferForInt.getInt();
    }


    private static ByteBuffer bufferForLong = ByteBuffer.allocate(Long.BYTES);
    synchronized public static byte[] longToBytes(long x){
        return longToBytes(x, bufferForLong);
    }
    public static byte[] longToBytes(long x, ByteBuffer bufferForLong){
        bufferForLong.clear();
        bufferForLong.putLong(0, x);
        return bufferForLong.array().clone();
    }
    synchronized public static long bytesToLong(byte[] bytes){
        return bytesToLong(bytes, 0);
    }
    synchronized public static long bytesToLong(byte[] bytes, int offset){
        return bytesToLong(bytes, offset, bufferForLong);
    }
    public static long bytesToLong(byte[] bytes, int offset, ByteBuffer bufferForLong){
        bufferForLong.clear();
        bufferForLong.put(bytes, offset, Long.BYTES);
        bufferForLong.clear();
        return bufferForLong.getLong();
    }


    private static ByteBuffer bufferForFloat = ByteBuffer.allocate(Float.BYTES);
    synchronized public static byte[] floatToBytes(float x){
        return floatToBytes(x, bufferForFloat);
    }
    public static byte[] floatToBytes(float x, ByteBuffer bufferForFloat){
        bufferForFloat.clear();
        bufferForFloat.putFloat(0, x);
        return bufferForFloat.array().clone();
    }
    synchronized public static float bytesToFloat(byte[] bytes){
        return bytesToFloat(bytes, 0);
    }
    synchronized public static float bytesToFloat(byte[] bytes, int offset){
        return bytesToFloat(bytes, offset, bufferForFloat);
    }
    public static float bytesToFloat(byte[] bytes, int offset, ByteBuffer bufferForFloat){
        bufferForFloat.clear();
        bufferForFloat.put(bytes, offset, Float.BYTES);
        bufferForFloat.clear();
        return bufferForFloat.getFloat();
    }


    public static byte[] getBytesFast(String str) {
        final char buffer[] = new char[str.length()];
        final int length = str.length();
        str.getChars(0, length, buffer, 0);
        final byte b[] = new byte[length];
        for (int j = 0; j < length; j++)
            b[j] = (byte) buffer[j];
        return b;
    }

}