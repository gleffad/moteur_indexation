package com.dant.entity.datastruct;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Arrays;
import java.util.Objects;

@Data
@ToString
@EqualsAndHashCode
class Node{
    public final static int BYTES_SIZE = (BPTreeImpl.M + BPTreeImpl.M+1 + 3) * Integer.BYTES;
    /**
     * id for this node (its adress)
     * */
    int id;

    /**
     * Number of keys in this node
     * */
    int n;
    int[] keys;

    /**
     * internal node : contains positions of child nodes
     * leaf node : contains position of first element of corresponding list in raw file of index
     * */
    int[] childs;

    /**
     * internal node: equals to -2
     * leaf node : equals to next leaf node position, -1 if end of list
     * */
    int next;

    public Node(int id){
        this.id = id;
        this.n = 0;

        //By Default, we are a leaf node without next leaf existing
        this.next = -1;

        this.keys = new int[BPTreeImpl.M];
        this.childs = new int[BPTreeImpl.M+1];

        for (int i = 0; i < BPTreeImpl.M; i++) {
            this.keys[i] = -1;
            this.childs[i] = -1;
        }
        this.childs[BPTreeImpl.M] = -1;

    }

    public boolean isFull(){
        return n >= keys.length;
    }

    int insertKey(int key){
        if(n<keys.length){
            int i;
            for (i = 0; i < n; i++) {
                if(keys[i]>key){
                    for (int j = n; j > i; j--) {
                        keys[j] = keys[j-1];
                    }
                    break;
                }
            }
            keys[i] = key;
            n++;
            return i;
        }
        return -1;
    }



    /**
     * Returns the position where we should put a key. Sequencial search is used bcs array is small
     * */
    public int getPositionWhereShouldPutKey(int key){
        int i;
        for (i = 0; i < n; i++) {
            if(keys[i]>key)return i;
        }
        return i;
    }

    /**
     * Returns the position of the key, -1 if not found. Sequencial search is used bcs array is small
     * */
    public int getFirstPositionOfKey(int key){
        for (int i = 0; i < n; i++) {
            if (keys[i]==key) return i;
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return n == node.n &&
                next == node.next &&
                Arrays.equals(keys, node.keys) &&
                Arrays.equals(childs, node.childs);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(n, next);
        result = 31 * result + Arrays.hashCode(keys);
        result = 31 * result + Arrays.hashCode(childs);
        return result;
    }

}

