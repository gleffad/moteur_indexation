package com.dant.entity.datastruct;

import com.dant.entity.Index;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@EqualsAndHashCode
public class BPTreeImpl extends BPTree{
    @Getter
    private List<Node> nodes;

    public BPTreeImpl(){
        super();
        this.nodes = new ArrayList<>();
    }

    public  Node getNodeAt(int position){
        if(nbNodes<=position){
            return null;
        }
        return nodes.get(position);
    }

    public  int appendNode(Node node){
        int res = nbNodes;
        nodes.add(node);
        nbNodes++;
        return res;
    }

    public  void saveNode(Node node, int position){
        if(position >= nbNodes){
            nodes.add(node);
            nbNodes++;
        }else{
            nodes.set(position, node);
        }
    }

    public List<Integer> getAllValuesInPos(final Node leaf, final int pos){
        List<Integer> result = new ArrayList<>();
        result.add(leaf.childs[pos]);
        return result;
    }

    public void treatDuplicateKey(final int position, final int value, final Node leaf){
        leaf.childs[position] = value;
        saveNode(leaf);
    }

    public  int saveValue(int value){
        return value;
    }

}