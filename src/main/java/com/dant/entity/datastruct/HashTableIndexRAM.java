package com.dant.entity.datastruct;

import com.dant.entity.Column;
import com.dant.entity.Where;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashTableIndexRAM implements IndexDataStructure {

    HashMap<Integer, List<Integer>> data;

    public HashTableIndexRAM(){
        data = new HashMap<>();
    }


    @Override
    public void insert(int key, int val) {
        if(data.containsKey(key)){
            data.get(key).add(val);
        }else{
            List<Integer> lst = new ArrayList<>();
            lst.add(val);
            data.put(key, lst);
        }
    }

    @Override
    public List<Integer> get(int key) {
        if(data.containsKey(key)){
            return data.get(key);
        }
        return new ArrayList<>();
    }

    @Override
    public List<Integer> get(int key, String operator) {
        List<Integer> result = new ArrayList<>();
        if(operator.equals("=")){
            return get(key);
        }
        for(Map.Entry<Integer, List<Integer>> entry : data.entrySet()){
            switch(operator){
                case "<":
                    if(entry.getKey()<key){
                        result.addAll(entry.getValue());
                    }
                    break;
                case ">":
                    if(entry.getKey()>key){
                        result.addAll(entry.getValue());
                    }
                    break;
                case "!":
                    if(entry.getKey()!=key){
                        result.addAll(entry.getValue());
                    }
                    break;
            }
        }
        return result;
    }
}
