package com.dant.entity;

import lombok.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

@AllArgsConstructor
@Data
@EqualsAndHashCode
@ToString
@Builder
public class Column implements Serializable {

    private String name;
    private String type;

    public String getName(){
        if(name==null)return null;
        return name.toUpperCase();
    }

    public String getType(){
        if(type==null)return null;
        return type.toUpperCase();
    }

    public final static String[] types = new String[]{
            "INT",
            "STRING",
            "LONG",
            "FLOAT",
    };

    public static Object convertToType(String type, final String value){
        if(type==null || value == null) return null;
        try {
            switch (type) {
                case "INT":
                    return Integer.parseInt(value);
                case "STRING":
                    return value;
                case "LONG":
                    return Long.parseLong(value);
                case "FLOAT":
                    return Float.parseFloat(value);
            }
        }catch (Exception e){
            return null;
        }
        return null;
    }
    public void detectType(final String value){
        try{
            Integer.parseInt(value);
            type = "INT";
        }catch (Exception e){
            try{
                Long.parseLong(value);
                type = "LONG";
            }catch (Exception f){
                try{
                    Float.parseFloat(value);
                    type = "FLOAT";
                }catch(Exception g){
                    type = "STRING";
                }
            }
        }

    }
}
