package com.dant.entity;


import lombok.*;

import java.io.Serializable;
import java.util.Objects;

/**
 * WHERE column = value
 *
 */
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@Builder
@Data
public class Where implements Serializable {

    private String column;
    private String operator;
    private Object value;

    public boolean verify(Comparable value){
        if(value==null)return value==this.value;
        switch (operator){
            case "=":
                return (this.value == value) || (this.value != null && this.value.equals(value));
            case "<":
                return  (this.value != null &&  value.compareTo(this.value) < 0);
            case ">":
                return  (this.value != null && value.compareTo(this.value) > 0);
            case "!":
                return  (this.value != null && !this.value.equals(value));
            default:
                return  (this.value != null && this.value.equals(value));
        }
    }
    public boolean verify(Object value){
        if(value==null)return value==this.value;
        if(value instanceof Comparable){
            return verify((Comparable)value);
        }else{
            switch (operator){
                case "=":
                    return (this.value == value) || (this.value != null && this.value.equals(value));
                case "!":
                    return  (this.value != null && !this.value.equals(value));
                default:
                    return (this.value != null && this.value.equals(value));
            }
        }
    }
}

