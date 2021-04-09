package com.dant.entity;

import com.dant.webservices.LinesWS;
import com.dant.webservices.TableWS;
import com.dant.webservices.Utils;
import lombok.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

@AllArgsConstructor
@ToString
@EqualsAndHashCode
@Builder
@Data
public class QueryInsert implements Serializable {
    private Table into;
    private Collection<String[]> lines = new HashSet<>();

    public QueryInsert(final String query)throws IllegalArgumentException{
        this.parse(query);
    }

    public void parse(final String query)throws IllegalArgumentException{
        String[] formattedQuery;
        if((formattedQuery=formatAndVerifySyntax(query))==null)throw new IllegalArgumentException("Malformed query insert");
        //Si on est arrivé ici, alors l'expression est bien formée, on peut donc la supposer vérifiée
        try{
            into = TableWS.getInstance().getOneTable(formattedQuery[2]);
        }catch (RuntimeException e){
            throw new IllegalArgumentException("Table "+formattedQuery[2]+" not found, cannot insert on it");
        }
        if(into!=null){
            int i=5,j=-1;
            boolean isKey=true;
            String[] line = new String[into.getColumns().length];
            while(i<formattedQuery.length){
                if(formattedQuery[i].trim().equals("(")){
                    line = new String[into.getColumns().length];
                }

                else if(formattedQuery[i].trim().equals(")")){
                    lines.add(line);
                }

                else if( ! formattedQuery[i].trim().equals(",")){
                    if(isKey){
                        j = into.getColumnPosition(formattedQuery[i]);
                        isKey = false;
                        i+=1;
                    }else{
                        if(j!=-1){
                            line[j] = formattedQuery[i];
                        }
                        isKey = true;
                    }
                }

                i+=1;
            }

        }else{
            throw new IllegalArgumentException("Table "+formattedQuery[2]+" not found, cannot insert on it");
        }

    }

    //TODO Distribuer cette méthode
    public void execute(){
        for(String[] line : lines){
            try{
                into.insert(line);
            }catch (Exception e){
                System.err.println("Error while insert line "+line+" in table  of name : "+
                        ((into==null)?"null":into.toString()));
            }
        }
    }



    private boolean verifySyntaxForInsertWithKeyValue(final String[] splittedQuery) {
        if(splittedQuery == null)return false;

        if(splittedQuery.length<4)return false;

        if(!splittedQuery[0].toUpperCase().equals("INSERT")
                || !splittedQuery[1].toUpperCase().equals("INTO")
                || !splittedQuery[3].toUpperCase().equals("VALUES")
                || !splittedQuery[4].equals("(")){
            return false;
        }

        boolean openB = true;
        boolean isKey = true;
        for(int i=5; i<splittedQuery.length;i++){

            if( splittedQuery[i].trim().equals("(") ){
                if(openB){
                    return false;
                }else{
                    isKey = true;
                    openB = true;
                }
            }

            else if( splittedQuery[i].trim().equals(")")){
                if(openB){
                    openB = false;
                }else{
                    return false;
                }
            }

            else if( splittedQuery[i].trim().equals(",") ){
                if(openB){
                    if(isKey){
                        return false;
                    }else{
                        isKey = true;
                        if(i==splittedQuery.length-1){
                            return false;
                        }else if(splittedQuery[i+1].trim().equals(")")){
                            return false;
                        }
                    }
                }else{
                    if(i==splittedQuery.length-1){
                        return false;
                    } else if (! splittedQuery[i+1].trim().equals("(") ) {
                        return false;
                    }
                }
            }

            else if( splittedQuery[i].trim().equals(":") ){
                if(openB){
                    if(isKey){
                        return false;
                    }else{
                        if(i==splittedQuery.length-1){
                            return false;
                        }else{
                            if(splittedQuery[i+1].trim().equals(",")
                                    || splittedQuery[i+1].trim().equals(")")){
                                return false;
                            }
                        }
                    }
                }else{
                    return false;
                }
            }

            else{
                if(openB){
                    if(isKey){
                        if(i==splittedQuery.length-1){
                            return false;
                        }else{
                            if(! splittedQuery[i+1].trim().equals(":")){
                                return false;
                            }
                        }
                        isKey = false;
                    }else{
                        if(i==splittedQuery.length-1){
                            return false;
                        }else{
                            if(! splittedQuery[i+1].trim().equals(",")
                                    && ! splittedQuery[i+1].trim().equals(")")){
                                return false;
                            }
                        }
                    }
                }else{
                    return false;
                }
            }
        }

        if(openB)return false;
        return true;
    }

    private String[] formatAndVerifySyntax(String query){
        if(query==null)return null;

        String[] splittedQuery = Utils.format(query);

        if(!verifySyntaxForInsertWithKeyValue(splittedQuery))return null;

        return splittedQuery;
    }
}