package com.dant.entity;

import com.dant.webservices.TableWS;
import com.dant.webservices.Utils;
import lombok.*;

//import javax.rmi.CORBA.Util;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

@AllArgsConstructor
@ToString
@EqualsAndHashCode
@Builder
@Data
public class QuerySelect implements Serializable {



    private Table from;
    private String queryStr;
    private List<String> select=new ArrayList<>();
    private List<Where> wheres=new ArrayList<>();
    private List<String> operators = new ArrayList<>();
    private int limit;
    private Result result;

    public QuerySelect(final String query, final int limit)throws IllegalArgumentException{
        //this.parse(query);
        /*System.out.println("TEST-----------------------------------");
        String test = "SELECT * from tableName";
        System.out.println(test+" "+(formatAndVerifySyntax(test)!=null));
        test = "SELECT f1, f2, f3 from tableName";
        System.out.println(test+" "+(formatAndVerifySyntax(test)!=null));
        test = "select f1, f2,f3 from tablename where x=20 and ABS!30";
        System.out.println(test+" "+(formatAndVerifySyntax(test)!=null));
        test =" selectionne f1, f2,f3 from tablename where x=20 and ABS!30";
        System.out.println(test+" "+(formatAndVerifySyntax(test)!=null));
        test =" seleCt f1, f2,f3 from tablename where x=20 and ABS!30";
        System.out.println(test+" "+(formatAndVerifySyntax(test)!=null));
        test =" select *, f2,f3 from tablename where x=20 and ABS!30 or ";
        System.out.println(test+" "+(formatAndVerifySyntax(test)!=null));
        test =" select *, f2,f3 from tablename where x=20 and ABS!30 or xaze=2";
        System.out.println(test+" "+(formatAndVerifySyntax(test)!=null));
        test =" select * from tablename where x=20 and ABS!30 or xaze=2";
        System.out.println(test+" "+(formatAndVerifySyntax(test)!=null));

        System.out.println("TEST FIN -------------------------------------------");*/
        parse(query);
        this.limit = limit;
        this.queryStr = query;
    }

    public void parse(final String query)throws IllegalArgumentException{
        String[] formattedQuery;
        if((formattedQuery=formatAndVerifySyntax(query))==null){
            throw new IllegalArgumentException("Error in syntax of query : "+query);
        }
        else{
            int i=1;
            while(!formattedQuery[i].toUpperCase().equals("FROM")){
                if(!formattedQuery[i].toUpperCase().equals(",")){
                    select.add(formattedQuery[i].toUpperCase());
                }
                i++;
            }
            i++;
            try{
                from = TableWS.getInstance().getOneTable(formattedQuery[i].toUpperCase());
            }catch (RuntimeException e){
                throw new IllegalArgumentException("Table "+formattedQuery[i]+" not found, cannot select elements from it");
            }

            if(!select.get(0).equals("*")){
                String[] selectsArray = new String[select.size()];
                int iSelect=0;
                for(String s:select){
                    selectsArray[iSelect++] = s.toUpperCase();
                }
                byte[] positions = from.getColumnsPositions(selectsArray);
                for (int j = 0; j < positions.length; j++) {
                    if(positions[j]==-1){
                        throw new IllegalArgumentException("Column "+select.get(j)+" not found");
                    }
                }
            }
            if(i==formattedQuery.length-1){
                return;
            }
            else{
                i+=2;
                int colPos;
                while(i<formattedQuery.length){
                    if((colPos=from.getColumnPosition(formattedQuery[i].toUpperCase()))==-1){
                        throw new IllegalArgumentException("Column "+formattedQuery[i]+" not found");
                    }else{
                        wheres.add(Where.builder()
                                .column(formattedQuery[i].toUpperCase())
                                .operator(formattedQuery[i+1].toUpperCase())
                                .value(Column.convertToType(from.getColumns()[colPos].getType(),formattedQuery[i+2]))
                                .build()
                        );
                    }
                    i=i+3;
                    if(i<formattedQuery.length){
                        operators.add(formattedQuery[i].toUpperCase());
                        i++;
                    }
                }
            }
        }
    }
/*
    public void _execute() {
        if (from == null || select == null || select.size() == 0 || from.getFileManager()==null) return;
        try{
            from.getFileManager().openIfIsNot();
        }catch (Exception e){
            System.err.println("Error when opening file manager for table "+from.toString());
        }
        List<Object[]> resultLines = new ArrayList<>();
        List<String> selectedColumnNames = null;

        if (select.get(0).equals("*")) {
            selectedColumnNames = from.getColumnNames();
        } else {
            selectedColumnNames = select;
        }

        if (wheres == null || wheres.size() == 0) {
            //Select all lines, there is no where
            if(select.get(0).equals("*")){
                resultLines = from.getAll();
            }else{
                List<Object[]> allLines = from.getAll();
                for(Object[] line: allLines){
                    resultLines.add(fromLineToLineToAdd(line));
                }
            }
        } else {
            //With wheres
            List<Where> wheresEquals = getWheresEquals();
            byte[] wheresPositions = new byte[wheresEquals.size()];
            int k = 0;
            for (Where w : wheresEquals) {
                wheresPositions[k++] = from.getColumnPosition(w.getColumn());
            }
            List<Index> usefulIndexes = new ArrayList<>();
            byte[] currentIndexColumns;
            boolean all = false;
            List<Index> fromIndexes = from.getIndexes();

            //On cherche les index utiles
            if(fromIndexes!=null){
                for (Index i : fromIndexes) {
                    currentIndexColumns = i.getColumnsPositions();
                    if (!operators.contains("OR")) {
                        if (Utils.equalsArrays(currentIndexColumns, wheresPositions)) {
                            usefulIndexes.clear();
                            usefulIndexes.add(i);
                            //On a trouvé un index qui couvre toutes les colonnes concernées par notre where
                            all = true;
                            break;
                        }
                        if (currentIndexColumns.length == 1) {
                            for (int j = 0; j < wheresPositions.length; j++) {
                                if (wheresPositions[j] == currentIndexColumns[0]) {
                                    usefulIndexes.add(i);
                                }
                            }
                        }
                    }else{
                        //TODO Traiter les OR
                    }
                }
            }
            List<Integer> positionsOfResults = new ArrayList<>();
            Object[] key;
            if (all) {
                //Dans cette situation on est sur qu'il n'y a que des where = et qu'il y a un index indexant toutes
                // les colonnes dans les wheres
                Index i = usefulIndexes.get(0);
                byte[] iColPos = i.getColumnsPositions();
                key = new Object[i.getColumnsPositions().length];
                for (int j = 0; j < iColPos.length; j++) {
                    for (int l = 0; l < wheresPositions.length; l++) {
                        if (wheresPositions[l] == iColPos[j]) {
                            key[j] = wheresEquals.get(l).getValue();
                            break;
                        }
                    }
                }
                positionsOfResults = i.get(key);
            }
            else {
                int z = 0;
                List<Integer> thisIndexResult=new ArrayList<>();
                boolean thereIsIndex;
                positionsOfResults = new ArrayList<>();
                if (wheresEquals.size() == wheres.size()) {
                    int j = 0;
                    //There is no < or > or !
                    for (int i = 0; i < wheresPositions.length; i++) {
                        thereIsIndex = false;
                        for (j = 0; j < usefulIndexes.size(); j++) {
                            //All useful indexes are of one column only in this case
                            if (usefulIndexes.get(j).getColumnsPositions()[0] == wheresPositions[i]) {
                                thereIsIndex = true;
                                break;
                            }
                        }
                        if (thereIsIndex) {
                            thisIndexResult = usefulIndexes.get(j).get(new Object[]{wheresEquals.get(i).getValue()});
                        } else {
                            List<Object[]> lines = from.getAll();
                            int pos = 0, size = lines.size();
                            for (Object[] line : lines) {
                                if(line[wheresPositions[i]].equals(wheresEquals.get(i).getValue())){
                                    thisIndexResult.add(pos);
                                }
                                pos++;
                            }

                        }
                        if (i == 0) {
                            positionsOfResults.addAll(thisIndexResult);
                        } else {
                            if (operators.get(i - 1).equals("AND")) {
                                positionsOfResults = Utils.intersection(positionsOfResults, thisIndexResult);
                            } else {
                                //OR
                                positionsOfResults.addAll(thisIndexResult);
                                positionsOfResults = positionsOfResults.stream().distinct().collect(Collectors.toList());
                            }
                        }
                    }
                } else {
                    //There is also < and > and !                 }
                }
            }
            if(positionsOfResults!=null){
                for(int i:positionsOfResults){
                    if(select.get(0).equals("*")){
                        resultLines.add(from.getAtPosition(i));
                    }else{
                        resultLines.add(fromLineToLineToAdd(from.getAtPosition(i)));
                    }
                }
            }
        }
        result = Result.builder()
                .tableName(from!=null?from.getName():"")
                .rawQuery(queryStr)
                .lines(resultLines)
                .count(resultLines!=null?resultLines.size():0)
                .select(selectedColumnNames)
                .build();
        try{
            from.getFileManager().closeFile();
        }catch(Exception e){
            System.err.println("Error while trying to close file manager of table "+from);
        }
    }*/

    public void execute() {
        if (from == null || select == null || select.size() == 0 ) return;
        List<Object[]> resultLines = new ArrayList<>();
        List<String> selectedColumnNames = null;

        if (select.get(0).equals("*")) {
            selectedColumnNames = from.getColumnNames();
        } else {
            selectedColumnNames = select;
        }

        if (wheres == null || wheres.size() == 0) {
            //Select all lines, there is no where
            if(select.get(0).equals("*")){
                resultLines = from.getAll();
                if(resultLines.size() >= limit){
                    resultLines = resultLines.subList(0, limit);
                }
            }else{
                List<Object[]> allLines = from.getAll();
                if(allLines.size() >= limit){
                    allLines = allLines.subList(0, limit);
                }
                for(Object[] line: allLines){
                    resultLines.add(fromLineToLineToAdd(line));
                }
            }
        } else {
            //With wheres
            byte[] wheresPositions = new byte[wheres.size()];
            int k = 0;
            for (Where w : wheres) {
                wheresPositions[k++] = from.getColumnPosition(w.getColumn());
            }
            List<Index> usefulIndexes = new ArrayList<>();
            byte[] currentIndexColumns;
            boolean all = false;
            List<Index> fromIndexes = from.getIndexes();

            //On vérifie s'il n'y a que des equals
            boolean onlyEquals = true;
            for(Where w:wheres){
                if(w.getOperator()!=null && !w.getOperator().equals("=")){
                    onlyEquals = false;
                    break;
                }
            }
            //On vérifie s'il n'y a que des AND
            boolean onlyAnd = !operators.contains("OR");

            //On cherche les index utiles
            if(fromIndexes!=null){
                for (Index i : fromIndexes) {
                    currentIndexColumns = i.getColumnsPositions();
                    if (onlyAnd && onlyEquals && Utils.equalsArrays(currentIndexColumns, wheresPositions)) {
                        usefulIndexes.clear();
                        usefulIndexes.add(i);
                        //On a trouvé un index qui couvre toutes les colonnes concernées par notre where,
                        // de plus il n'y a que des = et que des and
                        all = true;
                        break;
                    }
                    if (currentIndexColumns.length == 1) {
                        for (int j = 0; j < wheresPositions.length; j++) {
                            if (wheresPositions[j] == currentIndexColumns[0]) {
                                usefulIndexes.add(i);
                            }
                        }
                    }
                }
            }
            List<Integer> positionsOfResults;
            Object[] key;
            if (all) {
                //Dans cette situation on est sur qu'il n'y a que des where = reliés par des and et qu'il y a un index
                // indexant toutes les colonnes dans les wheres
                Index i = usefulIndexes.get(0);
                byte[] iColPos = i.getColumnsPositions();
                key = new Object[i.getColumnsPositions().length];
                for (int j = 0; j < iColPos.length; j++) {
                    for (int l = 0; l < wheresPositions.length; l++) {
                        if (wheresPositions[l] == iColPos[j]) {
                            key[j] = wheres.get(l).getValue();
                            break;
                        }
                    }
                }
                positionsOfResults = i.get(key);
                if(positionsOfResults.size() >= limit){
                    positionsOfResults = positionsOfResults.subList(0, limit);
                }
            }
            else {
                int z = 0;
                List<Integer> thisIndexResult=new ArrayList<>();
                boolean thereIsIndex;
                positionsOfResults = new LinkedList<>();
                int j = 0;
                for (int i = 0; i < wheresPositions.length; i++) {
                    thereIsIndex = false;
                    for (j = 0; j < usefulIndexes.size(); j++) {
                        //All useful indexes are of one column only in this case
                        if (usefulIndexes.get(j).getColumnsPositions()[0] == wheresPositions[i]) {
                            thereIsIndex = true;
                            break;
                        }
                    }
                    if (thereIsIndex) {
                        if(wheres.get(i).getOperator().equals("=")){
                            thisIndexResult = usefulIndexes.get(j).get(new Object[]{wheres.get(i).getValue()});
                        }else{
                            thisIndexResult = usefulIndexes.get(j).get(wheres.get(i));
                        }
                        if(thisIndexResult.size() >= limit){
                            thisIndexResult = thisIndexResult.subList(0, limit);
                        }
                    } else {
                        from.startIterator();
                        Iterator<Object[]> iterator = from.iterator();
                        Object[] line;
                        int pos = 0;
                        while(iterator.hasNext() && thisIndexResult.size() <= limit){
                            line = iterator.next();
                            if(wheres.get(i).getOperator().equals("=")){
                                if(line[wheresPositions[i]].equals(wheres.get(i).getValue())){
                                    thisIndexResult.add(pos);
                                }
                            }else{
                                if(wheres.get(i).verify(line[wheresPositions[i]])){
                                    thisIndexResult.add(pos);
                                }
                            }
                            pos++;
                        }

                    }
                    if (i == 0) {
                        positionsOfResults.addAll(thisIndexResult);
                    } else {
                        if (operators.get(i - 1).equals("AND")) {
                            positionsOfResults = Utils.intersection(positionsOfResults, thisIndexResult);
                        } else {
                            //OR
                            positionsOfResults.addAll(thisIndexResult);
                            positionsOfResults = positionsOfResults.stream().distinct().collect(Collectors.toList());
                        }
                    }
                    thisIndexResult.clear();
                }
            }
            if(positionsOfResults!=null){
                if(positionsOfResults.size()>=limit){
                    positionsOfResults = positionsOfResults.subList(0, limit);
                }
                for(int i:positionsOfResults){
                    if(select.get(0).equals("*")){
                        resultLines.add(from.getAtPosition(i));
                    }else{
                        resultLines.add(fromLineToLineToAdd(from.getAtPosition(i)));
                    }
                }
            }
        }
        result = Result.builder()
                .tableName(from!=null?from.getName():"")
                .rawQuery(queryStr)
                .lines(resultLines)
                .count(resultLines!=null?resultLines.size():0)
                .select(selectedColumnNames)
                .build();
    }

    /**
     *@return the list of where clauses that the operator is equals, in their order in the wheres list
     * */
    public List<Where> getWheresEquals(){
        List<Where> result = new ArrayList<>();
        for(Where w:wheres){
            if(w.getOperator().equals("=")){
                result.add(w);
            }
        }
        return result;
    }

    public Object[] fromLineToLineToAdd(final Object[] line){
        String[] selectsArray = new String[select.size()];
        int iSelect=0;
        for(String s:select){
            selectsArray[iSelect++] = s;
        }
        byte[] positions = from.getColumnsPositions(selectsArray);
        Object[] lineToAdd = new Object[positions.length];
        int i=0;
        for(byte p:positions){
            lineToAdd[i++]=line[p];
        }
        return lineToAdd;
    }

    public boolean verifySyntax(final String[] splittedQuery){
        if(splittedQuery==null)return false;
        if(splittedQuery.length<4)return false;
        if(! splittedQuery[0].toUpperCase().equals("SELECT")){
            return false;
        }
        boolean containsFrom=false;
        for(String s:splittedQuery){
            if(s.toUpperCase().equals("FROM")){
                containsFrom = true;
            }
        }
        if(!containsFrom){
            return false;
        }

        //Cas ou on veut récupérer toutes les colonnes, avec *
        int i=1;
        if(splittedQuery[1].equals("*")){
            if(!splittedQuery[2].toUpperCase().equals("FROM"))return false;
            i = 3;
        }else{
            if(splittedQuery[i].toUpperCase().equals("FROM"))return false;
            for(;i<splittedQuery.length;i++){
                //On assure qu'il y a COLONNE , COLONNE , COLONNE ... (nom de colonnes séparés par des virgules)
                if(splittedQuery[i].toUpperCase().equals("FROM"))break;
                if(splittedQuery[i].equals(","))return false;
                if(Utils.isKeyword(splittedQuery[i]))return false;
                if(i<splittedQuery.length-1 && !splittedQuery[i+1].toUpperCase().equals("FROM")){
                    i++;
                }
            }
            if(i==splittedQuery.length){
                return false;
            }else{
                i = i+1;
            }
        }
        if(i==splittedQuery.length-1 ){
            return !Utils.isKeyword(splittedQuery[i].toUpperCase());
        }else{
            if(!Utils.isKeyword(splittedQuery[i].toUpperCase())){
                i++;
            }else{
                return false;
            }
            if(! splittedQuery[i].toUpperCase().equals("WHERE")){
                return false;
            }
            i++;
            for(;i<splittedQuery.length;i++){
                if(Utils.isKeyword(splittedQuery[i].toUpperCase())){
                    return false;
                }
                i++;
                if(i==splittedQuery.length || !Utils.isWhereOperator(splittedQuery[i].toUpperCase())){
                    return false;
                }
                i++;
                if(i==splittedQuery.length || Utils.isKeyword(splittedQuery[i].toUpperCase())){
                    return false;
                }
                i++;
                if(i==splittedQuery.length){
                    return true;
                }else{
                    if(!Utils.isBooleanOperator(splittedQuery[i].toUpperCase())){
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public String[] formatAndVerifySyntax(final String query){
        if(query==null){
            return null;
        }
        String[] splittedQuery = Utils.format(query);
        if(!verifySyntax(splittedQuery)){
            return null;
        }
        return splittedQuery;
    }

}



/**
 *
 TODO SELECT SUR DES OU mélangés avec des ET
 TODO Add number of results and time of computing.... in the result object
 TODO Add LIMIT, ORDER BY and DISTINCT
 * */
