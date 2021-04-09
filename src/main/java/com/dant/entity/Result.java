package com.dant.entity;

import lombok.*;

import java.util.List;

@AllArgsConstructor
@Data
@EqualsAndHashCode
@ToString
@Builder
public class Result {
    String tableName;
    String rawQuery;
    List<String> select;
    private long count;
    List<Object[]> lines;
}
