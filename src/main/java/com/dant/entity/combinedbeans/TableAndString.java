package com.dant.entity.combinedbeans;

import com.dant.entity.Table;
import lombok.*;

@AllArgsConstructor
@Data
@EqualsAndHashCode
@ToString
@Builder
public class TableAndString {
    private Table table;
    private String string;
}
