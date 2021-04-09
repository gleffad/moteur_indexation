package com.dant.entity.combinedbeans;

import lombok.*;

@AllArgsConstructor
@Data
@EqualsAndHashCode
@ToString
@Builder
public class StringAndArrayOfStringArray {
    String string;
    String[][] arrayOfStringArray;
}
