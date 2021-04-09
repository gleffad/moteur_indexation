package com.dant.entity.combinedbeans;

import lombok.*;

@AllArgsConstructor
@Data
@EqualsAndHashCode
@ToString
@Builder
public class StringAndStringArray {

    String string;
    String[] stringArray;
}
