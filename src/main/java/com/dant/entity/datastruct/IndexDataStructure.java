package com.dant.entity.datastruct;

import java.util.List;

public interface IndexDataStructure {
    void insert(final int key, final int val);

    List<Integer> get(final int key);

    List<Integer> get(final int key, final String operator );

}
