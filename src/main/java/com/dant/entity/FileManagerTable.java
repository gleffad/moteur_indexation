package com.dant.entity;

import java.io.IOException;
import java.util.List;

public interface FileManagerTable {
    boolean isOpen();

    void openIfIsNot()throws Exception;

    void openFile()throws Exception;

    void closeFile()throws Exception;

    void resetPos()throws Exception;

    boolean isEndOfFile();

    int writeLine(byte[] line);

    int writeLine(byte[][] line);

    byte[] readLineAtPosition(int logicalPosition);

    byte[] readNextLine();

    void save()throws Exception;
    void loadAll()throws Exception;
    boolean isLoaded();
    void unLoad();
    boolean isFull();

}
