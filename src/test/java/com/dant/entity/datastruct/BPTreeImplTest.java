package com.dant.entity.datastruct;

import com.dant.webservices.FileSystemWS;
import org.junit.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class BPTreeImplTest {

    static final String basePath = FileSystemWS.HOME+
            FileSystemWS.FILE_SEP+
            FileSystemWS.APP_DIR_NAME+
            FileSystemWS.FILE_SEP;


    @Test
    public void testGet(){

    }

    @Test
    public void testGetLeafNodePositionFromKey() throws Exception{

        final String srcFilename  = "tests/test1BPTree";
        BPTreeImpl mock = fromFile(basePath + srcFilename);
        Assert.assertEquals(7, mock.getLeafNodePositionFromKey(13));
        Assert.assertEquals(4, mock.getLeafNodePositionFromKey(4));
        Assert.assertEquals(5, mock.getLeafNodePositionFromKey(6));
        Assert.assertEquals(3, mock.getLeafNodePositionFromKey(0));

    }

    @Test
    public void testGetLeafNodeFromKey() throws Exception{
        final String srcFilename  = "tests/test1BPTree";
        BPTreeImpl mock = fromFile(basePath + srcFilename);
        Assert.assertEquals(mock.getNodeAt(7), mock.getLeafNodeFromKey(13));
        Assert.assertEquals(mock.getNodeAt(4), mock.getLeafNodeFromKey(4));
        Assert.assertEquals(mock.getNodeAt(5), mock.getLeafNodeFromKey(6));
        Assert.assertEquals(mock.getNodeAt(3), mock.getLeafNodeFromKey(0));

    }

    @Test
    public void testInsert()throws Exception{
        final String srcFilename  = "tests/test1BPTree";
        BPTreeImpl mock = fromFile(basePath + srcFilename);
        BPTreeImpl test = new BPTreeImpl();
        test.insert(1,1);
        test.insert(3,1);
        test.insert(5,1 );
        test.insert(7,1 );
        test.insert(9, 1);
        test.insert(2,3);
        test.insert(4, 3);
        test.insert(6, 3);
        test.insert(8, 3);
        test.insert(10, 3);
        Assert.assertEquals(test, mock);
    }

    public BPTreeImpl fromFile(String path)throws Exception{
        BufferedReader br = new BufferedReader(new FileReader(path));
        int i=0;
        String line;
        List<int[]> lines = new ArrayList<>();
        int rootNodeId = Integer.parseInt(br.readLine());
        while((line=br.readLine())!=null) {
            String[] splitted = line.split(" ");
            int[] intSplitted = new int[splitted.length];
            for (int j = 0; j < splitted.length; j++) {
                intSplitted[j] = Integer.parseInt(splitted[j]);
            }
            lines.add(intSplitted);
        }

        BPTreeImpl result = new BPTreeImpl();
        result.rootId = rootNodeId;

        Iterator it = lines.iterator();
        int[] next;
        Node n;
        int z=0;
        while(it.hasNext()){
            next =(int[])it.next();
            n = new Node(z++);

            for (int j = 0; j < next.length; j++) {
                if(next[j]==-1)break;
                n.keys[j] = next[j];
                n.n ++;
            }
            if(it.hasNext()){
                next = (int[]) it.next();
                for (int j = 0; j < next.length; j++) {
                    if(j==0){
                        if(next[0]==-1){
                            if(1 < next.length){
                                n.next = next[1];
                            }
                            for (int k = 2; k < next.length; k++) {
                                n.childs[k-2] = next[k];
                            }
                            break;
                        }else{
                            n.next = -2;
                        }
                    }
                    n.childs[j] = next[j];
                }
            }
            result.saveNode(n);
        }
        return result;
    }

}
