package lsm;

import lsm.impl.MemTable;
import lsm.impl.SSTable;
import lsm.model.SetCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestSSTable {


    String fileName = "test.sst";



    @Test
    public  void addData() throws IOException {

        ISSTable ssTable   = new SSTable();
        ssTable.open(fileName);

        IMemTable iMemTable = new MemTable();

        for(int i=1;i<=100;i++){

            SetCommand setCommand = new SetCommand(String.valueOf(i),"hello"+i);
            iMemTable.put(setCommand);


        }

        ssTable.write(iMemTable);


        ssTable.close();
    }


    @Test
    public void query() throws IOException {
        ISSTable ssTable   = new SSTable();
        ssTable.open(fileName);

        ssTable.load();

        ssTable.get("1");
        ssTable.close();
    }


}
