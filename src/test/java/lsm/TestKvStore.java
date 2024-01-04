package lsm;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestKvStore {

    private KvEngine kvEngine = null;

    @Before
    public  void init(){

        try {
            kvEngine = new KvEngine("./",100);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Test
    public void addData(){

        for(int i=1;i<=10000;i++){
            kvEngine.put("key"+i,"value"+i);
        }
    }


    @Test
    public  void query(){
        for(int i=1;i<=10000;i++){
           String data =  kvEngine.get("key"+i);
           System.out.println(data);
        }


    }

    @Test
    public void del(){
        kvEngine.del("key"+100);
        System.out.println( kvEngine.get("key"+100));

        

    }
}
