package lsm;

import lsm.model.Command;

import java.io.IOException;
import java.util.List;

public interface ISSTable {


    void write(IMemTable memTable) throws IOException;

    Command get(String key) throws IOException;

    List<Command> scan(String startKey, String endKey);

    void open(String fileName) throws IOException;

    void close();
    void load() throws IOException;
    void destory();
}
