package lsm.impl;

import lsm.IMemTable;
import lsm.model.Command;
import lsm.model.DeleteCommand;
import lsm.model.SetCommand;

import java.util.Map;
import java.util.TreeMap;

public class MemTable implements IMemTable {

    private TreeMap<String, Command> stringCommandTreeMap = new TreeMap<>();


    @Override
    public Map<String, Command> getRawData() {
        return stringCommandTreeMap;
    }


    public void put(SetCommand setCommand){
        stringCommandTreeMap.put(setCommand.getKey(),setCommand);
    }


    public void del(DeleteCommand deleteCommand){
        stringCommandTreeMap.put(deleteCommand.getKey(), deleteCommand);

    }

    public  Command get(String key){
        return  stringCommandTreeMap.get(key);
    }
}
