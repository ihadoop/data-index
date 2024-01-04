package lsm;

import lsm.model.Command;
import lsm.model.DeleteCommand;
import lsm.model.SetCommand;

import java.util.Map;

public interface IMemTable {
    public Map<String, Command> getRawData();

    public void put(SetCommand setCommand);

    public Command get(String key);

    public void del(DeleteCommand deleteCommand);
}
