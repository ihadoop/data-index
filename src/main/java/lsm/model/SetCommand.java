package lsm.model;

import lsm.CommandEnum;

public class SetCommand extends Command{

    public SetCommand(String key,String value){
        this.key = key;
        this.value = value;
    }
    @Override
    public CommandEnum getCommand() {
            return CommandEnum.SET;
    }
}
