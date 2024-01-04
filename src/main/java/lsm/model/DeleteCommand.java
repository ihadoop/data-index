package lsm.model;

import lsm.CommandEnum;

public class DeleteCommand extends Command {

    public DeleteCommand(String key){
        this.key  = key;

    }
    @Override
    public CommandEnum getCommand() {
        return CommandEnum.DELETE;
    }
}
