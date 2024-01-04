package lsm;

public enum CommandEnum {
    DELETE(-1),
    SET(0);

    private  int flag = -1;


    private CommandEnum(int flag){
        this.flag = flag;
    }
    public  static  CommandEnum valueOf(int flag){
        if(flag== DELETE.flag){
            return DELETE;
        }
        return SET;
    }

    public int getFlag() {
        return this.flag;
    }
}
