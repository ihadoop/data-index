package lsm.model;

import lsm.CommandEnum;

import java.nio.ByteBuffer;

public abstract class Command {

    protected String key;
    protected String value;



    public abstract CommandEnum getCommand();

    public byte[] toBytes(){
        //(key-len(int) + key-value +cmd(int)+ value-len(int) + value+))+
        byte[] keyBytes = key.getBytes();
        byte[] valueBytes  = new byte[0];
        ByteBuffer byteBuffer  =null;

        if(getCommand().equals(CommandEnum.DELETE)){
            byteBuffer = ByteBuffer.allocate(4+keyBytes.length+4+4+0);
        }else{
            valueBytes = value.getBytes();
            byteBuffer = ByteBuffer.allocate(4+keyBytes.length+4+4+valueBytes.length);
        }

        byteBuffer.putInt(keyBytes.length);
        byteBuffer.put(keyBytes);
        byteBuffer.putInt(getCommand().getFlag());

        if(getCommand().equals(CommandEnum.DELETE)){
            byteBuffer.putInt(0);
        }else{
            byteBuffer.putInt(valueBytes.length);
            byteBuffer.put(valueBytes);
        }

        return byteBuffer.array();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }




}
