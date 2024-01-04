package lsm.model;

public class KeyIndex {

    private String key;
    private int offset;

    public KeyIndex(String key, int offset) {
        this.key = key;
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
