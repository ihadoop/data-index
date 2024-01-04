package lsm.impl;

import lsm.CommandEnum;
import lsm.IMemTable;
import lsm.ISSTable;
import lsm.model.Command;
import lsm.model.DeleteCommand;
import lsm.model.KeyIndex;
import lsm.model.SetCommand;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * file format
 * <p>
 * <p>
 * raw-data：+(key-len(int) + key-value +cmd(int)+ value-len(int) + value+))+
 * <p>
 * key-index:(key-len(int) + key-value+offset(int))+
 * <p>
 * summary:   file_version(int)+key_index_size(个数int)++key(offset);
 */
public class SSTable implements ISSTable {

    private static final int version = 1;
    private String fileName;
    private File file = null;
    private RandomAccessFile randomAccessFile = null;
    private int index;
    private LinkedList<KeyIndex> keyIndexs = null;

    @Override
    public void write(IMemTable memTable) throws IOException {
        Map<String, Command> map = memTable.getRawData();
        keyIndexs  = new LinkedList<>();

        //write raw-data
        LinkedList<KeyIndex> tempIndexes = new LinkedList<KeyIndex>();
        int current = 0;
        for (Map.Entry<String, Command> stringCommandEntry : map.entrySet()) {
            byte[] values = stringCommandEntry.getValue().toBytes();
            randomAccessFile.write(values);
            tempIndexes.add(new KeyIndex(stringCommandEntry.getKey(), current));
            current += values.length;
        }
        keyIndexs  = new LinkedList<>(tempIndexes);
        //write key-index
        for (KeyIndex tempIndex : tempIndexes) {
            randomAccessFile.writeInt(tempIndex.getKey().getBytes().length);
            randomAccessFile.write(tempIndex.getKey().getBytes());
            randomAccessFile.writeInt(tempIndex.getOffset());
        }

        randomAccessFile.writeInt(version);
        randomAccessFile.writeInt(map.size());
        randomAccessFile.writeInt(current);

    }

    @Override
    public Command get(String key) throws IOException {
        for (KeyIndex keyIndex : keyIndexs) {
            if (keyIndex.getKey().equals(key)) {
                int offset = keyIndex.getOffset();
                randomAccessFile.seek(offset);
                int keyLength = randomAccessFile.readInt();
                randomAccessFile.skipBytes(keyLength);
                int flag = randomAccessFile.readInt();
                int valueLen  =randomAccessFile.readInt();
                byte[] bytes = new byte[valueLen];
                randomAccessFile.read(bytes);
               if(flag== CommandEnum.SET.getFlag()){
                    return new SetCommand(key,new String(bytes));
               }else{
                   return new DeleteCommand(key);
               }
            }
        }

        return null;
    }

    @Override
    public List<Command> scan(String startKey, String endKey) {
        return null;
    }

    @Override
    public void open(String fileName) throws IOException {

        file = new File(fileName);
        if (!file.exists()) {
            file.createNewFile();
        }
        randomAccessFile = new RandomAccessFile(fileName, "rw");
    }

    public void load() throws IOException {
        keyIndexs = new LinkedList<>();
        int metaOffset = (int) (file.length() - 4 * 3);
        randomAccessFile.seek(metaOffset);
        int version = randomAccessFile.readInt();
        int size = randomAccessFile.readInt();
        int keyOffset = randomAccessFile.readInt();


        keyIndexs = new LinkedList<>();

        randomAccessFile.seek(keyOffset);

        for (int i = 1; i <= size; i++) {
            int keyLen = randomAccessFile.readInt();
            byte[] bytes = new byte[keyLen];
            randomAccessFile.read(bytes);
            String key = new String(bytes);
            int offset = randomAccessFile.readInt();
            keyIndexs.add(new KeyIndex(key, offset));
        }


    }

    @Override
    public void close() {
        if (randomAccessFile != null) {
            try {
                randomAccessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void destory() {
        this.close();
        if (file != null) {
            file.delete();
        }
    }
}
