package lsm;


import lsm.impl.MemTable;
import lsm.impl.SSTable;
import lsm.model.Command;
import lsm.model.DeleteCommand;
import lsm.model.SetCommand;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KvEngine {

    private ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();

    private IMemTable writable;

    private IMemTable readOnly;


    private List<ISSTable> ssTableList = new ArrayList<>();

    private File walFile = null;
    private RandomAccessFile wal = null;
    private int storeThreshold;
    private String dataDir;

    public KvEngine(String dataDir, int storeThreshold) throws IOException {

        this.dataDir = dataDir;
        this.storeThreshold = storeThreshold;
        writable = new MemTable();
        initWal();
        recoverFromLog();
        loadSSTable();

    }

    private void loadSSTable() {
        File [] files = new File(dataDir).listFiles((p->p.getName().startsWith("sstable")));
        for (File file : files) {

            try {
                ISSTable issTable = new SSTable();
                issTable.open(file.getAbsolutePath());
                issTable.load();
                ssTableList.add(0,issTable);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        

    }

    public void del(String key){

        DeleteCommand command = new DeleteCommand(key);
        ReentrantReadWriteLock.WriteLock writeLock = indexLock.writeLock();

        try {
            writeLock.lock();
            if (writable.getRawData().size() >= storeThreshold) {
                switchIndex();
            }
            writeWal(command);
            writable.del(command);

        } catch (Exception exception) {
            exception.printStackTrace();

        } finally {
            writeLock.unlock();
        }

    }

    private void initWal() throws IOException {

        walFile = new File(dataDir + "/wal.log");
        if (!walFile.exists()) {
            walFile.createNewFile();
        }
        wal = new RandomAccessFile(walFile, "rw");

    }

    private void recoverFromLog() throws IOException {

        int lenght = (int) wal.length();
        if (lenght > 0) {
            int total = 0;
            while (lenght > total) {

                int keyLength = wal.readInt();
                total += 4;
                byte[] keyBytes = new byte[keyLength];
                wal.read(keyBytes);
                total += keyLength;
                String key = new String(keyBytes);

                int flag = wal.readInt();
                total += 4;
                int valueLen = wal.readInt();
                total += 4;
                byte[] bytes = new byte[valueLen];
                total += valueLen;
                wal.read(bytes);
                if (flag == CommandEnum.SET.getFlag()) {
                    writable.put(new SetCommand(key, new String(bytes)));
                } else {
                    writable.del(new DeleteCommand(key));
                }


            }
        }

    }


    public boolean put(String key, String value) {

        ReentrantReadWriteLock.WriteLock writeLock = indexLock.writeLock();

        try {
            writeLock.lock();
            if (writable.getRawData().size() >= storeThreshold) {
                switchIndex();
            }
            SetCommand setCommand = new SetCommand(key, value);
            writeWal(setCommand);
            writable.put(setCommand);

        } catch (Exception exception) {
        exception.printStackTrace();

        } finally {
            writeLock.unlock();
        }


        return true;
    }

    private void writeWal(Command command) {
        try {
            wal.write(command.toBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public String get(String key) {

        ReentrantReadWriteLock.ReadLock readLock = indexLock.readLock();

        try {
            readLock.lock();
            Command command = writable.get(key);
            if (command != null) {
                return command.getValue();
            }
            if(readOnly!=null){
            command = readOnly.get(key);
            if (command != null) {
                return command.getValue();
            }
            }

            for (ISSTable issTable : ssTableList) {
                command = issTable.get(key);
                if (command != null) {
                    return command.getValue();
                }
            }


        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            readLock.unlock();
        }


        return null;
    }


    public void close() {

        try {
            wal.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private void switchIndex() {

        try {
            readOnly = writable;
            writable = new MemTable();
            ISSTable issTable = new SSTable();
            //memory to
            issTable.open(dataDir + "/sstable." + System.currentTimeMillis());
            issTable.write(readOnly);
            //删除old log
            wal.close();
            walFile.delete();
            walFile = new File(dataDir + "/wal.log");

            wal = new RandomAccessFile(walFile, "rw");

            ssTableList.add(0, issTable);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
