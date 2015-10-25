import java.io.*;
import java.util.*;

public class DataBase {
    private Map<String, ArrayList<String>> tables = new HashMap<>();
    private String[] metadata;

    public void addAttribute(String table, String...attributes) {
        for (String atr : attributes) tables.get(table).add(atr);
    }
    public void addTable(String table, String...attributes) {
        tables.put(table, new ArrayList<>());
        for (String atr : attributes) tables.get(table).add(atr);
    }
    public void addTables(Map<String, ArrayList<String>> tables) {
        this.tables.putAll(tables);
    }

    /**
     * Reads metadata from given @param file in active memory
     * @param file
     * @throws IOException
     */
    private void readMetadata(String file) throws IOException {
        if (!file.contains(".txt")) file = file + ".txt";
        BufferedReader reader = new BufferedReader(new FileReader(file));
        StringBuilder array = new StringBuilder();


        int bufferSize = 50;
        char[] buffer = new char[bufferSize];


        while (true) {

            //if file is empty add basic metadata
            if (reader.read(buffer) == -1) {
                BufferedWriter writer = new BufferedWriter(new FileWriter(file));
                ArrayList<String> attributes = tables.get(file.substring(0, file.length()-4));
                metadata = new String[1 + attributes.size()];
                int in = 0;

                writer.write("$");

                for (String atr : attributes) {
                    writer.write(atr);
                    writer.write(" ");
                    metadata[in++] = atr;
                }

                writer.write("0");
                metadata[in] = "0";
                return;
            }

            //otherwise locate and read metadata and cut redundnant buffer part if needed
            String input = String.valueOf(buffer);
            int ind;
            if ((ind = input.indexOf('\u0000')) != -1) input = input.substring(0, ind);
            int index = input.indexOf('$');

            if (index != -1) {
                array.append(input.substring(index + 1));
                buffer = new char[bufferSize];
                int i = reader.read(buffer);
                while (i != -1) {
                    input = String.valueOf(buffer);
                    if ((index = input.indexOf('\u0000')) != -1) input = input.substring(0, index);
                    array.append(input);
                    buffer = new char[bufferSize];
                    i = reader.read(buffer);
                }
                break;
            }
            buffer = new char[bufferSize];
        }

        metadata = array.toString().split(" ");
    }

    public void insert(String into, String...values) {
        if (!tables.containsKey(into) || values.length == 0) return;

        try {
            readMetadata(into);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> newMetadata = new ArrayList<>();
        int dataLength = 0;
        for (String s : values) dataLength += s.length() + 1;
        newMetadata.add(metadata[metadata.length-1]);
        newMetadata.add(values[0]);
        newMetadata.add(String.valueOf(Integer.valueOf(metadata[metadata.length-1]) + dataLength));

        RandomAccessFile writer;

        try {
            writer = new RandomAccessFile(into + ".txt", "rw");
            writer.seek(Integer.valueOf(metadata[metadata.length-1]));
            for (String s : values) {
                writer.writeBytes(s);
                writer.writeBytes("%");
            }

            writer.writeBytes("$");

            for (int i = 0; i < metadata.length - 1; i++) {
                writer.writeBytes(metadata[i]);
                writer.writeBytes(" ");
            }

            for (String s : newMetadata) {
                writer.writeBytes(s);
                writer.writeBytes(" ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Integer[] search(String in, String where) {
        if (!tables.containsKey(in)) return null;

        try {
            readMetadata(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Integer> toReturnList = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(in + ".txt"));

            for (int i = tables.get(in).size(); i < metadata.length - 2; i += 2) {
                char[] buffer = new char[Integer.valueOf(metadata[i + 2]) - Integer.valueOf(metadata[i])];
                reader.read(buffer);
                if (String.valueOf(buffer).contains(where)) toReturnList.add(i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Integer[] toReturn = new Integer[toReturnList.size()];
        return toReturnList.toArray(toReturn);
    }

    public void delete(String from, String where) {
        if (!tables.containsKey(from)) return;

        Integer[] fromInd = search(from, where);
        if (fromInd.length == 0) return;


        try {
            for (int i : fromInd) {
                BufferedReader reader = new BufferedReader(new FileReader(from + ".txt"));
                File temp = new File("temp.txt");
                if (!temp.exists()) temp.createNewFile();
                BufferedWriter writer = new BufferedWriter(new FileWriter(temp.getAbsoluteFile()));

                char[] buffer;
                if (!metadata[i].equals("0")) {
                    buffer = new char[Integer.valueOf(metadata[i])
                            - Integer.valueOf(metadata[tables.get(from).size()]) - 1];
                    reader.read(buffer);
                    writer.write(buffer);
                }


                int size = Integer.valueOf(metadata[i + 2]) - Integer.valueOf(metadata[i]);
                buffer = new char[size];
                reader.read(buffer);

                buffer = new char[Integer.valueOf(metadata[metadata.length - 1]) - Integer.valueOf(metadata[i + 2])];
                reader.read(buffer);
                writer.write(buffer);

                List<String> newMetadata = new ArrayList<>();
                for (int j = 0; j < i; j++) newMetadata.add(metadata[j]);

                for (int j = i + 2; j < metadata.length - 2; j += 2) {
                    newMetadata.add(String.valueOf(Integer.valueOf(metadata[j]) - size));
                    newMetadata.add(metadata[j + 1]);
                }
                newMetadata.add(String.valueOf(Integer.valueOf(metadata[metadata.length - 1]) - size));

                metadata = new String[newMetadata.size()];
                newMetadata.toArray(metadata);

                writer.write("$");
                for (String s : metadata) {
                    writer.write(s);
                    writer.write(" ");
                }
                writer.close();
                File original = new File(from + ".txt").getAbsoluteFile();
                original.delete();
                temp.renameTo(original);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void update(String in, String where, String...values) {
        if (!tables.containsKey(in) || values.length == 0) return;

        delete(in, where);
        insert(in, values);
    }

    public static void main(String[] args) {
        DataBase dataBase = new DataBase();
        dataBase.addTable("Student", "id", "name", "email", "address");
        dataBase.addTable("Employee", "id", "name", "designation", "address");

        dataBase.insert("Student", "123", "Dima", "d.kapitun@innopolis.ru", "Innopolis");
        dataBase.insert("Student", "456", "Maria", "m.koreneva@innopolus.ru", "Khabarovsk");
        dataBase.insert("Student", "789", "Shalala", "sh.lalala@shalala.la", "Shalal");

        try {
            dataBase.readMetadata("Student");
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String s : dataBase.metadata) System.out.println(s);

        System.out.println("Updating");
        dataBase.update("Student", "Dima", "123", "Dmitry", "d.kapitun@innopolis.ru", "Innopolis");

        try {
            dataBase.readMetadata("Student");
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String s : dataBase.metadata) System.out.println(s);

        System.out.println("Deleting");
        dataBase.delete("Student", "123");

        try {
            dataBase.readMetadata("Student");
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String s : dataBase.metadata) System.out.println(s);
    }
}
