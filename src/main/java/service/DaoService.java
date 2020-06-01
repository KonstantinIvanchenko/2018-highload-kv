package service;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.util.NoSuchElementException;

public class DaoService implements KVDao {
    @NotNull
    private final File dir;

    public DaoService(@NotNull final File data){
        this.dir = data;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws IOException, NoSuchElementException {

        final File file = new File(dir, new String(key));

        if (!file.exists()){
            throw new NoSuchElementException("No such file");
        }


        final byte[] value = new byte[(int) file.length()];

        try(InputStream is = new FileInputStream(file)){
            if (is.read(value) != value.length){
                throw new IOException("Can't read file..");
            }
        }
        return value;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {

        final File file = new File(dir, new String(key));

        try(OutputStream os = new FileOutputStream(file, false)){
            os.write(value);
        }
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {

        String keyString = new String(key);

        if (keyString.contains("*")){
            String keyAllFiles = keyString.substring(0, keyString.indexOf("*"));

            for(File f : this.dir.listFiles())
                if (f.getName().startsWith(keyAllFiles))
                    f.delete();
        }else{
            final File file = new File(dir, keyString);
            file.delete();
        }
    }

    @Override
    public void close() throws IOException {

    }
}
