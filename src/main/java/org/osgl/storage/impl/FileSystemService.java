package org.osgl.storage.impl;

import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.storage.KeyGenerator;
import org.osgl.util.E;
import org.osgl.util.IO;

import java.io.*;
import java.util.Map;
import java.util.Properties;

public class FileSystemService extends StorageServiceBase implements IStorageService {

    public static final String CONF_HOME_DIR = "storage.fs.home.dir";
    public static final String CONF_HOME_URL = "storage.fs.home.url";

    private File root_ = null;
    private String urlRoot_ = null;

    public void configure(Map<String, String> conf) {
        super.configure(conf);
        
        if (null == conf) throw new NullPointerException();

        String s = conf.get(CONF_HOME_DIR);
        root_ = new File(s);
        if (!root_.exists()) {
            root_.mkdir();
        } else if (!root_.isDirectory()) {
            E.invalidConfiguration("cannot create root dir for file storage");
        }
        urlRoot_ = conf.get(CONF_HOME_URL).replace('\\', '/');
        if (!urlRoot_.endsWith("/")) {
            urlRoot_ = urlRoot_ + '/';
        }
    }

    public FileSystemService(KeyGenerator keygen) {
        super(keygen);
    }

    public FileSystemService(Map<String, String> conf) {
        configure(conf);
    }

    @Override
    public String getUrl(String key) {
        return urlRoot_ + key;
    }

    @Override
    public String getKey(String key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ISObject get(String key) {
        key = key.replace('\\', '/');
        String[] path = key.split("/");
        int l = path.length;
        File f = root_;
        for (int i = 0; i < l; ++i) {
            f = IO.child(f, path[i]);
        }
        return SObject.valueOf(key, f);
    }

    @Override
    public void put(String key, ISObject stuff) {
        key = key.replace('\\', '/');
        String[] path = key.split("/");
        int l = path.length;
        File f = root_;
        for (int i = 0; i < l - 1; ++i) {
            f = IO.child(f, path[i]);
            if (!f.exists()) {
                f.mkdir();
            } else {
                if (!f.isDirectory()) {
                    E.ioException("cannot store the object into storage: %s is not a directory");
                }
            }
        }
        File fObj = IO.child(f, path[l - 1]);
        OutputStream os = new BufferedOutputStream(IO.os(fObj));
        IO.write(IO.buffered(stuff.asInputStream()), os);
        
        if (stuff.hasAttribute()) {
            File fAttr = IO.child(f, path[l - 1] + ".attr");
            os = IO.buffered(IO.os(fAttr));
            Properties p = new Properties();
            p.putAll(stuff.getAttributes());
            try {
                p.store(os, "");
            } catch (IOException e) {
                E.ioException(e);
            }
            IO.close(os);
        }
    }

    @Override
    public void remove(String key) {
        key = key.replace('\\', '/');
        String[] path = key.split("/");
        int l = path.length;
        File f = root_;
        for (int i = 0; i < l - 1; ++i) {
            f = IO.child(f, path[i]);
        }
        File fObj = IO.child(f, path[l - 1]);
        IO.delete(fObj);
        File fAttr = IO.child(f, path[l - 1] + ".attr");
        IO.delete(fAttr);
    }
}
