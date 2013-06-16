package com.greenlaw110.storage.impl;

import com.greenlaw110.storage.ISObject;
import com.greenlaw110.storage.IStorageService;
import com.greenlaw110.util.E;
import com.greenlaw110.util.IO;

import java.io.*;
import java.util.Map;
import java.util.Properties;

public class FileSystemService implements IStorageService {

    private File root_ = null;
    private String urlRoot_ = null;

    public void configure(Map<String, String> conf) {
        if (null == conf) throw new NullPointerException();

        String s = conf.get("storage.file.dir");
        root_ = new File(s);
        if (!root_.exists()) {
            root_.mkdir();
        } else if (!root_.isDirectory()) {
            E.configException("cannot create root dir for file storage");
        }
        urlRoot_ = conf.get("storage.url.root").replace('\\', '/');
        if (!urlRoot_.endsWith("/")) {
            urlRoot_ = urlRoot_ + '/';
        }
    }

    public FileSystemService(Map<String, String> conf) {
        configure(conf);
    }

    @Override
    public String getUrl(String key) {
        return urlRoot_ + key;
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
        return SObject.asSObject(key, f);
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
        IO.write(new BufferedInputStream(stuff.asInputStream()), os);
        
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
