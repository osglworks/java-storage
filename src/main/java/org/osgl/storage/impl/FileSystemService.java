package org.osgl.storage.impl;

import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.storage.KeyGenerator;
import org.osgl.util.E;
import org.osgl.util.IO;
import org.osgl.util.S;

import java.io.*;
import java.util.Map;
import java.util.Properties;

public class FileSystemService extends StorageServiceBase implements IStorageService {

    public static final String CONF_HOME_DIR = "storage.fs.home.dir";
    public static final String CONF_HOME_URL = "storage.fs.home.url";
    public static final String CONF_LAZY_LOAD = "storage.fs.lazy";
    public static final String CONF_GET_NO_GET = "storage.fs.get.noGet";

    private File root_ = null;
    private String urlRoot_ = null;
    private boolean lazyLoad_ = false;
    private boolean noGet_ = false;

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
        urlRoot_ = conf.get(CONF_HOME_URL);
        if (null == urlRoot_) return;
        urlRoot_ = urlRoot_.replace('\\', '/');
        if (!urlRoot_.endsWith("/")) {
            urlRoot_ = urlRoot_ + '/';
        }

        Object o = conf.get(CONF_LAZY_LOAD);
        if (null != o) {
            lazyLoad_ = Boolean.parseBoolean(o.toString());
        }

        o = conf.get(CONF_GET_NO_GET);
        if (null != o) {
            noGet_ = Boolean.parseBoolean(o.toString());
        }

        logger.debug("FileSystemService configured. home: %s, url root: %s", root_, urlRoot_);
    }

    public FileSystemService(KeyGenerator keygen) {
        super(keygen);
    }

    public FileSystemService(Map<String, String> conf) {
        configure(conf);
    }

    public File root() {
        return root_;
    }

    @Override
    public String getUrl(String key) {
        return null == urlRoot_ ? null : urlRoot_ + key;
    }

    @Override
    public ISObject get(String key) {
        if (noGet_) {
            return SObject.getDumpObject(key);
        }
        key = key.replace('\\', '/');
        String[] path = key.split("/");
        int l = path.length;
        File f = root_, fa = null;
        for (int i = 0; i < l; ++i) {
            f = IO.child(f, path[i]);
            fa = IO.child(f.getParentFile(), path[i] + ".attr");
        }
        ISObject obj = SObject.of(key, f);
        if (fa.exists()) {
            try {
                Properties p = new Properties();
                InputStream is = IO.buffered(IO.is(fa));
                p.load(is);
                is.close();
                for (Object o : p.keySet()) {
                    obj.setAttribute(o.toString(), S.string(p.get(o)));
                }
            } catch (IOException e) {
                E.ioException(e);
            }
        }
        return obj;
    }

    @Override
    public ISObject loadContent(ISObject sobj) {
        String key = sobj.getKey();
        key = key.replace('\\', '/');
        String[] path = key.split("/");
        int l = path.length;
        File f = root_;
        for (int i = 0; i < l; ++i) {
            f = IO.child(f, path[i]);
        }
        Map<String, String> attrs = sobj.getAttributes();
        sobj = SObject.of(key, f);
        sobj.setAttributes(attrs);
        return sobj;
    }

    @Override
    public ISObject put(String key, ISObject stuff) {
        E.NPE(stuff);
        if (stuff instanceof SObject.FileSObject && S.eq(key, stuff.getKey())) {
            return stuff;
        }
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
        SObject.FileSObject fsobj = new SObject.FileSObject(key, fObj);
        fsobj.setAttributes(stuff.getAttributes());
        return fsobj;
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
