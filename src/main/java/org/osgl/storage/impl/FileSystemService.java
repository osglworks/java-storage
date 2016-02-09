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
    public static final String CONF_GET_NO_GET = "storage.fs.get.noGet";

    private File root_ = null;
    private String urlRoot_ = null;
    private boolean noGet_ = false;

    public void configure(Map<String, String> conf) {
        super.configure(conf);

        String s = conf.get(CONF_HOME_DIR);
        root_ = new File(s);
        if (!root_.exists() && !root_.mkdir()) {
            throw E.invalidConfiguration("Cannot create root dir: %s", root_.getAbsolutePath());
        } else if (!root_.isDirectory()) {
            throw E.invalidConfiguration("Root dir specified is not a directory: %s", root_.getAbsolutePath());
        }

        urlRoot_ = conf.get(CONF_HOME_URL);
        if (null == urlRoot_) return;
        urlRoot_ = urlRoot_.replace('\\', '/');
        if (!urlRoot_.endsWith("/")) {
            urlRoot_ = urlRoot_ + '/';
        }

        Object o = conf.get(CONF_GET_NO_GET);
        if (null != o) {
            noGet_ = Boolean.parseBoolean(o.toString());
        }

        logger.debug("FileSystemService configured. home: %s, url root: %s", root_, urlRoot_);
    }

    public FileSystemService(KeyGenerator keygen) {
        super(keygen);
    }

    public FileSystemService(KeyGenerator keygen, String contextPath) {
        super(keygen, contextPath);
    }

    public FileSystemService(Map<String, String> conf) {
        configure(conf);
    }

    public File root() {
        return root_;
    }

    @Override
    public String getUrl(String key) {
        return null == urlRoot_ ? null : urlRoot_ + keyWithContextPath(key);
    }

    @Override
    public ISObject get(String key) {
        E.illegalArgumentIf(S.blank(key));
        if (noGet_) {
            return SObject.getDumpObject(key);
        }
        key = key.replace('\\', '/');
        String[] path = keyWithContextPath(key).split("/");
        int l = path.length;
        assert l > 0;
        File f = root_, fa = null;
        for (int i = 0; i < l; ++i) {
            f = IO.child(f, path[i]);
            fa = IO.child(f.getParentFile(), path[i] + ".attr");
        }
        assert fa != null;
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
                throw E.ioException(e);
            }
        }
        obj.setAttribute(ISObject.ATTR_SS_ID, id());
        obj.setAttribute(ISObject.ATTR_SS_CTX, contextPath());
        if (null != urlRoot_) {
            obj.setAttribute(ISObject.ATTR_URL, getUrl(key));
        }
        return obj;
    }

    @Override
    public ISObject loadContent(ISObject sobj) {
        String key = sobj.getKey();
        key = key.replace('\\', '/');
        String[] path = keyWithContextPath(key).split("/");
        int l = path.length;
        File f = root_;
        for (int i = 0; i < l; ++i) {
            f = IO.child(f, path[i]);
        }
        Map<String, String> attrs = sobj.getAttributes();
        sobj = SObject.of(key, f);
        sobj.setAttributes(attrs);
        sobj.setAttribute(ISObject.ATTR_SS_ID, id());
        sobj.setAttribute(ISObject.ATTR_SS_CTX, contextPath());
        if (null != urlRoot_) {
            sobj.setAttribute(ISObject.ATTR_URL, getUrl(key));
        }
        return sobj;
    }

    @Override
    public ISObject put(String key, ISObject stuff) {
        E.NPE(stuff);
        if (stuff instanceof SObject.FileSObject && S.eq(key, stuff.getKey()) && S.eq(id(), stuff.getAttribute(ISObject.ATTR_SS_ID)) && S.eq(contextPath(), stuff.getAttribute(ISObject.ATTR_SS_CTX))) {
            return stuff;
        }
        key = key.replace('\\', '/');
        String[] path = keyWithContextPath(key).split("/");
        int l = path.length;
        File f = root_;
        for (int i = 0; i < l - 1; ++i) {
            f = IO.child(f, path[i]);
            if (!f.exists() && !f.mkdir()) {
                throw E.ioException("Cannot create directory: %s", f.getAbsolutePath());
            } else if (!f.isDirectory()) {
                throw E.ioException("cannot store the object into storage: %s is not a directory", f);
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
                throw E.ioException(e);
            }
            IO.close(os);
        }
        SObject.FileSObject fsobj = new SObject.FileSObject(key, fObj);
        fsobj.setAttributes(stuff.getAttributes());
        fsobj.setAttribute(ISObject.ATTR_SS_ID, id());
        fsobj.setAttribute(ISObject.ATTR_SS_CTX, contextPath());
        if (null != urlRoot_) {
            fsobj.setAttribute(ISObject.ATTR_URL, getUrl(key));
        }
        return fsobj;
    }

    @Override
    public void remove(String key) {
        key = key.replace('\\', '/');
        key = keyWithContextPath(key);
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

    @Override
    protected IStorageService createSubFolder(String path) {
        FileSystemService subFolder = new FileSystemService(conf);
        subFolder.keygen = keygen;
        subFolder.contextPath = keyWithContextPath(path);
        return subFolder;
    }
}
