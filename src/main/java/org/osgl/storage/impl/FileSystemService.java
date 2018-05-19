package org.osgl.storage.impl;

/*-
 * #%L
 * Java Storage Service
 * %%
 * Copyright (C) 2013 - 2017 OSGL (Open Source General Library)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.osgl.$;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.util.C;
import org.osgl.util.E;
import org.osgl.util.IO;

import java.io.*;
import java.util.Map;
import java.util.Properties;

public class FileSystemService extends StorageServiceBase<FileObject> implements IStorageService {

    public static final String CONF_HOME_DIR = "storage.fs.home.dir";

    /**
     * The configuration of static URL root of this file system storage.
     * <p>
     * Note, this configuration item is deprecated. Please use
     * {@link StorageServiceBase#CONF_STATIC_WEB_ENDPOINT} instead
     * </p>
     */
    @Deprecated
    public static final String CONF_HOME_URL = "storage.fs.home.url";

    /**
     * This configuration item has been deprecated. Please use
     * {@link StorageServiceBase#CONF_GET_NO_GET} instead
     */
    @Deprecated
    public static final String CONF_FS_GET_NO_GET = "storage.fs.get.noGet";

    private File root_;

    public FileSystemService(Map<String, String> conf) {
        super(conf, FileObject.class);
    }

    @Override
    protected void configure(Map<String, String> conf) {
        conf = C.newMap(conf);
        if (!conf.containsKey(CONF_STATIC_WEB_ENDPOINT) && conf.containsKey(CONF_HOME_URL)) {
            conf.put(CONF_STATIC_WEB_ENDPOINT, conf.get(CONF_HOME_URL));
        }
        if (!conf.containsKey(CONF_GET_NO_GET) && conf.containsKey(CONF_FS_GET_NO_GET)) {
            conf.put(CONF_GET_NO_GET, conf.get(CONF_FS_GET_NO_GET));
        }
        super.configure(conf, "fs");

        String s = conf.get(CONF_HOME_DIR);
        root_ = new File(s);
        if (!root_.exists() && !root_.mkdirs()) {
            throw E.invalidConfiguration("Cannot create root dir: %s", root_.getAbsolutePath());
        } else if (!root_.isDirectory()) {
            throw E.invalidConfiguration("Root dir specified is not a directory: %s", root_.getAbsolutePath());
        }
    }

    @SuppressWarnings("unused")
    public File root() {
        return root_;
    }

    @Override
    protected void doRemove(String fullPath) {
        doOperate(fullPath, DELETE_FILE, DELETE_FILE);
    }

    @Override
    protected Map<String, String> doGetMeta(String fullPath) {
        InputStream is = doOperate(fullPath, null, SAFE_GET_INPUT_STREAM);
        if (null == is) {
            return C.newMap();
        }
        Properties p = new Properties();
        try {
            p.load(is);
        } catch (IOException e) {
            throw E.ioException(e);
        } finally {
            IO.close(is);
        }
        return $.cast(p);
    }

    protected File doGetFile(String fullPath) {
        return doOperate(fullPath, $.F.<File>identity(), null);
    }

    @Override
    protected InputStream doGetInputStream(String fullPath) {
        return doOperate(fullPath, GET_INPUT_STREAM, null);
    }

    protected File getFile(String fullPath) {
        fullPath = fullPath.replace('\\', '/');
        String[] path = fullPath.split("/");
        int len = path.length;
        assert len > 0;
        File folder = root_;
        for (int i = 0; i < len - 1; ++i) {
            folder = IO.child(folder, path[i]);
            if (folder.exists() && !folder.isDirectory()) {
                throw E.ioException("cannot store the object into storage: %s is not a directory", folder);
            }
        }
        return new File(folder, path[len - 1]);
    }

    private <T> T doOperate(String fullPath, $.Function<File, T> blobOperator, $.Function<File, T> attrOperator) {
        return doOperate(fullPath, blobOperator, attrOperator, false);
    }

    private <T> T doOperate(String fullPath, $.Function<File, T> blobOperator, $.Function<File, T> attrOperator, boolean mkdir) {
        File file = getFile(fullPath);
        T retVal = null;
        if (mkdir) {
            File dir = file.getParentFile();
            if (!dir.exists() && !dir.mkdirs()) {
                throw E.ioException("Cannot create dir: " + dir.getAbsolutePath());
            }
        }
        if (null != blobOperator) {
            retVal = blobOperator.apply(file);
        }
        if (null != attrOperator) {
            File fAttr = new File(file.getParent(), file.getName() + ".attr");
            if (null != retVal) {
                retVal = attrOperator.apply(fAttr);
            } else {
                attrOperator.apply(fAttr);
            }
        }
        return retVal;
    }

    @Override
    protected ISObject newSObject(String key) {
        return new FileObject(key, this);
    }

    @Override
    protected void doPut(String fullPath, ISObject stuff, Map<String, String> attrs) {
        doOperate(fullPath, writeBlob(stuff), writeAttributes(attrs), true);
    }

    @Override
    protected StorageServiceBase newService(Map<String, String> conf) {
        return new FileSystemService(conf);
    }

    private static final $.Transformer<File, InputStream> SAFE_GET_INPUT_STREAM = new $.Transformer<File, InputStream>() {
        @Override
        public InputStream transform(File file) {
            return file.canRead() ? IO.inputStream(file) : IO.inputStream();
        }
    };

    private static final $.Transformer<File, InputStream> GET_INPUT_STREAM = new $.Transformer<File, InputStream>() {
        @Override
        public InputStream transform(File file) {
            return IO.inputStream(file);
        }
    };

    private static final $.Visitor<File> DELETE_FILE = new $.Visitor<File>() {
        @Override
        public void visit(File file) throws $.Break {
            IO.delete(file);
        }
    };

    private static $.Visitor<File> writeBlob(final ISObject sobj) {
        return new $.Visitor<File>() {
            @Override
            public void visit(File file) throws $.Break {
                OutputStream os = new BufferedOutputStream(IO.outputStream(file));
                IO.write(IO.buffered(sobj.asInputStream()), os);
            }
        };
    }

    private static $.Visitor<File> writeAttributes(final Map<String, String> attrs) {
        return new $.Visitor<File>() {
            @Override
            public void visit(File file) throws $.Break {
                if (null != attrs && !attrs.isEmpty()) {
                    OutputStream os = IO.buffered(IO.outputStream(file));
                    Properties p = new Properties();
                    p.putAll(attrs);
                    try {
                        p.store(os, "");
                    } catch (IOException e) {
                        throw E.ioException(e);
                    }
                    IO.close(os);
                }
            }
        };
    }
}
