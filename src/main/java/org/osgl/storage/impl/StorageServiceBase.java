package org.osgl.storage.impl;

import org.osgl.$;
import org.osgl.logging.L;
import org.osgl.logging.Logger;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.storage.KeyGenerator;
import org.osgl.util.C;
import org.osgl.util.E;

import java.util.Map;

import static org.osgl.storage.KeyGenerator.BY_DATE;

/**
 * The implementation base of {@link IStorageService} 
 */
public abstract class StorageServiceBase implements IStorageService {

    protected static Logger logger = L.get(StorageServiceBase.class);

    private String id;
    protected KeyGenerator keygen;
    protected String contextPath;
    protected Map<String, String> conf = C.newMap();

    protected StorageServiceBase(){
        keygen = BY_DATE;
        contextPath = "";
        id = DEFAULT;
    }
    protected StorageServiceBase(KeyGenerator keygen) {
        this.keygen = $.notNull(keygen);
        this.contextPath = "";
        this.id = DEFAULT;
    }
    protected StorageServiceBase(KeyGenerator keygen, String contextPath) {
        this.keygen = $.notNull(keygen);
        this.contextPath = canonicalContextPath(contextPath);
        this.id = DEFAULT;
    }

    @Override
    public void configure(Map<String, String> conf) {
        if (conf.containsKey(CONF_ID)) {
            id = conf.get(CONF_ID);
        } else {
            id = DEFAULT;
        }
        if (conf.containsKey(CONF_KEY_GEN)) {
            String s = conf.get(CONF_KEY_GEN);
            keygen = KeyGenerator.valueOfIgnoreCase(s);
        }
        if (conf.containsKey(CONF_CONTEXT_PATH)) {
            String s = conf.get(CONF_CONTEXT_PATH);
            contextPath = canonicalContextPath(s);
        }
        this.conf.putAll(conf);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String getKey(String key) {
        return keygen.getKey(key);
    }

    @Override
    public String getKey() {
        return keygen.getKey();
    }

    @Override
    public ISObject forceGet(String key) {
        return getFull(key);
    }

    @Override
    public ISObject getFull(String key) {
        return get(key);
    }

    @Override
    public ISObject getLazy(String key, Map<String, String> attrs) {
        SObject sobj = SObject.lazyLoad(key, this);
        sobj.setAttributes(attrs);
        return sobj;
    }

    @Override
    public String getContextPath() {
        return contextPath;
    }

    static String canonicalContextPath(String path) {
        path = path.trim();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        if ("/".equals(path) || "".equals(path)) {
            path = "";
        }
        return path.intern();
    }

    protected String keyWithContextPath(String key) {
        if ("" == contextPath) {
            return key;
        }
        StringBuilder sb = new StringBuilder(contextPath);
        if (!key.startsWith("/")) {
            sb.append("/");
        }
        sb.append(key);
        return sb.toString();
    }

}
