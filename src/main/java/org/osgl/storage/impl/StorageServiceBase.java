package org.osgl.storage.impl;

import org.osgl.$;
import org.osgl.logging.L;
import org.osgl.logging.Logger;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.storage.KeyGenerator;
import org.osgl.util.C;
import org.osgl.util.S;

import java.io.InputStream;
import java.util.Map;

import static org.osgl.storage.KeyGenerator.BY_DATE;

/**
 * The implementation base of {@link IStorageService}
 */
public abstract class StorageServiceBase<SOBJ_TYPE extends SObject> implements IStorageService {

    protected static Logger logger = L.get(StorageServiceBase.class);
    /**
     * For certain case for example, gallery application, it doesn't need the GET operation because end
     * user can access the object directly from aws' static web service
     */
    public static final String CONF_GET_NO_GET = "storage.get.noGet";

    /**
     * Get Meta Data only
     */
    public static final String CONF_GET_META_ONLY = "storage.get.MetaOnly";

    /**
     * The static URL to retrieve cloud object
     */
    public static final String CONF_STATIC_WEB_ENDPOINT = "storage.staticWebEndpoint";


    private String staticWebEndPoint = null;
    private boolean loadMetaOnly = false;
    private boolean noGet = false;


    private Class<SOBJ_TYPE> sobjType;
    private String id;
    protected KeyGenerator keygen;
    protected String contextPath;
    protected Map<String, String> conf = C.newMap();
    protected Map<String, IStorageService> subFolders = C.newMap();

    public StorageServiceBase(Map<String, String> conf, Class<SOBJ_TYPE> sobjType) {
        this.sobjType = $.notNull(sobjType);
        configure(conf);
    }

    protected void configure(Map<String, String> conf) {
        if (conf.containsKey(CONF_ID)) {
            id = conf.get(CONF_ID);
        } else {
            id = DEFAULT;
        }
        if (conf.containsKey(CONF_KEY_GEN)) {
            String s = conf.get(CONF_KEY_GEN);
            keygen = KeyGenerator.valueOfIgnoreCase(s);
        } else {
            keygen = BY_DATE;
        }
        if (conf.containsKey(CONF_CONTEXT_PATH)) {
            String s = conf.get(CONF_CONTEXT_PATH);
            contextPath = canonicalContextPath(s);
        } else {
            contextPath = "";
        }

        staticWebEndPoint = conf.get(CONF_STATIC_WEB_ENDPOINT);
        loadMetaOnly = Boolean.parseBoolean(getConfValue(conf, CONF_GET_META_ONLY, "false"));
        noGet = Boolean.parseBoolean(getConfValue(conf, CONF_GET_NO_GET, "false"));

        this.conf.putAll(conf);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String contextPath() {
        return getContextPath();
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
        if ("".equals(contextPath)) {
            return key;
        }
        StringBuilder sb = new StringBuilder(contextPath);
        if (!key.startsWith("/")) {
            sb.append("/");
        }
        sb.append(key);
        return sb.toString();
    }

    @Override
    public synchronized IStorageService subFolder(String path) {
        IStorageService ss = subFolders.get(path);
        if (null == ss) {
            ss = createSubFolder(path);
            subFolders.put(path, ss);
        }
        return ss;
    }

    @Override
    public final ISObject get(String key) {
        if (noGet) {
            return SObject.getDumpObject(key);
        }
        if (loadMetaOnly) {
            return SObject.getDumpObject(key, getMeta(key));
        }
        return getFull(key);
    }

    @Override
    public final ISObject getFull(String key) {
        ISObject sobj = newSObject(key);
        setDefAttributes(sobj);
        return sobj;
    }

    @Override
    public ISObject loadContent(ISObject sobj) {
        return getFull(sobj.getKey());
    }

    @Override
    public ISObject put(String key, ISObject stuff) {
        if (S.eq(key, stuff.getKey()) && isManagedObject(stuff)) {
            return stuff;
        }

        Map<String, String> attrs = stuff.getAttributes();
        if (!(stuff instanceof SObject.InputStreamSObject)) {
            attrs.put(ISObject.ATTR_CONTENT_LENGTH, S.string(stuff.getLength()));
        }
        doPut(keyWithContextPath(key), stuff, attrs);
        return getFull(key);
    }

    @Override
    public String getUrl(String key) {
        if (null == staticWebEndPoint) {
            return null;
        }
        return "//" + staticWebEndPoint + "/" + keyWithContextPath(key);
    }

    protected final IStorageService createSubFolder(String path) {
        StorageServiceBase subFolder = newService(conf);
        subFolder.keygen = this.keygen;
        subFolder.contextPath = keyWithContextPath(path);
        return subFolder;
    }

    protected Map<String, String> getMeta(String key) {
        return getMeta(key, false);
    }

    protected Map<String, String> getMeta(String key, boolean force) {
        if (noGet && !force) return C.map();
        Map<String, String> map = doGetMeta(keyWithContextPath(key));
        setDefAttributes(key, map);
        return map;
    }

    @Override
    public final void remove(String key) {
        doRemove(keyWithContextPath(key));
    }

    final InputStream getInputStream(String key) {
        return doGetInputStream(keyWithContextPath(key));
    }

    protected final String getConfValue(Map<String, String> conf, String key, String def) {
        String val = conf.get(key);
        return null == val ? def : val;
    }

    private void setDefAttributes(ISObject sobj) {
        Map<String, String> map = C.newMap();
        setDefAttributes(sobj.getKey(), map);
        sobj.setAttributes(map);
    }

    private void setDefAttributes(String key, Map<String, String> map) {
        map.put(ISObject.ATTR_SS_ID, id());
        map.put(ISObject.ATTR_SS_CTX, contextPath());
        if (null != staticWebEndPoint) {
            map.put(ISObject.ATTR_URL, getUrl(key));
        }
    }

    // Check if the specified sobj is an sobject managed by this service
    private boolean isManagedObject(ISObject sobj) {
        return serviceMatches(sobj) && typeMatches(sobj);
    }

    private boolean typeMatches(ISObject sobj) {
        return sobjType.isAssignableFrom(sobj.getClass());
    }

    private boolean serviceMatches(ISObject sobj) {
        return serviceIdMatches(sobj) && serviceContextPathMatches(sobj);
    }

    private boolean serviceIdMatches(ISObject sobj) {
        return S.eq(id(), sobj.getAttribute(ISObject.ATTR_SS_ID));
    }

    private boolean serviceContextPathMatches(ISObject sobj) {
        return S.eq(contextPath(), S.string(sobj.getAttribute(ISObject.ATTR_SS_CTX)));
    }

    /**
     * Remove the storage object specified by fullPath. The fullPath is composed of
     * * {@link #contextPath()}
     * * {@link ISObject#getKey()}
     *
     * @param fullPath the full path to locate the storage object
     */
    protected abstract void doRemove(String fullPath);

    /**
     * Returns the meta attributes from fullPath specified. The fullPath is composed of
     * * {@link #contextPath()}
     * * {@link ISObject#getKey()}
     *
     * @param fullPath the full path to locate the storage object
     * @return the meta attributes of the storage object
     */
    protected abstract Map<String, String> doGetMeta(String fullPath);

    /**
     * Returns the input stream from fullPath specified. The fullPath is composed of
     * * {@link #contextPath()}
     * * {@link ISObject#getKey()}
     *
     * @param fullPath the full path to locate the storage object
     * @return the input stream to get the storage object
     */
    protected abstract InputStream doGetInputStream(String fullPath);

    /**
     * Put the storage object specified by fullPath into the storage service. The fullPath is composed of
     * * {@link #contextPath()}
     * * {@link ISObject#getKey()}
     *
     * @param fullPath the full path to locate the storage object
     * @param attrs    the meta attributes of the storage object
     */
    protected abstract void doPut(String fullPath, ISObject stuff, Map<String, String> attrs);

    protected abstract ISObject newSObject(String key);

    protected abstract StorageServiceBase newService(Map<String, String> conf);
}
