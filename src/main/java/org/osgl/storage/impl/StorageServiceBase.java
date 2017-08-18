package org.osgl.storage.impl;

import org.osgl.$;
import org.osgl.logging.L;
import org.osgl.logging.Logger;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.storage.KeyGenerator;
import org.osgl.storage.KeyNameProvider;
import org.osgl.util.C;
import org.osgl.util.FastStr;
import org.osgl.util.S;

import java.io.InputStream;
import java.util.Map;

import static org.osgl.storage.KeyGenerator.Predefined.BY_DATE;

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

    /**
     * Whether it shall ignore the suffix in the key or not
     */
    public static final String CONF_STORE_SUFFIX = "storage.storeSuffix";

    /**
     * User supplied {@link KeyNameProvider}
     */
    public static final String CONF_KEY_NAME_PROVIDER = "storage.keyNameProvider";


    private String staticWebEndpoint = null;
    private boolean staticWebEndpointIsAbsolute = false;
    private boolean loadMetaOnly = false;
    private boolean noGet = false;
    private boolean storeSuffix = true;
    private KeyNameProvider keyNameProvider = KeyNameProvider.DEF_PROVIDER;


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
        configure(conf, "");
    }

    protected void configure(Map<String, String> conf, String prefix) {
        if (!prefix.endsWith(".")) {
            prefix = prefix + ".";
        }
        id = val(conf, CONF_ID, prefix);
        if (S.blank(id)) {
            id = DEFAULT;
        }

        String s = val(conf, CONF_CONTEXT_PATH, prefix);
        contextPath = S.blank(s) ? "" : canonicalContextPath(s);

        staticWebEndpoint = val(conf, CONF_STATIC_WEB_ENDPOINT, prefix);
        if (null != staticWebEndpoint) {
            if (staticWebEndpoint.endsWith("/")) {
                // strip off the last "/"
                staticWebEndpoint = staticWebEndpoint.substring(0, staticWebEndpoint.length() - 1);
            }
            staticWebEndpointIsAbsolute = staticWebEndpoint.startsWith("http") || staticWebEndpoint.startsWith("//");
            if (!staticWebEndpointIsAbsolute) {
                if (staticWebEndpoint.startsWith("/")) {
                    staticWebEndpoint = staticWebEndpoint.substring(1);
                }
            }
        }

        s = val(conf, CONF_GET_META_ONLY, prefix);
        loadMetaOnly = Boolean.parseBoolean(S.blank(s) ? "false" : s);

        s = val(conf, CONF_GET_NO_GET, prefix);
        noGet = Boolean.parseBoolean(S.blank(s) ? "false" : s);

        s = val(conf, CONF_STORE_SUFFIX, prefix);
        storeSuffix = Boolean.parseBoolean(S.blank(s) ? "true" : s);

        s = val(conf, CONF_KEY_NAME_PROVIDER, prefix);
        if (S.notBlank(s)) {
            keyNameProvider = $.newInstance(s);
        }

        s = val(conf, CONF_KEY_GEN, prefix);
        keygen = S.blank(s) ? BY_DATE : KeyGenerator.Predefined.valueOfIgnoreCase(s);
        if (null == keygen) {
            keygen = $.newInstance(s);
        }

        this.conf.putAll(conf);
    }

    /**
     * Allow framework to set the {@link KeyNameProvider} directly
     *
     * @param keyNameProvider the key name provider instance
     */
    public void setKeyNameProvider(KeyNameProvider keyNameProvider) {
        this.keyNameProvider = $.notNull(keyNameProvider);
    }

    /**
     * Allow framework to set the {@link KeyGenerator} directly
     *
     * @param keygen the key generator instance
     */
    public void setKeyGenerator(KeyGenerator keygen) {
        this.keygen = $.notNull(keygen);
    }

    private String val(Map<String, String> conf, String key, String prefix) {
        String val = conf.get(key);
        if (S.blank(val)) {
            if (key.startsWith("storage.")) {
                key = FastStr.of(key).insert(8, prefix).toString();
            }
            val = conf.get(key);
        }
        return val;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String contextPath() {
        return getContextPath();
    }

    /**
     * Returns the full path key from the key name
     * @param keyName the key name (which does not contains the hierarchical path)
     * @return the full path key
     */
    @Override
    public String getKey(String keyName) {
        return keygen.getKey(keyName, keyNameProvider);
    }

    /**
     * Returns a full path key with a generated unique key name
     * @return a full path key
     */
    @Override
    public String getKey() {
        return keygen.getKey(null, keyNameProvider);
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

    @Override
    public String getStaticWebEndpoint() {
        return staticWebEndpoint;
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
            return createDumbObject(key);
        }
        if (loadMetaOnly) {
            ISObject obj = createDumbObject(key);
            obj.setAttributes(getMeta(key));
            return obj;
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
        String processedKey = key;
        if (storeSuffix) {
            String originalFilename = stuff.getAttribute(ISObject.ATTR_FILE_NAME);
            if (null != originalFilename) {
                String suffix = S.afterLast(originalFilename, ".");
                if (S.notBlank(suffix)) {
                    suffix = S.concat(".", suffix);
                    if (!key.endsWith(suffix)) {
                        processedKey = S.concat(key, suffix);
                    }
                }
            }
        }

        if ((S.eq(key, stuff.getKey()) || S.eq(processedKey, stuff.getKey()) && isManagedObject(stuff))) {
            return stuff;
        }

        Map<String, String> attrs = stuff.getAttributes();
        removeRuntimeAttributes(attrs);
        if (!(stuff instanceof SObject.InputStreamSObject)) {
            long len = stuff.getLength();
            if (0L < len) {
                attrs.put(ISObject.ATTR_CONTENT_LENGTH, S.string(len));
            }
        }
        doPut(keyWithContextPath(processedKey), stuff, attrs);
        return getFull(processedKey);
    }

    // Runtime attributes are added by storage engine when loading the SObject
    private void removeRuntimeAttributes(Map<String, String> attrs) {
        attrs.remove(ISObject.ATTR_SS_ID);
        attrs.remove(ISObject.ATTR_URL);
        attrs.remove(ISObject.ATTR_SS_CTX);
    }

    @Override
    public boolean isManaged(ISObject sobj) {
        return S.eq(sobj.getAttribute(ISObject.ATTR_SS_ID), id()) && S.eq(sobj.getAttribute(ISObject.ATTR_SS_CTX), contextPath());
    }

    @Override
    public String getUrl(String key) {
        if (null == staticWebEndpoint) {
            return null;
        }
        if (staticWebEndpointIsAbsolute) {
            return new StringBuilder(staticWebEndpoint).append("/").append(key).toString();
        } else {
            return new StringBuilder("/").append(staticWebEndpoint).append("/").append(key).toString();
        }
    }

    protected final IStorageService createSubFolder(String path) {
        StorageServiceBase subFolder = newService(conf);
        subFolder.keygen = this.keygen;
        subFolder.contextPath = keyWithContextPath(path);
        if (S.notBlank(this.staticWebEndpoint)) {
            subFolder.staticWebEndpoint = new StringBuilder(this.staticWebEndpoint).append("/").append(path).toString();
        }
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
        if (null != staticWebEndpoint) {
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

    private ISObject createDumbObject(String key) {
        DumbObject obj = new DumbObject(key);
        setDefAttributes(obj);
        return obj;
    }

}
