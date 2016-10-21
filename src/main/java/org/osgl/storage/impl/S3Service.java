package org.osgl.storage.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.util.C;
import org.osgl.util.E;
import org.osgl.util.S;

import java.io.InputStream;
import java.util.Map;

/**
 * Implement {@link org.osgl.storage.IStorageService} on Amazon S3
 */
public class S3Service extends StorageServiceBase<S3Obj> implements IStorageService {

    public static enum StorageClass {
        STANDARD, REDUCED_REDUNDANCY, GLACIER;

        public static StorageClass valueOfIgnoreCase(String s, StorageClass def) {
            if (S.blank(s)) {
                return def;
            }
            s = s.trim();
            if ("r".equalsIgnoreCase(s) || "rrs".equalsIgnoreCase(s) || "rr".equalsIgnoreCase(s) || "reduced_redundancy".equalsIgnoreCase(s) || "reducedRedundancy".equalsIgnoreCase(s)) {
                return REDUCED_REDUNDANCY;
            } else if ("glacier".equalsIgnoreCase(s) || "g".equalsIgnoreCase(s)) {
                return GLACIER;
            } else if ("s".equalsIgnoreCase(s) || "standard".equalsIgnoreCase(s)) {
                return STANDARD;
            } else {
                return def;
            }
        }
    }

    public static final String CONF_KEY_ID = "storage.s3.keyId";
    public static final String CONF_KEY_SECRET = "storage.s3.keySecret";
    public static final String CONF_DEF_STORAGE_CLASS = "storage.s3.defStorageClass";
    public static final String CONF_BUCKET = "storage.s3.bucket";
    public static final String CONF_MAX_ERROR_RETRY = "storage.s3.maxErrorRetry";
    public static final String CONF_CONN_TIMEOUT = "storage.s3.connectionTimeout";
    public static final String CONF_SOCKET_TIMEOUT = "storage.s3.socketTimeout";
    public static final String CONF_TCP_KEEP_ALIVE = "storage.s3.tcpKeepAlive";
    public static final String CONF_MAX_CONN = "storage.s3.maxConnection";

    /**
     * <p>This configuration item is deprecated and might be removed from future versions.</p>
     * <p>
     * Please use {@link #CONF_GET_NO_GET} instead
     */
    @Deprecated
    public static final String CONF_S3_GET_NO_GET = "storage.s3.get.noGet";

    /**
     * <p>This configuration item is deprecated and might be removed from future versions.</p>
     * <p>
     * Please use {@link #CONF_GET_META_ONLY} instead
     */
    @Deprecated
    public static final String CONF_S3_GET_META_ONLY = "storage.s3.get.MetaOnly";

    /**
     * <p>This configuration item is deprecated and might be removed from future versions.</p>
     * <p>
     * Please use {@link #CONF_STATIC_WEB_ENDPOINT} instead
     */
    @Deprecated
    public static final String CONF_S3_STATIC_WEB_ENDPOINT = "storage.s3.staticWebEndpoint";

    public static final String ATTR_STORAGE_CLASS = "x-amz-storage-class";


    private String awsKeyId;
    private String awsKeySecret;
    private StorageClass defStorageClass = StorageClass.REDUCED_REDUNDANCY;
    private String bucket;

    public static AmazonS3 s3;

    public S3Service(Map<String, String> conf) {
        super(conf, S3Obj.class);
    }

    @Override
    protected void configure(Map<String, String> conf) {
        super.configure(conf);
        conf = C.newMap(conf);
        if (!conf.containsKey(CONF_GET_NO_GET) && conf.containsKey(CONF_S3_GET_NO_GET)) {
            conf.put(CONF_GET_NO_GET, conf.get(CONF_S3_GET_NO_GET));
        }
        if (!conf.containsKey(CONF_GET_META_ONLY) && conf.containsKey(CONF_S3_GET_META_ONLY)) {
            conf.put(CONF_GET_META_ONLY, conf.get(CONF_S3_GET_META_ONLY));
        }
        if (!conf.containsKey(CONF_STATIC_WEB_ENDPOINT) && conf.containsKey(CONF_S3_STATIC_WEB_ENDPOINT)) {
            conf.put(CONF_STATIC_WEB_ENDPOINT, conf.get(CONF_S3_STATIC_WEB_ENDPOINT));
        }
        super.configure(conf);
        awsKeyId = conf.get(CONF_KEY_ID);
        awsKeySecret = conf.get(CONF_KEY_SECRET);
        if (null == awsKeySecret || null == awsKeyId) {
            E.invalidConfiguration("AWS Key ID or AWS Key Secret not found in the configuration");
        }
        String sc = conf.get(CONF_DEF_STORAGE_CLASS);
        if (null != sc) {
            defStorageClass = StorageClass.valueOfIgnoreCase(sc, defStorageClass);
        }
        bucket = conf.get(CONF_BUCKET);
        if (null == bucket) {
            E.invalidConfiguration("AWS bucket not found in the configuration");
        }

        System.setProperty("line.separator", "\n");
        AWSCredentials cred = new BasicAWSCredentials(awsKeyId, awsKeySecret);

        ClientConfiguration cc = new ClientConfiguration();
        if (conf.containsKey(CONF_MAX_ERROR_RETRY)) {
            int n = Integer.parseInt(conf.get(CONF_MAX_ERROR_RETRY));
            cc = cc.withMaxErrorRetry(n);
        }
        if (conf.containsKey(CONF_CONN_TIMEOUT)) {
            int n = Integer.parseInt(conf.get(CONF_CONN_TIMEOUT));
            cc = cc.withConnectionTimeout(n);
        }
        if (conf.containsKey(CONF_MAX_CONN)) {
            int n = Integer.parseInt(conf.get(CONF_MAX_CONN));
            cc = cc.withMaxConnections(n);
        }
        if (conf.containsKey(CONF_TCP_KEEP_ALIVE)) {
            boolean b = Boolean.parseBoolean(conf.get(CONF_TCP_KEEP_ALIVE));
            cc = cc.withTcpKeepAlive(b);
        }
        if (conf.containsKey(CONF_SOCKET_TIMEOUT)) {
            int n = Integer.parseInt(conf.get(CONF_SOCKET_TIMEOUT));
            cc = cc.withSocketTimeout(n);
        }

        s3 = new AmazonS3Client(cred, cc);
    }

    @Override
    protected void doRemove(String fullPath) {
        s3.deleteObject(new DeleteObjectRequest(bucket, fullPath));
    }

    @Override
    protected ISObject newSObject(String key) {
        return new S3Obj(key, this);
    }

    @Override
    protected Map<String, String> doGetMeta(String fullPath) {
        GetObjectMetadataRequest req = new GetObjectMetadataRequest(bucket, fullPath);
        ObjectMetadata meta = s3.getObjectMetadata(req);
        return meta.getUserMetadata();
    }

    @Override
    protected InputStream doGetInputStream(String fullPath) {
        GetObjectRequest req = new GetObjectRequest(bucket, fullPath);
        S3Object s3obj = s3.getObject(req);
        return s3obj.getObjectContent();
    }

    @Override
    protected void doPut(String fullPath, ISObject stuff, Map<String, String> attrs) {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType(stuff.getAttribute(ISObject.ATTR_CONTENT_TYPE));
        if (!(stuff instanceof SObject.InputStreamSObject)) {
            meta.setContentLength(stuff.getLength());
        }
        meta.setUserMetadata(attrs);

        PutObjectRequest req = new PutObjectRequest(bucket, fullPath, stuff.asInputStream(), meta);
        StorageClass storageClass = StorageClass.valueOfIgnoreCase(attrs.remove(ATTR_STORAGE_CLASS), defStorageClass);
        if (null != storageClass) {
            req.setStorageClass(storageClass.toString());
        }
        req.withCannedAcl(CannedAccessControlList.PublicRead);
        s3.putObject(req);
    }

    @Override
    protected StorageServiceBase newService(Map conf) {
        return new S3Service(conf);
    }
}
