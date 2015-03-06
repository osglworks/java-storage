package org.osgl.storage.impl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.storage.KeyGenerator;
import org.osgl.util.C;
import org.osgl.util.E;
import org.osgl.util.S;

import java.io.InputStream;
import java.util.Map;

/**
 * Implement {@link org.osgl.storage.IStorageService} on Amazon S3
 */
public class S3Service extends StorageServiceBase implements IStorageService {

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
    public static final String CONF_STATIC_WEB_ENDPOINT = "storage.s3.staticWebEndpoint";

    /**
     * Get Meta Data only 
     */
    public static final String CONF_GET_META_ONLY = "storage.s3.get.MetaOnly";

    /**
     * For certain case for example, gallery application, it doesn't need the GET operation because end 
     * user can access the object directly from aws' static web service 
     */
    public static final String CONF_GET_NO_GET = "storage.s3.get.noGet";

    public static final String ATTR_STORAGE_CLASS = "x-amz-storage-class";


    private String awsKeyId;
    private String awsKeySecret;
    private StorageClass defStorageClass = StorageClass.REDUCED_REDUNDANCY;
    private String bucket;
    private String staticWebEndPoint = null;
    private boolean loadMetaOnly = false;
    private boolean noGet = false;
    
    
    public static AmazonS3 s3;
    
    public S3Service(KeyGenerator keygen) {
        super(keygen); 
    }

    public S3Service(Map<String, String> conf) {
        super.configure(conf);
    }

    @Override
    public void configure(Map<String, String> conf) {
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

        staticWebEndPoint = conf.get(CONF_STATIC_WEB_ENDPOINT);
        System.setProperty("line.separator", "\n");
        AWSCredentials cred = new BasicAWSCredentials(awsKeyId, awsKeySecret);
        s3 = new AmazonS3Client(cred);

        if (conf.containsKey(CONF_GET_META_ONLY)) {
            loadMetaOnly = Boolean.parseBoolean(conf.get(CONF_GET_META_ONLY));
        }

        if (conf.containsKey(CONF_GET_NO_GET)) {
            noGet = Boolean.parseBoolean(conf.get(CONF_GET_NO_GET));
        }
    }

//    String authStr(String stringToSign) {
//        byte[] key = awsKeySecret.getBytes();
//        String ss = Crypto.sign(stringToSign, key);
//        ss = Codec.encodeBASE64(ss);
//        return "AWS " + awsKeyId + ":" + ss;
//    }

    @Override
    public ISObject get(String key) {
        if (noGet) {
            return SObject.getDumpObject(key);
        }
        return new S3Obj(key, this);
    }

    Map<String, String> getMeta(String key) {
        if (noGet) return C.map();
        GetObjectMetadataRequest req = new GetObjectMetadataRequest(bucket, key);
        ObjectMetadata meta = s3.getObjectMetadata(req);
        return meta.getUserMetadata();
    }

    @Override
    public ISObject getFull(String key) {
        return new S3Obj(key, this);
    }

    @Override
    public ISObject loadContent(ISObject sobj0) {
        String key = sobj0.getKey();
        return new S3Obj(key, this);
    }

    InputStream getInputStream(String key) {
        GetObjectRequest req = new GetObjectRequest(bucket, key);
        S3Object s3obj = s3.getObject(req);
        return s3obj.getObjectContent();
    }

    @Override
    public void put(String key, ISObject stuff) {
        ObjectMetadata meta = new ObjectMetadata();
        Map<String, String> attrs = stuff.getAttributes();
        meta.setContentType(stuff.getAttribute(ISObject.ATTR_CONTENT_TYPE));
        meta.setContentLength(stuff.getLength());
        attrs.remove(ISObject.ATTR_CONTENT_TYPE);
        meta.setUserMetadata(attrs); 
        
        PutObjectRequest req = new PutObjectRequest(bucket, key, stuff.asInputStream(), meta);
        StorageClass storageClass = StorageClass.valueOfIgnoreCase(attrs.remove(ATTR_STORAGE_CLASS), defStorageClass);
        if (null != storageClass) {
            req.setStorageClass(storageClass.toString());
        }
        req.withCannedAcl(CannedAccessControlList.PublicRead);
        s3.putObject(req);
    }

    @Override
    public void remove(String key) {
        s3.deleteObject(new DeleteObjectRequest(bucket, key));
//        S3Action act = S3Action.delete(key, this);
//        WS.HttpResponse resp = act.doIt();
    }

    @Override
    public String getUrl(String key) {
        if (null == staticWebEndPoint) {
            return null;
        }
        return "//" + staticWebEndPoint + "/" + key;
    }
}
