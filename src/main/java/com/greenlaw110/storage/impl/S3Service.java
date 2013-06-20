package com.greenlaw110.storage.impl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.greenlaw110.storage.ISObject;
import com.greenlaw110.storage.IStorageService;
import com.greenlaw110.storage.KeyGenerator;
import com.greenlaw110.util.E;
import com.greenlaw110.util.S;

import java.util.Map;

/**
 * Implement {@link IStorageService} on Amazon S3
 */
public class S3Service extends StorageServiceBase implements IStorageService {

    public static enum StorageClass {
        STANDARD, REDUCED_REDUNDANCY;

        public static StorageClass valueOfIgnoreCase(String s) {
            s = s.trim();
            if ("rrs".equalsIgnoreCase(s) || "rr".equalsIgnoreCase(s) || "reduced_redundancy".equalsIgnoreCase(s) || "reducedRedundancy".equalsIgnoreCase(s)) {
                return REDUCED_REDUNDANCY;
            } else {
                return STANDARD;
            }
        }
    }

    public static final String CONF_KEY_ID = "storage.s3.keyId";
    public static final String CONF_KEY_SECRET = "storage.s3.keySecret";
    public static final String CONF_DEF_STORAGE_CLASS = "storage.s3.defStorageClass";
    public static final String CONF_BUCKET = "storage.s3.bucket";
    public static final String CONF_STATIC_WEB_ENDPOINT = "storage.s3.staticWebEndpoint";

    /**
     * For certain case for example, gallery application, it doesn't need the GET operation because end 
     * user can access the object directly from aws' static web service 
     */
    public static final String CONF_NO_GET = "storage.s3.noGet";

    public static final String ATTR_STORAGE_CLASS = "storage-class";


    private String awsKeyId;
    private String awsKeySecret;
    private StorageClass defStorageClass = StorageClass.REDUCED_REDUNDANCY;
    private String bucket;
    private String staticWebEndPoint = null;
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
            defStorageClass = StorageClass.valueOfIgnoreCase(sc);
        }
        bucket = conf.get(CONF_BUCKET);
        if (null == bucket) {
            E.invalidConfiguration("AWS bucket not found in the configuration");
        }

        staticWebEndPoint = conf.get(CONF_STATIC_WEB_ENDPOINT);
        System.setProperty("line.separator", "\n");
        AWSCredentials cred = new BasicAWSCredentials(awsKeyId, awsKeySecret);
        s3 = new AmazonS3Client(cred);

        if (conf.containsKey(CONF_NO_GET)) {
            noGet = Boolean.parseBoolean(conf.get(CONF_NO_GET));
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
        if (noGet || S.empty(staticWebEndPoint)) {
            return SObject.getDumpObject(key);
        } else {
            try {
                GetObjectRequest req = new GetObjectRequest(bucket, key);
                S3Object s3obj = s3.getObject(req);
                ISObject sobj = SObject.valueOf(key, s3obj.getObjectContent());
                ObjectMetadata meta = s3obj.getObjectMetadata();
                Map<String, String> map = meta.getUserMetadata();
                for (String k : map.keySet()) {
                    sobj.setAttribute(k, map.get(k));
                }
                return sobj;
            } catch (AmazonS3Exception e) {
                return SObject.getInvalidObject(key, e);
            }
        }
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
