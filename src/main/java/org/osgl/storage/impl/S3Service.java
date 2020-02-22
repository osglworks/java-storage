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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.osgl.exception.AccessDeniedException;
import org.osgl.exception.ResourceNotFoundException;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.util.E;
import org.osgl.util.S;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implement {@link org.osgl.storage.IStorageService} on Amazon S3
 */
public class S3Service extends StorageServiceBase<S3Obj> implements IStorageService {

    public enum StorageClass {
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
    private StorageClass defStorageClass;
    private String bucket;

    public static AmazonS3 s3;

    public S3Service(Map<String, String> conf) {
        super(conf, S3Obj.class);
    }

    @Override
    protected void configure(Map<String, String> conf) {
        super.configure(conf, "s3");
        awsKeyId = conf.get(CONF_KEY_ID);
        awsKeySecret = conf.get(CONF_KEY_SECRET);
        if (null == awsKeySecret || null == awsKeyId) {
            E.invalidConfiguration("AWS Key ID or AWS Key Secret not found in the configuration");
        }
        String sc = conf.get(CONF_DEF_STORAGE_CLASS);
        if (null != sc) {
            defStorageClass = StorageClass.valueOfIgnoreCase(sc, defStorageClass);
        } else {
            defStorageClass = StorageClass.REDUCED_REDUNDANCY;
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
        try {
            s3.deleteObject(new DeleteObjectRequest(bucket, fullPath));
        } catch (AmazonS3Exception e) {
            throw handleException(fullPath, e);
        }
    }

    @Override
    protected ISObject newSObject(String key) {
        try {
            return new S3Obj(key, this);
        } catch (AmazonS3Exception e) {
            throw handleException(key, e);
        }
    }

    @Override
    protected Map<String, String> doGetMeta(String fullPath) {
        GetObjectTaggingRequest req0 = new GetObjectTaggingRequest(bucket, fullPath);
        GetObjectTaggingResult resp = s3.getObjectTagging(req0);
        return tagListToMap(resp.getTagSet());
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
        //meta.setContentType(stuff.getAttribute(ISObject.ATTR_CONTENT_TYPE));
        meta.setUserMetadata(attrs);
        if (!(stuff instanceof SObject.InputStreamSObject)) {
            long length = stuff.getLength();
            if (0 < length) {
                meta.setContentLength(stuff.getLength());
            }
        }

        PutObjectRequest req = new PutObjectRequest(bucket, fullPath, stuff.asInputStream(), meta);
        req.setTagging(mapToTagList(attrs));
        StorageClass storageClass = StorageClass.valueOfIgnoreCase(attrs.remove(ATTR_STORAGE_CLASS), defStorageClass);
        if (null != storageClass) {
            req.setStorageClass(storageClass.toString());
        }
        req.withCannedAcl(CannedAccessControlList.PublicRead);
        try {
            s3.putObject(req);
        } catch (AmazonS3Exception e) {
            throw handleException(fullPath, e);
        }
    }

    private static ObjectTagging mapToTagList(Map<String, String> map) {
        List<Tag> list = new ArrayList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            list.add(new Tag(entry.getKey(), entry.getValue()));
        }
        return new ObjectTagging(list);
    }

    private static Map<String, String> tagListToMap(List<Tag> tagging) {
        Map<String, String> map = new HashMap<>();
        for (Tag tag : tagging) {
            map.put(tag.getKey(), tag.getValue());
        }
        return map;
    }

    private static AmazonS3Exception handleException(String key, AmazonS3Exception e) {
        int status = e.getStatusCode();
        switch (status) {
            case 404:
                throw new ResourceNotFoundException(e, key);
            case 403:
                throw new AccessDeniedException(e, key);
            default:
                throw e;
        }
    }

    @Override
    protected StorageServiceBase newService(Map conf) {
        return new S3Service(conf);
    }

}
