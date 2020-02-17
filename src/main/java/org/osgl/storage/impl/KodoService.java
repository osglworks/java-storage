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

import com.qiniu.common.QiniuException;
import com.qiniu.common.Zone;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.osgl.$;
import org.osgl.exception.AccessDeniedException;
import org.osgl.exception.ResourceNotFoundException;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.util.E;
import org.osgl.util.S;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

/**
 * Implement {@link org.osgl.storage.IStorageService} on 七牛云存储-kodo
 */
public class KodoService extends StorageServiceBase<KodoObject> implements IStorageService {

    public static final String CONF_ACCESS_KEY = "storage.kodo.access.key";
    public static final String CONF_SECRET_KEY = "storage.kodo.secret.key";
    public static final String CONF_BUCKET = "storage.kodo.bucket";
    public static final String CONF_PERMISSION = "storage.kodo.bucket.permission";
    public static final String CONF_DOMAIN = "storage.kodo.domain";

    public static final String BUCKET_PUB = "public";
    public static final String BUCKET_PRI = "private";

    private static OkHttpClient httpClient;
    private String bucket;
    private String permission;
    private String domain;

    private BucketManager bucketManager;
    private UploadManager uploadManager;
    private Auth auth;

    public KodoService(Map<String, String> conf) {
        super(conf, KodoObject.class);
    }

    @Override
    protected void configure(Map<String, String> conf) {
        super.configure(conf, "kodo");

        String accessKey = conf.get(CONF_ACCESS_KEY);
        String secretKey = conf.get(CONF_SECRET_KEY);
        if (S.isAnyEmpty(accessKey, secretKey)) {
            E.invalidConfiguration("Kodo accessKey or Kodo secretKey not found in the configuration");
        }

        bucket = conf.get(CONF_BUCKET);
        if (S.isEmpty(bucket)) {
            E.invalidConfiguration("Kodo bucket not found in the configuration");
        }

        domain = conf.get(CONF_DOMAIN);
        if (S.isEmpty(domain)) {
            E.invalidConfiguration("Kodo domain not found in the configuration");
        }

        permission = conf.get(CONF_PERMISSION);
        if (S.notBlank(permission) && !(S.eq(permission, BUCKET_PUB) || S.eq(permission, BUCKET_PRI))) {
            E.invalidConfiguration("The permission fields need 'public' or 'private'");
        }

        auth = Auth.create(accessKey, secretKey);
        Configuration c = new Configuration(Zone.autoZone());

        bucketManager = new BucketManager(auth, c);
        uploadManager = new UploadManager(c);
        httpClient = new OkHttpClient();
    }

    @Override
    protected void doRemove(String fullPath) {
        try {
            bucketManager.delete(bucket, fullPath);
        } catch (QiniuException e) {
            throw handleException(fullPath, e);
        }
    }

    @Override
    protected Map<String, String> doGetMeta(String fullPath) {
        try {
            FileInfo stat = bucketManager.stat(bucket, fullPath);
            return $.copy(stat).to(Map.class);
        } catch (QiniuException e) {
            throw handleException(fullPath, e);
        }
    }


    @Override
    protected InputStream doGetInputStream(String fullPath) {

        String baseUrl = S.msgFmt("http://{0}/{1}", domain, fullPath);

        if (isPrivate(permission)) {
            baseUrl = auth.privateDownloadUrl(baseUrl);
        }

        Request req = new Request.Builder().url(baseUrl).build();
        try {
            Response resp = httpClient.newCall(req).execute();
            if (resp.isSuccessful()) {
                return Objects.requireNonNull(resp.body()).byteStream();
            }
            switch (resp.code()) {
                case 404:
                    throw new ResourceNotFoundException(fullPath);
                case 403:
                    throw new AccessDeniedException(fullPath);
                default:
                    throw E.unexpected("Error accessing %s: %s", fullPath, resp.body().string());
            }
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    private boolean isPrivate(String permission) {
        return S.eq(BUCKET_PRI, permission);
    }

    private String getUploadToken() {
        return auth.uploadToken(bucket);
    }

    @Override
    protected void doPut(String fullPath, ISObject stuff, Map<String, String> attrs) {

        StringMap meta = new StringMap();
        for (String k : attrs.keySet()) {
            meta.putNotEmpty(k, attrs.get(k));
        }

        String contentType = stuff.getAttribute(ISObject.ATTR_CONTENT_TYPE);

        try {
            uploadManager.put(stuff.asInputStream(), fullPath, getUploadToken(), meta, contentType);
        } catch (QiniuException e) {
            throw handleException(fullPath, e);
        }
    }

    @Override
    protected ISObject newSObject(String key) {
        return new KodoObject(key, this);
    }

    @Override
    protected StorageServiceBase newService(Map<String, String> conf) {
        return new KodoService(conf);
    }

    private static RuntimeException handleException(String key, QiniuException e) {
        switch (e.code()) {
            case 404:
                throw new ResourceNotFoundException(e, key);
            case 403:
                throw new AccessDeniedException(e, key);
            default:
                throw E.unexpected(key);
        }
    }
}