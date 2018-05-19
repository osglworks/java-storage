package org.osgl.storage.spring;

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

import static org.osgl.storage.impl.S3Service.*;

import org.osgl.storage.IStorageService;
import org.osgl.storage.KeyGenerator;
import org.osgl.storage.impl.S3Service;
import org.osgl.util.C;
import org.osgl.util.E;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class S3ServiceConfigurer extends StorageServiceConfigurerBase implements StorageServiceConfigurer {

    private String awsKeyId;
    private String awsKeySecret;
    private S3Service.StorageClass defStorageClass;
    private String bucket;
    private String staticWebEndPoint = null;
    private boolean getMetaOnly = false;
    private boolean noGet = false;

    public void setAwsKeyId(String awsKeyId) {
        E.NPE(awsKeyId);
        this.awsKeyId = awsKeyId;
    }

    public void setAwsKeySecret(String awsKeySecret) {
        E.NPE(awsKeySecret);
        this.awsKeySecret = awsKeySecret;
    }

    public void setDefStorageClass(S3Service.StorageClass defStorageClass) {
        E.NPE(defStorageClass);
        this.defStorageClass = defStorageClass;
    }

    public void setBucket(String bucket) {
        E.NPE(bucket);
        this.bucket = bucket;
    }

    public void setStaticWebEndPoint(String staticWebEndPoint) {
        E.NPE(staticWebEndPoint);
        this.staticWebEndPoint = staticWebEndPoint;
    }

    public void setGetMetaOnly(boolean getMetaOnly) {
        this.getMetaOnly = getMetaOnly;
    }

    public void setNoGet(boolean noGet) {
        this.noGet = noGet;
    }

    @Override
    public IStorageService getStorageService() {
        Map<String, String> conf = C.Map(CONF_KEY_ID, awsKeyId,
                CONF_KEY_SECRET, awsKeySecret,
                CONF_DEF_STORAGE_CLASS, defStorageClass,
                CONF_BUCKET, bucket,
                CONF_GET_META_ONLY, getMetaOnly,
                CONF_GET_NO_GET, noGet);
        S3Service ss = new S3Service(conf);
        KeyGenerator keyGenerator = getKeyGenerator();
        if (null != keyGenerator) {
            ss.setKeyGenerator(keyGenerator);
        }
        return ss;
    }
}
