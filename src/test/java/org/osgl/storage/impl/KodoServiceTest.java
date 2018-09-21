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

import com.alibaba.fastjson.JSON;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.osgl.logging.L;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.storage.TestBase;
import org.osgl.util.C;

import java.util.Map;

@Ignore
public class KodoServiceTest extends TestBase {

    private static final String AK = "AccessKey";
    private static final String SK = "SecretKey";
    private static final String DOMAIN = "bucketDomain";
    private KodoService kds;
    private IStorageService subFolder;
    private String key1 = "test123.txt";
    private String subFolderPath = "csb";
    private ISObject sobj;

    @Before
    public void setUp() {
        sobj = SObject.of("hello world");
        C.Map<String, String> conf = C.newMap(
                KodoService.CONF_BUCKET, "test",
                KodoService.CONF_ACCESS_KEY, AK,
                KodoService.CONF_SECRET_KEY, SK,
                KodoService.CONF_DOMAIN, DOMAIN,
                KodoService.CONF_PERMISSION, KodoService.BUCKET_PUB);
        kds = new KodoService(conf);
        subFolder = kds.subFolder(subFolderPath);
    }

//    @After
//    public void tearDown() {
//        kds.remove(key1);
//    }

    @Test
    public void testPutAndGet() {
        kds.put(key1, sobj);
        ISObject loaded = kds.get(key1);
        eq(sobj.asString(), loaded.asString());
    }

    @Test
    public void testSubFolderPut() {
        subFolder.put(key1, sobj);
        ISObject loaded = kds.get(subFolderPath + "/" + key1);
        eq(sobj.asString(), loaded.asString());
    }

    @Test
    public void testSubFolderGet() {
        kds.put(subFolderPath + "/" + key1, sobj);
        ISObject loaded = subFolder.get(key1);
        subFolder.remove(key1);
        eq(sobj.asString(), loaded.asString());
    }

    @Test
    public void testSubFolderRemove() {
        kds.put(subFolderPath + "/" + key1, sobj);
        subFolder.remove(key1);
//        ISObject obj = kds.get(subFolderPath + "/" + key1);
//        assertFalse(obj.isValid());
    }

    @Test
    public void testDoGetMeta() {
        kds.put(subFolderPath + "/" + key1, sobj);
        Map<String, String> stringStringMap = kds.doGetMeta(subFolderPath + "/" + key1);

        L.info(JSON.toJSONString(stringStringMap));
    }

}
