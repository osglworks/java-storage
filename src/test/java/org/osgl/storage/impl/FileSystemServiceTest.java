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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.storage.TestBase;
import org.osgl.util.C;

import static org.osgl.storage.impl.FileSystemService.CONF_HOME_DIR;
import static org.osgl.storage.impl.FileSystemService.CONF_HOME_URL;

@Ignore
public class FileSystemServiceTest extends TestBase {

    private FileSystemService fss;
    private IStorageService subFolder;
    private String key1 = "test.txt";
    private String subFolderPath = "csb";
    private ISObject sobj;

    @Before
    public void setUp() {
        sobj = SObject.of("hello world");
        C.Map<String, String> conf = C.newMap(CONF_HOME_DIR, "tmp", CONF_HOME_URL, "/uploads");
        fss = new FileSystemService(conf);
        subFolder = fss.subFolder(subFolderPath);
    }

    @After
    public void tearDown() {
        fss.remove(key1);
    }

    @Test
    public void testPutAndGet() {
        fss.put(key1, sobj);
        ISObject loaded = fss.get(key1);
        eq(sobj.asString(), loaded.asString());
    }

    @Test
    public void testSubFolderPut() {
        subFolder.put(key1, sobj);
        ISObject loaded = fss.get(subFolderPath + "/" + key1);
        eq(sobj.asString(), loaded.asString());
    }

    @Test
    public void testSubFolderGet() {
        fss.put(subFolderPath + "/" + key1, sobj);
        ISObject loaded = subFolder.get(key1);
        eq(sobj.asString(), loaded.asString());
    }

    @Test
    public void testSubFolderRemove() {
        fss.put(subFolderPath + "/" + key1, sobj);
        subFolder.remove(key1);
        ISObject obj = fss.get(subFolderPath + "/" + key1);
        assertFalse(obj.isValid());
    }

    @Test
    public void testLoadResource() {
        ISObject obj = SObject.loadResource("/test.txt");
        eq("test.txt", obj.getAttribute(ISObject.ATTR_FILE_NAME));
    }

}
