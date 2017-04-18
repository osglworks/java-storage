package org.osgl.storage.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.osgl.exception.UnexpectedIOException;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.storage.TestBase;
import org.osgl.util.C;

import static org.osgl.storage.impl.FileSystemService.CONF_HOME_DIR;
import static org.osgl.storage.impl.FileSystemService.CONF_HOME_URL;

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

    @Test(expected = UnexpectedIOException.class)
    public void testSubFolderRemove() {
        ISObject obj = fss.put(subFolderPath + "/" + key1, sobj);
        subFolder.remove(key1);
        obj = fss.get(subFolderPath + "/" + key1);
        assertNull(obj.getAttribute(ISObject.ATTR_FILE_NAME));
        obj.asByteArray();
    }

}
