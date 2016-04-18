package org.osgl.storage.spring;

import org.osgl.storage.IStorageService;
import org.osgl.storage.impl.FileSystemService;
import org.osgl.util.C;
import org.osgl.util.E;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

import static org.osgl.storage.IStorageService.CONF_KEY_GEN;

/**
 * Created by luog on 3/01/14.
 */
@Component
public class FileSystemServiceConfigurer extends StorageServiceConfigurerBase implements StorageServiceConfigurer {

    private String homeDir;
    private String homeUrl;

    public void setHomeDir(Resource homeDir) throws IOException {
        E.NPE(homeDir);
        this.homeDir = homeDir.getFile().getAbsolutePath();
    }

    public void setHomeUrl(String homeUrl) {
        E.NPE(homeUrl);
        this.homeUrl = homeUrl;
    }

    @Override
    public IStorageService getStorageService() {
        Map<String, String> conf = C.map(
                FileSystemService.CONF_HOME_DIR, homeDir,
                FileSystemService.CONF_HOME_URL, homeUrl,
                CONF_KEY_GEN, getKeyGenerator().name());
        return new FileSystemService(conf);
    }
}
