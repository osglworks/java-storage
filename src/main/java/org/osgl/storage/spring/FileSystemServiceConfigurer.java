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

import org.osgl.storage.IStorageService;
import org.osgl.storage.KeyGenerator;
import org.osgl.storage.impl.FileSystemService;
import org.osgl.util.C;
import org.osgl.util.E;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * A file system based {@link IStorageService} implementation
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
        Map<String, String> conf = C.Map(
                FileSystemService.CONF_HOME_DIR, homeDir,
                FileSystemService.CONF_HOME_URL, homeUrl);
        FileSystemService ss = new FileSystemService(conf);
        KeyGenerator keyGenerator = getKeyGenerator();
        if (null != keyGenerator) {
            ss.setKeyGenerator(keyGenerator);
        }
        return ss;
    }
}
