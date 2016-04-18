package org.osgl.storage.spring;

import org.osgl.storage.KeyGenerator;
import org.osgl.util.E;

public abstract class StorageServiceConfigurerBase implements StorageServiceConfigurer {

    private KeyGenerator keyGenerator;

    protected KeyGenerator getKeyGenerator() {
        return keyGenerator;
    }

    public void setKeyGenerator(KeyGenerator keyGenerator) {
        E.NPE(keyGenerator);
        this.keyGenerator = keyGenerator;
    }

}
