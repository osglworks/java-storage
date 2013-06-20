package com.greenlaw110.storage.impl;

import com.greenlaw110.storage.IStorageService;
import com.greenlaw110.storage.KeyGenerator;
import com.greenlaw110.util._;

import java.util.Map;

/**
 * The implementation base of {@link IStorageService} 
 */
public abstract class StorageServiceBase implements IStorageService {
    protected KeyGenerator keygen;
    protected StorageServiceBase(){
        keygen = KeyGenerator.BY_DATE;
    }
    protected StorageServiceBase(KeyGenerator keygen) {
        _.NPE(keygen);
        this.keygen = keygen;
    }

    @Override
    public void configure(Map<String, String> conf) {
        if (conf.containsKey(CONF_KEY_GEN)) {
            String s = conf.get(CONF_KEY_GEN);
            keygen = KeyGenerator.valueOfIgnoreCase(s);
        }
    }

    @Override
    public String getKey(String key) {
        return keygen.getKey(key);
    }

    @Override
    public String getKey() {
        return keygen.getKey();
    }
}
