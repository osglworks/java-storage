package org.osgl.storage;

import java.util.UUID;

/**
 * Responsible for providing unique ID as the key name
 */
public interface KeyNameProvider {

    /**
     * Generate key name using random UUID
     */
    KeyNameProvider DEF_PROVIDER = new KeyNameProvider() {
        @Override
        public String newKeyName() {
            return UUID.randomUUID().toString();
        }
    };

    /**
     * Returns a unique key name. Note this method shall not
     * return the hierarchical structure which is the responsibility
     * of the {@link KeyGenerator}
     *
     * @return the key name
     */
    String newKeyName();

}
