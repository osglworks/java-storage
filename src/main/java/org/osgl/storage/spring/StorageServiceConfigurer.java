package org.osgl.storage.spring;

import org.osgl.storage.IStorageService;

/**
 * Help spring app to configure the {@link org.osgl.storage.IStorageService}
 */
public interface StorageServiceConfigurer {
    IStorageService getStorageService();
}
