package org.osgl.storage.impl;

class AzureObject extends StorageObject<AzureObject, AzureService> {
    AzureObject(String key, AzureService azureService) {
        super(key, azureService);
    }
}
