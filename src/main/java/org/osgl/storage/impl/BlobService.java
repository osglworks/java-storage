package org.osgl.storage.impl;

import java.util.Map;

/**
 * This class is obsolete. Use {@link AzureService} instead
 */
@Deprecated
public class BlobService extends AzureService {
    @Deprecated
    public BlobService(Map<String, String> conf) {
        super(conf);
    }
}
