package org.osgl.storage.impl;

class S3Obj extends StorageObject<S3Obj, S3Service> {
    S3Obj(String key, S3Service svc) {
        super(key, svc);
    }
}
