package org.osgl.storage.impl;

import org.osgl.storage.ISObject;

class FileObject extends StorageObject<FileObject, FileSystemService> {

    FileObject(String key, FileSystemService fileSystemService) {
        super(key, fileSystemService);
        buf(); // eager load buf
    }

    @Override
    public long getLength() {
        return buf().getLength();
    }

    @Override
    protected ISObject loadBuf() {
        String fullPath = svc.keyWithContextPath(getKey());
        return SObject.of(fullPath, svc.getFile(fullPath));
    }
}
