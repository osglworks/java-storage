package org.osgl.storage.impl;

import org.osgl.exception.UnexpectedIOException;

import java.io.File;

class FileObject extends StorageObject<FileObject, FileSystemService> {
    FileObject(String key, FileSystemService fileSystemService) {
        super(key, fileSystemService);
    }

    @Override
    public File asFile() throws UnexpectedIOException {
        return svc.doGetFile(svc.keyWithContextPath(getKey()));
    }
}
