package org.osgl.storage.impl;

class FileObject extends StorageObject<FileObject, FileSystemService> {
    FileObject(String key, FileSystemService fileSystemService) {
        super(key, fileSystemService);
    }
}
