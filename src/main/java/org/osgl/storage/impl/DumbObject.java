package org.osgl.storage.impl;

import org.osgl.exception.UnexpectedIOException;
import org.osgl.util.E;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * A DumbObject is a placeholder of ISObject that returned
 * by a StorageService when {@link StorageServiceBase#CONF_GET_NO_GET} is true
 */
class DumbObject extends SObject {

    DumbObject(String key) {
        super(key);
    }

    @Override
    public long getLength() {
        return -1;
    }

    @Override
    public File asFile() throws UnexpectedIOException {
        throw E.unsupport();
    }

    @Override
    public String asString() throws UnexpectedIOException {
        throw E.unsupport();
    }

    @Override
    public String asString(Charset charset) throws UnexpectedIOException {
        throw E.unsupport();
    }

    @Override
    public byte[] asByteArray() throws UnexpectedIOException {
        throw E.unsupport();
    }

    @Override
    public InputStream asInputStream() throws UnexpectedIOException {
        throw E.unsupport();
    }

    @Override
    public boolean isDumb() {
        return true;
    }
}
