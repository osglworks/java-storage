package org.osgl.storage.impl;

import org.osgl.exception.UnexpectedIOException;
import org.osgl.storage.ISObject;
import org.osgl.util.IO;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.nio.charset.Charset;

public class BlobObject extends SObject {

    private transient BlobService blobService;
    private SoftReference<byte[]> cache;

    BlobObject(String key, BlobService blobService) {
        super(key);
        this.blobService = blobService;
        setAttributes(blobService.getMeta(key));
    }

    @Override
    public long getLength() {
        return 0;
    }

    private synchronized byte[] read() {
        if (null != cache) {
            byte[] ba = cache.get();
            if (null != ba) {
                return ba;
            }
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        InputStream is = blobService.getInputStream(getKey());
        IO.copy(is, baos);
        byte[] ba = baos.toByteArray();
        cache = new SoftReference<byte[]>(ba);
        return ba;
    }

    private ISObject buf() {
        return SObject.of(read());
    }

    @Override
    public File asFile() throws UnexpectedIOException {
        return buf().asFile();
    }

    @Override
    public String asString() throws UnexpectedIOException {
        return buf().asString();
    }

    @Override
    public String asString(Charset charset) throws UnexpectedIOException {
        return buf().asString(charset);
    }

    @Override
    public byte[] asByteArray() throws UnexpectedIOException {
        return buf().asByteArray();
    }

    @Override
    public InputStream asInputStream() throws UnexpectedIOException {
        return blobService.getInputStream(getKey());
    }
}
