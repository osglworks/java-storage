package org.osgl.storage.impl;

import org.osgl.exception.UnexpectedIOException;
import org.osgl.storage.ISObject;
import org.osgl.util.IO;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.nio.charset.Charset;

class S3Obj extends SObject {
    private transient S3Service s3;
    private SoftReference<byte[]> cache;

    S3Obj(String key, S3Service s3) {
        super(key);
        this.s3 = s3;
        setAttributes(s3.getMeta(key));
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
        InputStream is = s3.getInputStream(getKey());
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
        return s3.getInputStream(getKey());
    }
}
