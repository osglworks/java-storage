package org.osgl.storage.impl;

import org.osgl.exception.UnexpectedIOException;
import org.osgl.storage.ISObject;
import org.osgl.util.IO;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * Created by luog on 28/05/2014.
 */
class S3Obj extends SObject {
    private transient S3Service s3;
    private byte[] buf_ = null;

    S3Obj(String key, S3Service s3) {
        super(key);
        this.s3 = s3;
        setAttributes(s3.getMeta(key));
    }

    @Override
    public long getLength() {
        return 0;
    }

    private synchronized void flush() {
        if (null != buf_) return;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        InputStream is = s3.getInputStream(getKey());
        IO.copy(is, baos);
        IO.close(is);
        buf_ = baos.toByteArray();
    }

    private ISObject buf() {
        flush();
        return SObject.of(buf_);
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
