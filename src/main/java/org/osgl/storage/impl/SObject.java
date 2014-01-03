/* 
 * Copyright (C) 2013 The Java Storage project
 * Gelin Luo <greenlaw110(at)gmail.com>
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.osgl.storage.impl;

import org.osgl.storage.ISObject;
import org.osgl.util.C;
import org.osgl.util.E;
import org.osgl.util.IO;
import org.osgl.util.S;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * The implementation of {@link ISObject}
 */
public abstract class SObject implements ISObject {
    private String key;
    private Map<String, String> attrs = new HashMap<String, String>();
    private boolean valid = true;
    private Throwable cause = null;
    private SObject(String key) {
        if (null == key) {
            throw new NullPointerException();
        }
        this.key = key;
    }

    public static SObject getInvalidObject(String key, Throwable cause) {
        SObject sobj = valueOf(key, "");
        sobj.valid = false;
        sobj.cause = cause;
        return sobj;
    }

    public static ISObject getDumpObject(String key) {
        return valueOf(key, "");
    }
    
    public String getKey() {
        return key;
    }

    @Override
    public String getAttribute(String key) {
        return attrs.get(key);
    }

    @Override
    public ISObject setAttribute(String key, String val) {
        attrs.put(key, val);
        return this;
    }

    @Override
    public boolean hasAttribute() {
        return !attrs.isEmpty();
    }

    @Override
    public Map<String, String> getAttributes() {
        return C.newMap(attrs);
    }

    @Override
    public boolean isEmpty() {
        String s = asString();
        return null == s || "".equals(s);
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public Throwable getException() {
        return cause;
    }

    public static SObject valueOf(String key, File f) {
        if (f.canRead() && f.isFile()) {
            SObject sobj = new ByteArraySObject(key, IO.readContent(f));
            sobj.setAttribute(ATTR_FILE_NAME, f.getName());
            sobj.setAttribute(ATTR_CONTENT_TYPE, new MimetypesFileTypeMap().getContentType(f));
            return sobj;
        } else {
            return getInvalidObject(key, new IOException("File is a directory or not readable"));
        }
    }

    public static SObject valueOf(String key, InputStream is) {
        try {
            return new ByteArraySObject(key, IO.readContent(is));
        } catch (Exception e) {
            return getInvalidObject(key, e);
        }
    }

    public static SObject valueOf(String key, String s) {
        return new StringSObject(key, s);
    }

    public static SObject valueOf(String key, byte[] buf) {
        return new ByteArraySObject(key, buf);
    }

    public static SObject valueOf(String key, ISObject copy) {
        SObject sobj = valueOf(key, copy.asByteArray());
        sobj.attrs.putAll(copy.getAttributes());
        return sobj;
    }
    
    private static File createTempFile() {
        try {
            return File.createTempFile("sobj_", ".tmp");
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    private static class StringSObject extends SObject {
        private String s_ = null;

        StringSObject(String key, String s) {
            super(key);
            s_ = null == s ? "" : s;
        }

        @Override
        public byte[] asByteArray() {
            return s_.getBytes();
        }

        @Override
        public File asFile() {
            File tmpFile = createTempFile();
            IO.writeContent(s_, tmpFile);
            return tmpFile;
        }

        @Override
        public InputStream asInputStream() {
            return IO.is(asByteArray());
        }

        @Override
        public String asString() {
            return s_;
        }

        @Override
        public long getLength() {
            return s_.length();
        }
    }

    private static class ByteArraySObject extends SObject {
        private byte[] buf_;

        ByteArraySObject(String key, byte[] buf) {
            super(key);
            buf_ = buf;
        }
        
        @Override
        public byte[] asByteArray() {
            return buf_;
        }

        @Override
        public File asFile() {
            File tmpFile = createTempFile();
            IO.write(buf_, tmpFile);
            return tmpFile;
        }

        @Override
        public InputStream asInputStream() {
            return IO.is(buf_);
        }

        @Override
        public String asString() {
            return S.string(buf_);
        }

        @Override
        public long getLength() {
            return buf_.length;
        }
    }
    
}
