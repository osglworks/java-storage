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
package com.greenlaw110.storage.impl;

import com.greenlaw110.storage.ISObject;
import com.greenlaw110.storage.IStorageService;
import com.greenlaw110.util.E;
import com.greenlaw110.util.IO;
import com.greenlaw110.util.S;
import com.greenlaw110.util._;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * The implementation of {@link ISObject}
 */
public abstract class SObject implements ISObject {
    private String key;
    private Map<String, String> attrs = new HashMap<String, String>();
    private SObject(String key) {
        if (null == key) {
            throw new NullPointerException();
        }
        this.key = key;
    }
    private IStorageService service;

    public static ISObject getDumpObject(String key) {
        return asSObject(key, "");
    }
    
    public String getKey() {
        return key;
    }

    @Override
    public String getAttribute(String key) {
        return attrs.get(key);
    }

    @Override
    public void setAttribute(String key, String val) {
        attrs.put(key, val);
    }

    @Override
    public boolean hasAttribute() {
        return !attrs.isEmpty();
    }

    @Override
    public Map<String, String> getAttributes() {
        return new HashMap<String, String>(attrs);
    }

    @Override
    public void save() {
        service.put(key, this);
    }

    public String getUrl() {
        return service.getUrl(getKey());
    }

    public static ISObject asSObject(String key, File f) {
        return new FileSObject(key, f);
    }

    public static ISObject asSObject(String key, InputStream is) {
        return new InputStreamSObject(key, is);
    }

    public static ISObject asSObject(String key, String s) {
        return new StringSObject(key, s);
    }

    public static ISObject asSObject(String key, byte[] buf) {
        return new ByteArraySObject(key, buf);
    }
    
    private static File createTempFile() {
        try {
            return File.createTempFile("sobj_", ".tmp");
        } catch (IOException e) {
            throw E.ioException(e);
        }
    }

    private static class FileSObject extends SObject {
        private File f_ = null;

        FileSObject(String key, File f) {
            super(key);
            E.NPE(f);
            if (!f.exists() || !f.canRead() || f.isDirectory()) {
                E.unexpected("File object[%s] not readable or is a director", f.getPath());
            }
            f_ = f;
        }

        @Override
        public byte[] asByteArray() {
            return IO.readContent(f_);
        }

        @Override
        public File asFile() {
            return f_;
        }

        @Override
        public InputStream asInputStream() {
            return IO.is(f_);
        }

        @Override
        public String asString() {
            return IO.readContentAsString(f_);
        }

        @Override
        public long getLength() {
            return f_.length();
        }
    }

    private static class StringSObject extends SObject {
        private String s_ = null;

        StringSObject(String key, String s) {
            super(key);
            _.NPE(s);
            
            s_ = s;
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

    private static class InputStreamSObject extends SObject {
        private InputStream is_ = null;

        InputStreamSObject(String key, InputStream is) {
            super(key);
            _.NPE(is);
            is_ = is;
        }

        @Override
        public byte[] asByteArray() {
            return IO.readContent(is_);
        }

        @Override
        public File asFile() {
            File tmpFile = createTempFile();
            IO.write(is_, tmpFile);
            return tmpFile;
        }

        @Override
        public InputStream asInputStream() {
            return is_;
        }

        @Override
        public String asString() {
            return IO.readContentAsString(is_);
        }

        @Override
        public long getLength() {
            return asByteArray().length;
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
            return S.str(buf_);
        }

        @Override
        public long getLength() {
            return buf_.length;
        }
    }
    
}
