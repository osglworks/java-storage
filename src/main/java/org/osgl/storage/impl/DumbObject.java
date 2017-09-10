package org.osgl.storage.impl;

/*-
 * #%L
 * Java Storage Service
 * %%
 * Copyright (C) 2013 - 2017 OSGL (Open Source General Library)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
