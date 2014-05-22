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
package org.osgl.storage;

import org.osgl._;
import org.osgl.exception.UnexpectedIOException;

import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Represent an item stored in an <code>IStorageService</code> 
 * 
 * @author greenl
 */
public interface ISObject extends Serializable {

    /**
     * A standard attribute: content-type
     */
    public static final String ATTR_CONTENT_TYPE = "content-type";

    /**
     * A standard attribute: filename
     */
    public static final String ATTR_FILE_NAME = "filename";

    /**
     * @return key of this object
     */
    String getKey();

    /**
     * @return length of the object
     */
    long getLength();
    
    /** 
     * Return attribute associated with this storage object by key. If there is 
     * no such attribute found then <code>null</code> is returned
     * 
     * @return the attribute if found or <code>null</code> if not found
     */
    String getAttribute(String key);

    /**
     * Set an attribute to the storage object associated by key specified. 
     * @param key
     * @param val
     */
    ISObject setAttribute(String key, String val);

    /**
     * Set attributes to the storage object
     * @param attrs
     * @return the object with attributes set
     */
    ISObject setAttributes(Map<String, String> attrs);

    /**
     * @return <code>true</code> if the storage object has attributes
     */
    boolean hasAttribute();

    /**
     * @return a copy of attributes of this storage object
     */
    Map<String, String> getAttributes();

    /**
     * Is content is empty 
     * 
     * @return
     */
    public boolean isEmpty();

    /**
     * Is this storage object valid. A storage object is not valid
     * if the file/input stream is not readable
     * 
     * @return
     */
    public boolean isValid();

    /**
     * Return previous exception that cause the sobject invalid 
     * 
     * @return
     */
    public Throwable getException();

   /**
    * @return the the stuff content as an file
    */
   File asFile() throws UnexpectedIOException;
   /**
    * @return the stuff content as a string
    */
   String asString() throws UnexpectedIOException;

    /**
     * @return the stuff content as a string using the charset to encode
     */
   String asString(Charset charset) throws UnexpectedIOException;
   /**
    * @return the stuff content as a byte array
    */
   byte[] asByteArray() throws UnexpectedIOException;
   /**
    * @return the stuff content as an input stream
    */
   InputStream asInputStream() throws UnexpectedIOException;

    /**
     * Consume the inputstream of this storage object one time and then close the input stream
     * @param consumer the consumer function
     */
   void consumeOnce(_.Function<InputStream, ?> consumer) throws UnexpectedIOException;

}
