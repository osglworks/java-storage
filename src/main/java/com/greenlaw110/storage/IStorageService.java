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
package com.greenlaw110.storage;

import com.greenlaw110.exception.UnexpectedIOException;

import java.util.Map;


/**
 * Define A storage service
 */
public interface IStorageService {

    /**
     * Configure the service
     * 
     * @param conf
     */
    void configure(Map<String, String> conf);
    
    /**
     * Retrieve the stuff from the storage by key
     * 
     * If file cannot be find by key, then <code>null</code> is returned
     * 
     * @param key
     * @return the file associated with key or null if not found
     */
    ISObject get(String key);
    /**
     * Update the stuff in the storage. If the existing file cannot be find
     * in the storage then it will be added.
     * 
     * @param key
     * @param stuff
     */
    void put(String key, ISObject stuff) throws UnexpectedIOException;
    
    /**
     * Remove the file from the storage by key and return it to caller.
     * 
     * @param key
     */
    void remove(String key);

    /**
     * Return the URL to access a stored resource by key 
     * 
     * @param key
     * @return the URL
     */
    String getUrl(String key);
}
