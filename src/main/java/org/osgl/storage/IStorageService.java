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

import org.osgl.exception.UnexpectedIOException;
import org.osgl.util.F;

import java.util.Map;


/**
 * Define A storage service
 */
public interface IStorageService {

    public static final String CONF_KEY_GEN = "storage.keygen";

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
     * Force retrieving the stuff from storage without regarding to the configuratoin
     * 
     * @param key
     * @return the storage stuff
     */
    ISObject forceGet(String key);
    
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
    
    String getKey(String key);
    
    public String getKey();
    
    public static class f {
        public static F.F0<Void> put(final String key, final ISObject stuff, final IStorageService ss) {
            return put().curry(key, stuff, ss);
        }

        public static F.F3<Void, String, ISObject, IStorageService> put() {
            return new F.F3<Void, String, ISObject, IStorageService>() {
                @Override
                public Void run(String s, ISObject isObject, IStorageService iStorageService) {
                    iStorageService.put(s, isObject);
                    return null;
                }
            };
        }

        public static F.F0<ISObject> get(final String key, IStorageService ss) {
            return get().curry(key, ss);
        }
        
        public static F.F2<ISObject, String, IStorageService> get() {
            return new F.F2<ISObject, String, IStorageService>() {
                @Override
                public ISObject run(String key, IStorageService ss) {
                    return ss.get(key);
                }
            };
        }

        public static F.F0<Void> remove(final String key, IStorageService ss) {
            return remove().curry(key, ss);
        }
        
        public static F.F2<Void, String, IStorageService> remove() {
            return new F.F2<Void, String, IStorageService>() {
                @Override
                public Void run(String s, IStorageService ss) {
                    ss.remove(s);
                    return null;
                }
            };
        }
    }
}
