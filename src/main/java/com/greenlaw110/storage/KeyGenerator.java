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

import java.util.Calendar;
import java.util.UUID;

/**
 * A helper class to generate resource key
 */
public class KeyGenerator {
    
    /**
     * Define the storage structure. There are 2 possible structure
     * 1. PLAIN, all files are saved directly in one folder
     * 2. BY_DATE, files are saved in a hierarchical structure like yyyy/MM/dd
     * 3. BY_DATETIME, files are saved in a hierarchical structure like yyyy/MM/dd/HH/mm/ss
     *
     * @author greenl
     */
    public static enum Structure {
        /**
         * All item stored in the bucket (root folder) without hierarchy
         */
        PLAIN,
        /**
         * Items stored in a hierarchy structured by date: /yyyy/MM/dd/item 
         */
        BY_DATE, 
        /**
         * Items stored in a hierarchy structured by date and time: /yyyy/MM/dd/HH/mm/ss/item 
         */
        BY_DATETIME
    }

    /**
     * Generate a unique key using {@link Structure#BY_DATE} structure
     * 
     * @return a unique
     */
    public static String newKey() {
        return newKey(null, Structure.BY_DATE);
    }

    /**
     * Generate a unique key using specified {@link Structure}
     * 
     * @param structure
     * @return a unique key
     */
    public static String newKey(Structure structure) {
        return newKey(null, structure);
    }

    /**
     * Generate a key with {@link Structure#BY_DATE} structure
     * 
     * @param name
     * @return
     */
    public static String newKey(String name) {
        return newKey(name, Structure.BY_DATE);
    }

    /**
     * Generate a key using user specified name and structure. If name is <code>null</code>
     * or empty, then a random <code>UUID</code> string is generated with all dash symbol <code>-</code>
     * removed
     *
     * @param name      - the proposed name of the file. optional, if it is null
     *                  then a random name will be generated
     * @param structure - the storage structure
     * @return the key
     */
    public static String newKey(String name, Structure structure) {
        if (null == name || name.trim().equals("")) {
            name = UUID.randomUUID().toString().replace("-", "");
        }

        if (null == structure) structure = Structure.BY_DATE;
        switch (structure) {
            case BY_DATETIME:
                return String.format("%1$tY/%1$tm/%1$td/%1$tH/%1$tM/%1$tS/%2$s", Calendar.getInstance(), name);
            case BY_DATE:
                return String.format("%1$tY/%1$tm/%1$td/%2$s", Calendar.getInstance(), name);
            case PLAIN:
                return name;
            default:
                throw new RuntimeException("oops, how can I get here?");
        }
    }
    
    public static void main(String[] args) {
        System.out.println(newKey());
        System.out.println(newKey("Hello.world"));
        System.out.println(newKey("Hello.world", Structure.BY_DATETIME));
        System.out.println(newKey(Structure.PLAIN));
    }

}
