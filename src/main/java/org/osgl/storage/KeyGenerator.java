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

import org.osgl.util.E;
import org.osgl.util.S;

import java.util.Calendar;
import java.util.UUID;

/**
 * Generate or decorate storage object key. There are three types of key generator:
 * 1. PLAIN, all files are saved directly in one folder
 * 2. BY_DATE, files are saved in a hierarchical structure like yyyy/MM/dd
 * 3. BY_DATETIME, files are saved in a hierarchical structure like yyyy/MM/dd/HH/mm/ss
 */
public enum KeyGenerator {
    /**
     * All item stored in the bucket (root folder) without hierarchy
     */
    PLAIN {
        @Override
        protected String tmpl() {
            return null;
        }
    },
    /**
     * Items stored in a hierarchy structured by date: /yyyy/MM/dd/item 
     */
    BY_DATE {
        @Override
        protected String tmpl() {
            return "%1$tY/%1$tm/%1$td/%2$s";
        }
    }, 
    /**
     * Items stored in a hierarchy structured by date and time: /yyyy/MM/dd/HH/mm/ss/item 
     */
    BY_DATETIME {
        @Override
        protected String tmpl() {
            return "%1$tY/%1$tm/%1$td/%1$tH/%1$tM/%1$tS/%2$s";
        }
    };
    
    protected abstract String tmpl();
    
    public String getKey(String name) {
        if (S.blank(name)) {
            name = UUID.randomUUID().toString();
        }
        String tmpl = tmpl();
        if (S.blank(tmpl)) {
            return name;
        } else {
            return S.fmt(tmpl, Calendar.getInstance(), name);
        }
    }
    
    public String getKey() {
        return getKey(null);
    }

    public static KeyGenerator valueOfIgnoreCase(String s) {
        E.NPE(s);
        if (BY_DATE.name().equalsIgnoreCase(s) || "byDate".equalsIgnoreCase(s) || "date".equalsIgnoreCase(s)) {
            return BY_DATE;
        } else if (BY_DATETIME.name().equalsIgnoreCase(s) || "byDateTime".equalsIgnoreCase(s) || "dateTime".equalsIgnoreCase(s)) {
            return BY_DATETIME;
        } else if (PLAIN.name().equalsIgnoreCase(s)) {
            return PLAIN;
        }
        throw E.invalidArg("unknown KeyGenerator name: %s", s);
    }

}
