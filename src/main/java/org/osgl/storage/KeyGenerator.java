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

/**
 * Generate or decorate storage object key. There are three types of key generator:
 * 1. PLAIN, all files are saved directly in one folder
 * 2. BY_DATE, files are saved in a hierarchical structure like yyyy/MM/dd
 * 3. BY_DATETIME, files are saved in a hierarchical structure like yyyy/MM/dd/HH/mm/ss
 */
public interface KeyGenerator {

    String getKey(String name, KeyNameProvider keyNameProvider);

    enum Predefined implements KeyGenerator {
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
         * Items stored in a hierarchy structured by date and time: /yyyy/MM/dd/HH/item
         */
        BY_HOUR {
            @Override
            protected String tmpl() {
                return "%1$tY/%1$tm/%1$td/%1$tH/%2$s";
            }
        },
        /**
         * Items stored in a hierarchy structured by date and time: /yyyy/MM/dd/HH/mm/item
         */
        BY_MINUTE {
            @Override
            protected String tmpl() {
                return "%1$tY/%1$tm/%1$td/%1$tH/%1$tM/%2$s";
            }
        },
        /**
         * Items stored in a hierarchy structured by date and time: /yyyy/MM/dd/HH/mm/ss/item
         */
        BY_SECOND {
            @Override
            protected String tmpl() {
                return "%1$tY/%1$tm/%1$td/%1$tH/%1$tM/%1$tS/%2$s";
            }
        },
        /**
         * Items stored in a hierarchy structured by date and time: /yyyy/MM/dd/HH/mm/ss/item
         *
         * Note this enum value is deprecated, please use `BY_SECOND` instead
         */
        @Deprecated
        BY_DATETIME {
            @Override
            protected String tmpl() {
                return "%1$tY/%1$tm/%1$td/%1$tH/%1$tM/%1$tS/%2$s";
            }
        };

        protected abstract String tmpl();

        public String getKey(String name, KeyNameProvider keyNameProvider) {
            if (S.blank(name)) {
                name = keyNameProvider.newKeyName();
            }
            String tmpl = tmpl();
            if (S.blank(tmpl)) {
                return name;
            } else {
                return S.fmt(tmpl, Calendar.getInstance(), name);
            }
        }

        public static KeyGenerator valueOfIgnoreCase(String s) {
            E.NPE(s);
            if (BY_DATE.name().equalsIgnoreCase(s) || "byDate".equalsIgnoreCase(s) || "date".equalsIgnoreCase(s)) {
                return BY_DATE;
            } else if (BY_HOUR.name().equalsIgnoreCase(s) || "byHour".equalsIgnoreCase(s) || "hour".equalsIgnoreCase(s)) {
                return BY_HOUR;
            } else if (BY_MINUTE.name().equalsIgnoreCase(s) || "byMinute".equalsIgnoreCase(s) || "minute".equalsIgnoreCase(s) || "min".equalsIgnoreCase(s)) {
                return BY_MINUTE;
            } else if (PLAIN.name().equalsIgnoreCase(s)) {
                return PLAIN;
            } else if ("BY_DATETIME".equalsIgnoreCase(s)
                    || "byDateTime".equalsIgnoreCase(s)
                    || "dateTime".equalsIgnoreCase(s)
                    || BY_SECOND.name().equalsIgnoreCase(s)
                    || "bySecond".equalsIgnoreCase(s)
                    || "second".equalsIgnoreCase(s)
                    || "sec".equalsIgnoreCase(s)) {
                return BY_SECOND;
            }
            return null;
        }
    }



}
