package org.osgl.storage;

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

import java.util.UUID;

/**
 * Responsible for providing unique ID as the key name
 */
public interface KeyNameProvider {

    /**
     * Generate key name using random UUID
     */
    KeyNameProvider DEF_PROVIDER = new KeyNameProvider() {
        @Override
        public String newKeyName() {
            return UUID.randomUUID().toString();
        }
    };

    /**
     * Returns a unique key name. Note this method shall not
     * return the hierarchical structure which is the responsibility
     * of the {@link KeyGenerator}
     *
     * @return the key name
     */
    String newKeyName();

}
