/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.app.logging.slf4j;

import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.ESLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Slf4jESLoggerFactory extends ESLoggerFactory {

    @Override
    protected ESLogger rootLogger() {
        return getLogger(Logger.ROOT_LOGGER_NAME);
    }

    @Override
    protected ESLogger newInstance(String prefix, String name) {
        return new Slf4jESLogger(prefix, LoggerFactory.getLogger(name));
    }
}
