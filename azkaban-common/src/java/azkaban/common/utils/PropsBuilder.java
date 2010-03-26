/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.common.utils;

/**
 * A helper class for building props
 * 
 * @author jkreps
 * 
 */
public class PropsBuilder {

    Props _parent;
    Props _props;

    public PropsBuilder() {
        this(null);
    }

    public PropsBuilder(Props parent) {
        _parent = parent;
        _props = new Props(parent);
    }

    public PropsBuilder with(String key, String value) {
        _props.put(key, value);
        return this;
    }

    public PropsBuilder with(String key, int value) {
        _props.put(key, value);
        return this;
    }

    public PropsBuilder with(String key, double value) {
        _props.put(key, value);
        return this;
    }

    public PropsBuilder with(String key, long value) {
        _props.put(key, value);
        return this;
    }

    public Props create() {
        Props p = _props;
        _props = new Props(_parent);
        return p;
    }

}
