/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.query.temporal;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jena.datatypes.RDFDatatype;

import java.util.HashMap ;
import java.util.Map ;

public class Entity
{
    private final String id ;
    private final String graph ;
    private final RDFDatatype datatype ;
    private final Map<String, Object> map = new HashMap<>() ;

    public Entity(String entityId, String entityGraph) {
        this(entityId, entityGraph, null);
    }

    public Entity(String entityId, String entityGraph, RDFDatatype datatype) {
        this.id = entityId ;
        this.graph = entityGraph;
        this.datatype = datatype;
    }

    public String getId() {
        return id;
    }

    public String getGraph() {
        return graph;
    }

    public RDFDatatype getDatatype() {
        return datatype;
    }

    public void put(String key, Object value) {
        map.put(key, value);
    }
    
    public Object get(String key) {
        return map.get(key);
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public String getChecksum(String property, String value) {
        String key = getGraph() + "-" + getId() + "-" + property + "-" + value;
        return DigestUtils.sha256Hex(key);
    }

    @Override
    public String toString() {
        return id + " : " + map ;
    }
    
    public String toStringDetail() {
        return id + " : " + graph + " : " + datatype + " : " + map ;
    }
}
