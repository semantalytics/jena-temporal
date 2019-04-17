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

package org.apache.jena.query.text;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.text.assembler.TemporalAssembler;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestDatasetWithLuceneTemporalIndexWithLangField extends AbstractTestDatasetWithTextIndexBase {
    
    private static final String SPEC_BASE = "http://example.org/spec#";
    private static final String SPEC_ROOT_LOCAL = "lucene_text_dataset";
    private static final String SPEC_ROOT_URI = SPEC_BASE + SPEC_ROOT_LOCAL;
    private static final String SPEC;
    static {
        SPEC = StrUtils.strjoinNL(
                    "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ",
                    "prefix ja:   <http://jena.hpl.hp.com/2005/11/Assembler#> ",
                    "prefix tdb:  <http://jena.hpl.hp.com/2008/tdb#>",
                    "prefix temporal: <http://jena.apache.org/temporal#>",
                    "prefix :     <" + SPEC_BASE + ">",
                    "",
                    "[] ja:loadClass    \"org.apache.jena.query.temporal.TextQuery\" .",
                    "temporal:TextDataset      rdfs:subClassOf   ja:RDFDataset .",
                    "temporal:TextIndexLucene  rdfs:subClassOf   temporal:TextIndex .",
                    
                    ":" + SPEC_ROOT_LOCAL,
                    "    a              temporal:TextDataset ;",
                    "    temporal:dataset   :dataset ;",
                    "    temporal:index     :indexLucene ;",
                    "    .",
                    "",
                    ":dataset",
                    "    a               ja:RDFDataset ;",
                    "    ja:defaultGraph :graph ;",
                    ".",
                    ":graph",
                    "    a               ja:MemoryModel ;",
                    ".",
                    "",
                    ":indexLucene",
                    "    a temporal:TextIndexLucene ;",
                    "    temporal:directory \"mem\" ;",
                    "    temporal:entityMap :entMap ;",
                    "    .",
                    "",
                    ":entMap",
                    "    a temporal:EntityMap ;",
                    "    temporal:entityField      \"uri\" ;",
                    "    temporal:defaultField     \"label\" ;",
                    "    temporal:langField        \"language\" ;",
                    "    temporal:map (",
                    "         [ temporal:field \"label\" ; temporal:predicate rdfs:label ]",
                    "         [ temporal:field \"comment\" ; temporal:predicate rdfs:comment ]",
                    "         ) ."
                    );
    }
    
    @Before
    public void before() {
        Reader reader = new StringReader(SPEC);
        Model specModel = ModelFactory.createDefaultModel();
        specModel.read(reader, "", "TURTLE");
        TemporalAssembler.init();
        Resource root = specModel.getResource(SPEC_ROOT_URI);
        dataset = (Dataset) Assembler.general.open(root);
    }
    
    @After
    public void after() {
        dataset.close();
    }
    
    @Test
    public void testLiteralLanguageSearch(){
        final String turtle = StrUtils.strjoinNL(
                TURTLE_PROLOG,
                "<" + RESOURCE_BASE + "ParisInEnglish>",
                "  rdfs:label 'Paris, capital of France'@en",
                ".",
                TURTLE_PROLOG,
                "<" + RESOURCE_BASE + "ParisInFrench>",
                "  rdfs:label 'Paris, capitale de la France'@fr",
                "."
        );
        String queryString = StrUtils.strjoinNL(
                QUERY_PROLOG,
                "SELECT ?s",
                "WHERE {",
                "    ?s temporal:query ( rdfs:label 'paris' 'lang:en' 10 ) .",
                "}"
        );
        Set<String> expectedURIs = new HashSet<>() ;
        expectedURIs.addAll( Arrays.asList("http://example.org/data/resource/ParisInEnglish")) ;
        doTestSearch(turtle, queryString, expectedURIs);
    }

    @Test
    public void testLiteralLanguageSearchItalian(){
        final String turtle = StrUtils.strjoinNL(
                TURTLE_PROLOG,
                "<" + RESOURCE_BASE + "ParisInEnglish>",
                "  rdfs:label 'Paris, capital of France'@en",
                ".",
                TURTLE_PROLOG,
                "<" + RESOURCE_BASE + "ParisInItalian>",
                "  rdfs:label \"Paris è l'endonimo francese di Parigi, capitale della Francia\"@it",
                ".",
                TURTLE_PROLOG,
                "<" + RESOURCE_BASE + "ParisInFrench>",
                "  rdfs:label 'Paris, capitale de la France'@fr",
                "."
        );
        String queryString = StrUtils.strjoinNL(
                QUERY_PROLOG,
                "SELECT ?s",
                "WHERE {",
                "    ?s temporal:query ( rdfs:label 'paris' 'lang:it' 10 ) .",
                "}"
        );
        Set<String> expectedURIs = new HashSet<>() ;
        expectedURIs.addAll( Arrays.asList("http://example.org/data/resource/ParisInItalian")) ;
        doTestSearch(turtle, queryString, expectedURIs);
    }
}
