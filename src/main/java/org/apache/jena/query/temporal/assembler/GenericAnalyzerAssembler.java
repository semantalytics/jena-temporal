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

package org.apache.jena.query.temporal.assembler;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.atlas.logging.Log ;
import org.apache.jena.query.text.TemporalIndexException;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.lucene.analysis.Analyzer;

/**
 * Creates generic analyzers given a fully qualified Class name and a list
 * of parameters for a constructor of the Class.
 * <p>
 * The parameters may be of the following types:
 * <pre>
 *     temporal:TypeString    String
 *     temporal:TypeSet       org.apache.lucene.analysis.util.CharArraySet
 *     temporal:TypeFile      java.io.FileReader
 *     temporal:TypeInt       int
 *     temporal:TypeBoolean   boolean
 *     temporal:TypeAnalyzer  org.apache.lucene.analysis.Analyzer
 * </pre>
 * 
 * Although the list of types is not exhaustive it is a simple matter
 * to create a wrapper Analyzer that reads a file with information that can
 * be used to initialize any sort of parameters that may be needed for
 * a given Analyzer. The provided types cover the vast majority of cases.
 * <p>
 * For example, <code>org.apache.lucene.analysis.ja.JapaneseAnalyzer</code>
 * has a constructor with 4 parameters: a <code>UserDict</code>,
 * a <code>CharArraySet</code>, a <code>JapaneseTokenizer.Mode</code>, and a 
 * <code>Set&lt;String&gt;</code>. So a simple wrapper can extract the values
 * needed for the various parameters with types not available in this
 * extension, construct the required instances, and instantiate the
 * <code>JapaneseAnalyzer</code>.
 * <p>
 * Adding custom Analyzers such as the above wrapper analyzer is a simple
 * matter of adding the Analyzer class and any associated filters and tokenizer
 * and so on to the classpath for Jena - usually in a jar. Of course, all of 
 * the Analyzers that are included in the Lucene distribution bundled with Jena
 * are available as generic Analyzers as well.
 * <p>
 * Each parameter object is specified with:
 * <ul>
 * <li>an optional <code>temporal:paramName</code> that may be used to document which
 * parameter is represented</li>
 * <li>a <code>temporal:paramType</code> which is one of: <code>temporal:TypeString</code>,
 * <code>temporal:TypeSet</code>, <code>temporal:TypeFile</code>, <code>temporal:TypeInt</code>,
 * <code>temporal:TypeBoolean</code>, <code>temporal:TypeAnalyzer</code>.</li>
 * <li>a temporal:paramValue which is an xsd:string, xsd:boolean or xsd:int or resource.</li>
 * </ul>
 * <p>
 * A parameter of type <code>temporal:TypeSet</code> <i>must have</i> a list of zero or
 * more <code>String</code>s.
 * <p>
 * A parameter of type <code>temporal:TypeString</code>, <code>temporal:TypeFile</code>,
 * <code>temporal:TypeBoolean</code>, <code>temporal:TypeInt</code> or <code>temporal:TypeAnalyzer</code>
 * <i>must have</i> a single <code>temporal:paramValue</code> of the appropriate type.
 * <p>
 * Examples:
 * <pre>
    temporal:map (
         [ temporal:field "temporal" ;
           temporal:predicate rdfs:label;
           temporal:analyzer [
               a temporal:GenericAnalyzer ;
               temporal:class "org.apache.lucene.analysis.en.EnglishAnalyzer" ;
               temporal:params (
                    [ temporal:paramName "stopwords" ;
                      temporal:paramType temporal:TypeSet ;
                      temporal:paramValue ("the" "a" "an") ]
                    [ temporal:paramName "stemExclusionSet" ;
                      temporal:paramType temporal:TypeSet ;
                      temporal:paramValue ("ing" "ed") ]
                    )
           ] .
 * </pre>
 * <pre>
    temporal:map (
         [ temporal:field "temporal" ;
           temporal:predicate rdfs:label;
           temporal:analyzer [
               a temporal:GenericAnalyzer ;
               temporal:class "org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper" ;
               temporal:params (
                    [ temporal:paramName "defaultAnalyzer" ;
                      temporal:paramType temporal:TypeAnalyzer ;
                      temporal:paramValue [ a temporal:SimpleAnalyzer ] ]
                    [ temporal:paramName "maxShingleSize" ;
                      temporal:paramType temporal:TypeInt ;
                      temporal:paramValue 3 ]
                    )
           ] .
 * </pre>
 */
public class GenericAnalyzerAssembler extends AssemblerBase {
    /*
    temporal:map (
         [ temporal:field "temporal" ;
           temporal:predicate rdfs:label;
           temporal:analyzer [
               a temporal:GenericAnalyzer ;
               temporal:class "org.apache.lucene.analysis.en.EnglishAnalyzer" ;
               temporal:params (
                    [ temporal:paramName "stopwords" ;
                      temporal:paramType temporal:TypeSet ;
                      temporal:paramValue ("the" "a" "an") ]
                    [ temporal:paramName "stemExclusionSet" ;
                      temporal:paramType temporal:TypeSet ;
                      temporal:paramValue ("ing" "ed") ]
                    )
           ] .
     */

    @Override
    public Analyzer open(Assembler a, Resource root, Mode mode) {
        if (root.hasProperty(TemporalVocab.pClass)) {
            // temporal:class is expected to be a string literal
            String className = root.getProperty(TemporalVocab.pClass).getString();

            // is the class accessible?
            Class<?> clazz = null;
            try {
                clazz = Class.forName(className);
            } catch (ClassNotFoundException e) {
                Log.error(this, "Analyzer class " + className + " not found. " + e.getMessage(), e);
                return null;
            }

            // Is the class an Analyzer?
            if (!Analyzer.class.isAssignableFrom(clazz)) {
                Log.error(this, clazz.getName() + " has to be a subclass of " + Analyzer.class.getName());
                return null;
            }

            if (root.hasProperty(TemporalVocab.pParams)) {
                RDFNode node = root.getProperty(TemporalVocab.pParams).getObject();
                if (! node.isResource()) {
                    throw new TemporalIndexException("temporal:params must be a list of parameter resources: " + node);
                }

                List<Params.ParamSpec> specs = Params.getParamSpecs((Resource) node);

                // split the param specs into classes and values for constructor lookup
                final Class<?> paramClasses[] = new Class<?>[specs.size()];
                final Object paramValues[] = new Object[specs.size()];
                for (int i = 0; i < specs.size(); i++) {
                    Params.ParamSpec spec = specs.get(i);
                    paramClasses[i] = spec.getValueClass();
                    paramValues[i] = spec.getValue();
                }

                // Create new analyzer
                return newAnalyzer(clazz, paramClasses, paramValues);

            } else {
                // use the nullary Analyzer constructor
                return newAnalyzer(clazz, new Class<?>[0], new Object[0]);
            }
        } else {
            throw new TemporalIndexException("temporal:class property is required by GenericAnalyzer: " + root);
        }
    }

    /**
     * Create instance of the Lucene Analyzer, <code>class</code>, with provided parameters
     *
     * @param clazz The analyzer class
     * @param paramClasses The parameter classes
     * @param paramValues The parameter values
     * @return The lucene analyzer
     */
    private Analyzer newAnalyzer(Class<?> clazz, Class<?>[] paramClasses, Object[] paramValues) {

        String className = clazz.getName();

        try {
            final Constructor<?> cstr = clazz.getDeclaredConstructor(paramClasses);

            return (Analyzer) cstr.newInstance(paramValues);

        } catch (IllegalArgumentException | IllegalAccessException | InstantiationException | InvocationTargetException | SecurityException e) {
            Log.error(this, "Exception while instantiating analyzer class " + className + ". " + e.getMessage(), e);
        } catch (NoSuchMethodException ex) {
            Log.error(this, "Could not find matching analyzer class constructor for " + className + " " + ex.getMessage(), ex);
        }

        return null;
    }
}
