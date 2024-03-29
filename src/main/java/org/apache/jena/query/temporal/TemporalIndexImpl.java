/*
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

package org.apache.jena.query.temporal ;

import java.io.IOException ;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry ;

import de.qaware.chronix.ChronixClient;
import de.qaware.chronix.converter.ChronixTimeSeriesDefaults;
import de.qaware.chronix.converter.MetricTimeSeriesConverter;
import de.qaware.chronix.lucene.client.ChronixLuceneStorage;
import de.qaware.chronix.lucene.client.LuceneIndex;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.datatypes.RDFDatatype ;
import org.apache.jena.datatypes.TypeMapper ;
import org.apache.jena.datatypes.xsd.XSDDatatype ;
import org.apache.jena.graph.Node ;
import org.apache.jena.graph.NodeFactory ;
import org.apache.jena.query.text.analyzer.IndexingMultilingualAnalyzer;
import org.apache.jena.query.text.analyzer.MultilingualAnalyzer;
import org.apache.jena.query.text.analyzer.QueryMultilingualAnalyzer;
import org.apache.jena.query.text.analyzer.Util;
import org.apache.jena.sparql.util.NodeFactoryExtra ;
import org.apache.lucene.analysis.Analyzer ;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer ;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper ;
import org.apache.lucene.analysis.standard.StandardAnalyzer ;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.analyzing.AnalyzingQueryParser ;
import org.apache.lucene.queryparser.classic.ParseException ;
import org.apache.lucene.queryparser.classic.QueryParser ;
import org.apache.lucene.queryparser.classic.QueryParserBase ;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser ;
import org.apache.lucene.search.IndexSearcher ;
import org.apache.lucene.search.Query ;
import org.apache.lucene.search.ScoreDoc ;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment ;
import org.apache.lucene.store.Directory ;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class TemporalIndexImpl implements org.apache.jena.query.temporal.TemporalIndex {

    private static Logger log = LoggerFactory.getLogger(TemporalIndexImpl.class) ;

    private static int             MAX_N    = 10000 ;
    // prefix for storing datatype URIs in the index, to distinguish them from language tags
    private static final String    DATATYPE_PREFIX = "^^";
    
    private static final String    RIGHT_ARROW = "\u21a6";
    private static final String    LEFT_ARROW  = "\u21a4";
    private static final String    DIVIDES  =    "\u2223";
    private static final String    Z_MORE_SEPS = "([\\p{Z}\u0f0b\0f0c\0f0d\180e]*?)";

    public static final FieldType  ftIRI ;
    static {
        ftIRI = new FieldType() ;
        ftIRI.setTokenized(false) ;
        ftIRI.setStored(true) ;
        ftIRI.setIndexOptions(IndexOptions.DOCS);
        ftIRI.freeze() ;
    }
    public static final FieldType  ftString = StringField.TYPE_NOT_STORED ;

    private final EntityDefinition docDef ;
    private final Directory        directory ;
    private final Analyzer         indexAnalyzer ;
    private       Analyzer         defaultAnalyzer ;
    private       Map<String, Analyzer> analyzerPerField;
    private final Analyzer         queryAnalyzer ;
    private final String           queryParserType ;
    private final FieldType        ftText ;
    private final boolean          isMultilingual ;
    
    private Map<String, Analyzer> multilingualQueryAnalyzers = new HashMap<>();

    // The IndexWriter can't be final because we may have to recreate it if rollback() is called.
    // However, it needs to be volatile in case the next write transaction is on a different thread,
    // but we do not need locking because we are assuming that there can only be one writer
    // at a time (enforced elsewhere).
    private  IndexWriter   indexWriter ;

    /**
     * Constructs a new TextIndexLucene.
     *
     * @param directory The Lucene Directory for the index
     * @param config The config definition for the index instantiation.
     */
    public TemporalIndexImpl(Directory directory, TemporalIndexConfig config) {

        this.directory = directory ;
        this.docDef = config.getEntDef() ;

        MetricTimeSeries ts = new MetricTimeSeries.Builder("", "").build();
        ts.add

        thisther.isMultilingual = config.isMultilingualSupport();
        if (this.isMultilingual &&  config.getEntDef().getLangField() == null) {
            //multilingual index cannot work without lang field
            docDef.setLangField("lang");
        }

        // create the analyzer as a wrapper that uses KeywordAnalyzer for
        // entity and graph fields and the configured analyzer(s) for all other
        analyzerPerField = new HashMap<>() ;
        analyzerPerField.put(docDef.getEntityField(), new KeywordAnalyzer()) ;
        if ( docDef.getGraphField() != null )
            analyzerPerField.put(docDef.getGraphField(), new KeywordAnalyzer()) ;
        if ( docDef.getLangField() != null )
            analyzerPerField.put(docDef.getLangField(), new KeywordAnalyzer()) ;

        for (String field : docDef.fields()) {
            Analyzer _analyzer = docDef.getAnalyzer(field);
            if (_analyzer != null) {
                analyzerPerField.put(field, _analyzer);
            }
        }

        defaultAnalyzer = (null != config.getAnalyzer()) ? config.getAnalyzer() : new StandardAnalyzer();
        Analyzer indexDefault = defaultAnalyzer;
        Analyzer queryDefault = defaultAnalyzer;
        if (this.isMultilingual) {
            queryDefault = new MultilingualAnalyzer(defaultAnalyzer);
            indexDefault = Util.usingIndexAnalyzers() ? new IndexingMultilingualAnalyzer(defaultAnalyzer) : queryDefault;
        }
        this.indexAnalyzer = new PerFieldAnalyzerWrapper(indexDefault, analyzerPerField) ;
        this.queryAnalyzer = (null != config.getQueryAnalyzer()) ? config.getQueryAnalyzer() : new PerFieldAnalyzerWrapper(queryDefault, analyzerPerField) ;
        this.queryParserType = config.getQueryParser() ;
        log.debug("TextIndexLucene defaultAnalyzer: {}, indexAnalyzer: {}, queryAnalyzer: {}, queryParserType: {}", defaultAnalyzer, indexAnalyzer, queryAnalyzer, queryParserType);
        this.ftText = config.isValueStored() ? TextField.TYPE_STORED : TextField.TYPE_NOT_STORED ;
        if (config.isValueStored() && docDef.getLangField() == null)
            log.warn("Values stored but langField not set. Returned values will not have language tag or datatype.");

        openIndexWriter();
    }

    private void openIndexWriter() {
        private static final LuceneIndex luceneIndex = new LuceneIndex(FSDirectory.open(Paths.get("")), new StandardAnalyzer(());
        IndexWriterConfig wConfig = new IndexWriterConfig(indexAnalyzer) ;
        try
        {
            indexWriter = new IndexWriter(directory, wConfig) ;
            // Force a commit to create the index, otherwise querying before writing will cause an exception
            indexWriter.commit();
        }
        catch (IndexFormatTooOldException e) {
            throw new TemporalIndexException("jena-temporal/Lucene cannot use indexes created before Jena 3.3.0. "
                + "Please rebuild your temporal index using jena.textindexer from Jena 3.3.0 or above.", e);
        }
        catch (IOException e)
        {
            throw new TemporalIndexException("openIndexWriter", e) ;
        }
    }

    public Directory getDirectory() {
        return directory ;
    }

    public Analyzer getAnalyzer() {
        return indexAnalyzer ;
    }

    public Analyzer getQueryAnalyzer() {
        return queryAnalyzer ;
    }

    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    @Override
    public void prepareCommit() {
        try {
            indexWriter.prepareCommit();
        }
        catch (IOException e) {
            throw new TemporalIndexException("prepareCommit", e);
        }
    }

    @Override
    public void commit() {
        try {
            indexWriter.commit();
        }
        catch (IOException e) {
            throw new TemporalIndexException("commit", e);
        }
    }

    @Override
    public void rollback() {
        IndexWriter idx = indexWriter;
        indexWriter = null;
        try {
            idx.rollback();
        }
        catch (IOException e) {
            throw new TemporalIndexException("rollback", e);
        }

        // The rollback will close the indexWriter, so we need to reopen it
        openIndexWriter();
    }

    @Override
    public void close() {
        try {
            indexWriter.close() ;
        }
        catch (IOException ex) {
            throw new TemporalIndexException("close", ex) ;
        }
    }

    @Override public void updateEntity(Entity entity) {
        if ( log.isDebugEnabled() )
            if (log.isTraceEnabled() && entity != null)
                log.trace("Update entity: " + entity.toStringDetail()) ;
            else
                log.debug("Update entity: " + entity) ;
        try {
            updateDocument(entity);
        } catch (IOException e) {
            throw new TemporalIndexException("updateEntity", e) ;
        }
    }

    protected void updateDocument(Entity entity) throws IOException {
        Document doc = doc(entity);
        Term term = new Term(docDef.getEntityField(), entity.getId());
        indexWriter.updateDocument(term, doc);
        log.trace("updated: {}", doc) ;
    }

    @Override
    public void addEntity(Entity entity) {
        if ( log.isDebugEnabled() )
            if (log.isTraceEnabled() && entity != null)
                log.trace("Add entity: " + entity.toStringDetail()) ;
            else
                log.debug("Add entity: " + entity) ;
        try {
            addDocument(entity);
        }
        catch (IOException e) {
            throw new TemporalIndexException("addEntity", e) ;
        }
    }

    protected void addDocument(Entity entity) throws IOException {
        Document doc = doc(entity) ;
        indexWriter.addDocument(doc) ;
        log.trace("added: {}", doc) ;
    }

    @Override
    public void deleteEntity(Entity entity) {
        if (docDef.getUidField() == null)
            return;

        if ( log.isDebugEnabled() )
            if (log.isTraceEnabled() && entity != null)
                log.trace("Delete entity: " + entity.toStringDetail()) ;
            else
                log.debug("Delete entity: "+entity) ;
        try {
            Map<String, Object> map = entity.getMap();
            String property = map.keySet().iterator().next();
            String value = (String)map.get(property);
            String hash = entity.getChecksum(property, value);
            Term uid = new Term(docDef.getUidField(), hash);
            indexWriter.deleteDocuments(uid);

        } catch (Exception e) {
            throw new TemporalIndexException("deleteEntity", e) ;
        }
    }

    protected Document doc(Entity entity) {
        Document doc = new Document() ;
        Field entField = new Field(docDef.getEntityField(), entity.getId(), ftIRI) ;
        doc.add(entField) ;

        String graphField = docDef.getGraphField() ;
        if ( graphField != null ) {
            Field gField = new Field(graphField, entity.getGraph(), ftIRI) ;
            doc.add(gField) ;
        }

        String langField = docDef.getLangField() ;
        String uidField = docDef.getUidField() ;

        for ( Entry<String, Object> e : entity.getMap().entrySet() ) {
            doc.add( new Field(e.getKey(), (String) e.getValue(), ftText) );
            if (langField != null) {
                String lang = entity.getLanguage();
                RDFDatatype datatype = entity.getDatatype();
                if (lang != null && !"".equals(lang)) {
                    doc.add(new Field(langField, lang, StringField.TYPE_STORED));
                    if (this.isMultilingual) {
                        // add a field that uses a language-specific analyzer via MultilingualAnalyzer
                        doc.add(new Field(e.getKey() + "_" + lang, (String) e.getValue(), ftText));
                        // add fields for any defined auxiliary indexes
                        List<String> auxIndexes = Util.getAuxIndexes(lang);
                        if (auxIndexes != null) {
                            for (String auxTag : auxIndexes) {
                                doc.add(new Field(e.getKey() + "_" + auxTag, (String) e.getValue(), ftText));
                            }
                        }
                    }
                } else if (datatype != null && !datatype.equals(XSDDatatype.XSDstring)) {
                    // for non-string and non-langString datatypes, store the datatype in langField
                    doc.add(new Field(langField, DATATYPE_PREFIX + datatype.getURI(), StringField.TYPE_STORED));
                }
            }
            if (uidField != null) {
                String hash = entity.getChecksum(e.getKey(), (String) e.getValue());
                doc.add(new Field(uidField, hash, StringField.TYPE_STORED));
            }
        }
        return doc ;
    }

    @Override
    public Map<String, Node> get(String uri) {
        try {
            IndexReader indexReader = DirectoryReader.open(directory);
            List<Map<String, Node>> x = get$(indexReader, uri) ;
            if ( x.size() == 0 )
                return null ;
            // if ( x.size() > 1)
            // throw new TemporalIndexException("Multiple entires for "+uri) ;
            return x.get(0) ;
        }
        catch (Exception ex) {
            throw new TemporalIndexException("get", ex) ;
        }
    }

    private QueryParser getQueryParser(Analyzer analyzer) {
        switch(queryParserType) {
            case "QueryParser":
                return new QueryParser(docDef.getPrimaryField(), analyzer) ;
            case "AnalyzingQueryParser":
                return new AnalyzingQueryParser(docDef.getPrimaryField(), analyzer) ;
            case "ComplexPhraseQueryParser":
                return new ComplexPhraseQueryParser(docDef.getPrimaryField(), analyzer);
            default:
                log.warn("Unknown query parser type '" + queryParserType + "'. Defaulting to standard QueryParser") ;
                return new QueryParser(docDef.getPrimaryField(), analyzer) ;
        }
    }

    private Query parseQuery(String queryString, Analyzer analyzer) throws ParseException {
        QueryParser queryParser = getQueryParser(analyzer) ;
        queryParser.setAllowLeadingWildcard(true) ;
        Query query = queryParser.parse(queryString) ;
        return query ;
    }

    private List<Map<String, Node>> get$(IndexReader indexReader, String uri) throws ParseException, IOException {
        String escaped = QueryParserBase.escape(uri) ;
        String qs = docDef.getEntityField() + ":" + escaped ;
        Query query = parseQuery(qs, queryAnalyzer) ;
        IndexSearcher indexSearcher = new IndexSearcher(indexReader) ;
        ScoreDoc[] sDocs = indexSearcher.search(query, 1).scoreDocs ;
        List<Map<String, Node>> records = new ArrayList<>() ;

        for ( ScoreDoc sd : sDocs ) {
            Document doc = indexSearcher.doc(sd.doc) ;
            String[] x = doc.getValues(docDef.getEntityField()) ;
            if ( x.length != 1 ) {}
            String uriStr = x[0] ;
            Map<String, Node> record = new HashMap<>() ;
            Node entity = NodeFactory.createURI(uriStr) ;
            record.put(docDef.getEntityField(), entity) ;

            for ( String f : docDef.fields() ) {
                // log.info("Field: "+f) ;
                String[] values = doc.getValues(f) ;
                for ( String v : values ) {
                    Node n = entryToNode(v) ;
                    record.put(f, n) ;
                }
                records.add(record) ;
            }
        }
        return records ;
    }

    @Override
    public List<TemporalHit> query(Node property, String qs, String graphURI, String lang) {
        return query(property, qs, graphURI, lang, MAX_N) ;
    }

    @Override
    public List<TemporalHit> query(Node property, String qs, String graphURI, String lang, int limit) {
        return query(property, qs, graphURI, lang, MAX_N, null) ;
    }

    @Override
    public List<TemporalHit> query(Node property, String qs, String graphURI, String lang, int limit, String highlight) {
        try (IndexReader indexReader = DirectoryReader.open(directory)) {
            return query$(indexReader, property, qs, graphURI, lang, limit, highlight) ;
        }
        catch (ParseException ex) {
            throw new TemporalIndexParseException(qs, ex.getMessage()) ;
        }
        catch (Exception ex) {
            throw new TemporalIndexException("query", ex) ;
        }
    }

    private List<TemporalHit> simpleResults(ScoreDoc[] sDocs, IndexSearcher indexSearcher, Query query, String field)
            throws IOException {
        List<TemporalHit> results = new ArrayList<>() ;

        for ( ScoreDoc sd : sDocs ) {
            Document doc = indexSearcher.doc(sd.doc) ;
            log.trace("simpleResults[{}]: {}", sd.doc, doc) ;
            String entity = doc.get(docDef.getEntityField()) ;

            Node literal = null;
            //            String field = (property != null) ? docDef.getField(property) : docDef.getPrimaryField();
            String lexical = doc.get(field) ;

            if (lexical != null) {
                String doclang = doc.get(docDef.getLangField()) ;
                if (doclang != null) {
                    if (doclang.startsWith(DATATYPE_PREFIX)) {
                        String datatype = doclang.substring(DATATYPE_PREFIX.length());
                        TypeMapper tmap = TypeMapper.getInstance();
                        literal = NodeFactory.createLiteral(lexical, tmap.getSafeTypeByName(datatype));
                    } else {
                        literal = NodeFactory.createLiteral(lexical, doclang);
                    }
                } else {
                    literal = NodeFactory.createLiteral(lexical);
                }
            }

            String graf = docDef.getGraphField() != null ? doc.get(docDef.getGraphField()) : null ;
            Node graph = graf != null ? TextQueryFuncs.stringToNode(graf) : null;

            Node subject = TextQueryFuncs.stringToNode(entity) ;
            TemporalHit hit = new TemporalHit(subject, sd.score, literal, graph);
            results.add(hit) ;
        }
        
        return results ;
    }

    class HighlightOpts {
        int maxFrags = 3;
        int fragSize = 128;
        String start = RIGHT_ARROW;
        String end = LEFT_ARROW;
        String fragSep = DIVIDES;
        boolean joinHi = true;
        boolean joinFrags = true;
        
        public HighlightOpts(String optStr) {
            String[] opts = optStr.trim().split("\\|");
            for (String opt : opts) {
                opt = opt.trim();
                if (opt.startsWith("m:")) {
                    try {
                        maxFrags = Integer.parseInt(opt.substring(2));
                    } catch (Exception ex) { }
                } else if (opt.startsWith("z:")) {
                    try {
                        fragSize = Integer.parseInt(opt.substring(2));
                    } catch (Exception ex) { }
                } else if (opt.startsWith("s:")) {
                    start = opt.substring(2);
                } else if (opt.startsWith("e:")) {
                    end = opt.substring(2);
                } else if (opt.startsWith("f:")) {
                    fragSep = opt.substring(2);
                } else if (opt.startsWith("jh:")) {
                    String v = opt.substring(3);
                    if ("n".equals(v)) {
                        joinHi = false;
                    }
                } else if (opt.startsWith("jf:")) {
                    String v = opt.substring(3);
                    if ("n".equals(v)) {
                        joinFrags = false;
                    }
                }
            }
        }
    }

    private String frags2string(TextFragment[] frags, HighlightOpts opts) {
        String sep = "";
        String rez = "";
        
        for (TextFragment f : frags) {
            String s = opts.joinHi ? f.toString().replaceAll(opts.end+Z_MORE_SEPS+opts.start, "$1") : f.toString();
            rez += sep + s;
            sep = opts.fragSep;
        }
        
        return rez;
    }
    
    private Analyzer getQueryAnalyzer(boolean usingSearchFor, String lang) {
        if (usingSearchFor) {
            Analyzer qa = multilingualQueryAnalyzers.get(lang);
            if (qa == null) {
                qa = new PerFieldAnalyzerWrapper(new QueryMultilingualAnalyzer(defaultAnalyzer, lang), analyzerPerField);
                multilingualQueryAnalyzers.put(lang, qa);
            }
            return qa;
        } else {
            return queryAnalyzer;
        }
    }

    private List<TemporalHit> query$(IndexReader indexReader, Node property, String qs, String graphURI, String lang, int limit, String highlight)
            throws ParseException, IOException, InvalidTokenOffsetsException {
        String textField = docDef.getField(property) != null ?  docDef.getField(property) : docDef.getPrimaryField();
        String textClause = "";               
        String langField = getDocDef().getLangField();
        
        List<String> searchForTags = Util.getSearchForTags(lang);
        boolean usingSearchFor = !searchForTags.isEmpty();
        if (usingSearchFor) {            
            for (String tag : searchForTags) {
                String tf = textField + "_" + tag;
                textClause += tf + ":" + qs + " ";
            }
        } else {
            if (this.isMultilingual && StringUtils.isNotEmpty(lang) && !lang.equals("none")) {
                textField += "_" + lang;
            }
            
            if (docDef.getField(property) != null) {
                textClause = textField + ":" + qs;
            } else {
                textClause = qs;
            }
           
            String langClause = null;
            if (langField != null) {
                langClause = StringUtils.isNotEmpty(lang) ? (!lang.equals("none") ? langField + ":" + lang : "-" + langField + ":*") : null;
            }
            if (langClause != null)
                textClause = "(" + textClause + ") AND " + langClause ;
        }
        
        String graphClause = null;
        if (graphURI != null) {
            String escaped = QueryParserBase.escape(graphURI) ;
            graphClause = getDocDef().getGraphField() + ":" + escaped ;
        }
        
        String queryString = textClause ;

        if (graphClause != null)
            queryString = "(" + queryString + ") AND " + graphClause ;
        
        Analyzer qa = getQueryAnalyzer(usingSearchFor, lang);
        Query query = parseQuery(queryString, qa) ;
        
        if ( limit <= 0 )
            limit = MAX_N ;

        log.debug("Lucene queryString: {}, parsed query: {}, limit:{}", queryString, query, limit) ;

        IndexSearcher indexSearcher = new IndexSearcher(indexReader) ;

        ScoreDoc[] sDocs = indexSearcher.search(query, limit).scoreDocs ;
        
        if (highlight != null) {
            return highlightResults(sDocs, indexSearcher, query, textField, highlight, usingSearchFor);
        } else {
            return simpleResults(sDocs, indexSearcher, query, textField);
        }
    }

    @Override
    public EntityDefinition getDocDef() {
        return docDef ;
    }

    private Node entryToNode(String v) {
        // TEMP
        return NodeFactoryExtra.createLiteralNode(v, null, null) ;
    }
}
