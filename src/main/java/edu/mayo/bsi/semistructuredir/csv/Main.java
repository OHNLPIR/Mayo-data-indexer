package edu.mayo.bsi.semistructuredir.csv;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.googlecode.clearnlp.io.FileExtFilter;
import edu.mayo.bsi.semistructuredir.csv.cr.BlockingStreamCollectionReader;
import edu.mayo.bsi.semistructuredir.csv.elasticsearch.ElasticsearchIndexingThread;
import edu.mayo.bsi.semistructuredir.csv.pipelines.StreamingCTakesPipelineThread;
import edu.mayo.bsi.semistructuredir.csv.processing.DiagnosisIndexer;
import edu.mayo.bsi.semistructuredir.csv.processing.LabIndexer;
import edu.mayo.bsi.semistructuredir.csv.processing.ProcedureIndexer;
import edu.mayo.bsi.umlsvts.UMLSLookup;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.uima.resource.ResourceInitializationException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO ctakes stream pattern should really be submit jobs->barrier but re-engineering cost at this point is not really worth it
// Still want to change at some point though
public class Main extends Thread {

    private static Integer NUM_CSV_CORES;
    // Initialization of items is done sync
    public static Map<Optional<UMLSLookup.UMLSSourceVocabulary>, Cache<String, Set<String>>> SRC_CODE_TO_CUI_CACHE = new HashMap<>();
    public static Map<Optional<UMLSLookup.UMLSSourceVocabulary>, Cache<String, Set<String>>> CUI_TO_SRC_CODE = new HashMap<>();
    public static Map<Optional<UMLSLookup.UMLSSourceVocabulary>, Cache<String, Set<String>>> SRC_CODE_TO_SRC_VOCAB = new HashMap<>();
    public static Map<String, Long> PERSON_DOB_LOOKUP = new HashMap<>();
    private static ExecutorService CSV_THREAD_POOL;
    private static final File ROOT_DIR = new File("/infodev1/phi-data/EHR/BioBank/"); // TODO remove hardcoding


    public static void main(String... args) throws IOException, SQLException, ParseException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, ResourceInitializationException, InterruptedException {
        // Initialize System
        System.setProperty("vocab.src.dir", System.getProperty("user.dir"));
        for (UMLSLookup.UMLSSourceVocabulary vocab : UMLSLookup.UMLSSourceVocabulary.values()) {
            SRC_CODE_TO_CUI_CACHE.put(Optional.of(vocab), CacheBuilder.newBuilder().expireAfterAccess(600, TimeUnit.SECONDS).build());
            CUI_TO_SRC_CODE.put(Optional.of(vocab), CacheBuilder.newBuilder().expireAfterAccess(600, TimeUnit.SECONDS).build());
            SRC_CODE_TO_SRC_VOCAB.put(Optional.of(vocab), CacheBuilder.newBuilder().expireAfterAccess(600, TimeUnit.SECONDS).build());
        }
        SRC_CODE_TO_CUI_CACHE.put(Optional.empty(), CacheBuilder.newBuilder().expireAfterAccess(600, TimeUnit.SECONDS).build());
        CUI_TO_SRC_CODE.put(Optional.empty(), CacheBuilder.newBuilder().expireAfterAccess(600, TimeUnit.SECONDS).build());
        SRC_CODE_TO_SRC_VOCAB.put(Optional.empty(), CacheBuilder.newBuilder().expireAfterAccess(600, TimeUnit.SECONDS).build());
        // Execute pipeline with multiple threads and verify some parameters
        // - Detect number of cores to use
        if (System.getProperty("pipeline.threads.deserialization") != null) {
            NUM_CSV_CORES = Integer.valueOf(System.getProperty("pipeline.threads.deserialization"));
            System.out.println("Running pipeline with " + NUM_CSV_CORES + " threads for deserialization");
        } else {
            NUM_CSV_CORES = (int) Math.round(Runtime.getRuntime().availableProcessors() * 0.25); // Should never be high enough to actually cause an overflow
            if (NUM_CSV_CORES == 0) NUM_CSV_CORES = 1;
            System.out.println("-Dpipeline.threads.deserialization not set, running pipeline with " + NUM_CSV_CORES + " threads for deserialization based on system configuration");
            System.setProperty("pipeline.threads", NUM_CSV_CORES + "");
        }
        int numNLPCores;
        if (System.getProperty("pipeline.threads.nlp") != null) {
            numNLPCores = Integer.valueOf(System.getProperty("pipeline.threads.nlp"));
            System.out.println("Running pipeline with " + numNLPCores + " threads for NLP");
        } else {
            numNLPCores = (int) Math.round(Runtime.getRuntime().availableProcessors() * 0.25); // Should never be high enough to actually cause an overflow
            if (numNLPCores == 0) numNLPCores = 1;
            System.out.println("-Dpipeline.threads.nlp not set, running pipeline with " + numNLPCores + " threads for NLP based on system configuration");
            System.setProperty("pipeline.threads", numNLPCores + "");
        }
        int numIndexingCores;
        if (System.getProperty("indexing.threads") != null) {
            numIndexingCores = Integer.valueOf(System.getProperty("indexing.threads"));
            System.out.println("Running indexing with " + numIndexingCores + " threads");
        } else {
            numIndexingCores = 1; // Should never be high enough to actually cause an overflow
            System.out.println("-Dindexing.threads not set, running indexing with " + numIndexingCores + " threads based on system configuration");
            System.setProperty("indexing.threads", numIndexingCores + "");
        }
        ExecutorService cTakesExecutor = Executors.newFixedThreadPool(numNLPCores, new ThreadFactoryBuilder().setNameFormat("SemiStructuredIR-cTAKES-Pipeline-Thread-%d").build());
        for (int i = 0; i < numNLPCores; i++) {
            cTakesExecutor.submit(new StreamingCTakesPipelineThread());
        }
        ExecutorService esExecutor = Executors.newFixedThreadPool(numIndexingCores, new ThreadFactoryBuilder().setNameFormat("SemiStructuredIR-Elasticsearch-Indexer-Thread-%d").build());
        for (int i = 0; i < numIndexingCores; i++) {
            esExecutor.submit(new ElasticsearchIndexingThread());
        }
        // Two separate pools but they will generally exclude each other from running at the same time
        CSV_THREAD_POOL = Executors.newFixedThreadPool(NUM_CSV_CORES, new ThreadFactoryBuilder().setNameFormat("SemiStructuredIR-Processor-Thread-%d").build());
        loadDemographics();
        loadLabs();
        loadProcedures();
        loadDiagnosis();
        CSV_THREAD_POOL.shutdown();
        CSV_THREAD_POOL.awaitTermination(10000, TimeUnit.DAYS);
        StreamingCTakesPipelineThread.shutdown();
        ElasticsearchIndexingThread.shutdown();
        cTakesExecutor.shutdown();
        cTakesExecutor.awaitTermination(10000, TimeUnit.DAYS);
        esExecutor.shutdown();
        esExecutor.awaitTermination(10000, TimeUnit.DAYS);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private static void loadDemographics() throws IOException, SQLException, ParseException, ResourceInitializationException {
        File f = new File(ROOT_DIR, "patient_dart_output.csv");
        CSVParser parser = CSVParser.parse(f, StandardCharsets.UTF_8, CSVFormat.DEFAULT.withFirstRecordAsHeader());
        List<CSVRecord> records = parser.getRecords();
        final AtomicInteger SENTINEL = new AtomicInteger(NUM_CSV_CORES);
        for (List<CSVRecord> record : Lists.partition(records, (int) Math.ceil(records.size() / (double) NUM_CSV_CORES))) {
            CSV_THREAD_POOL.submit(new DemographicsIndexer(record, SENTINEL), new ThreadFactoryBuilder().setNameFormat("SemiStructuredIR-Demographics-Processing-Thread-%d").build());
        }
        synchronized (SENTINEL) {
            while (SENTINEL.get() != 0) {
                try {
                    SENTINEL.wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private static void loadLabs() throws IOException, SQLException, ParseException, ResourceInitializationException {
        for (File f : new File(ROOT_DIR, "lab").listFiles(new FileExtFilter("csv"))) { // TODO
            CSVParser parser = CSVParser.parse(f, StandardCharsets.UTF_8, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            List<CSVRecord> records = parser.getRecords();
            LabIndexer indexer = new LabIndexer(CSV_THREAD_POOL, records);
            indexer.run();
        }
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private static void loadDiagnosis() throws IOException, SQLException, ParseException, ResourceInitializationException {
        for (File f : new File(ROOT_DIR, "dx").listFiles(new FileExtFilter("csv"))) { // TODO
            CSVParser parser = CSVParser.parse(f, StandardCharsets.UTF_8, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            List<CSVRecord> records = parser.getRecords();
            DiagnosisIndexer indexer = new DiagnosisIndexer(CSV_THREAD_POOL, records);
            indexer.run();
        }
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private static void loadProcedures() throws IOException, SQLException, ParseException, ResourceInitializationException {
        for (File f : new File(ROOT_DIR, "proc").listFiles(new FileExtFilter("csv"))) { // TODO
            CSVParser parser = CSVParser.parse(f, StandardCharsets.UTF_8, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            List<CSVRecord> records = parser.getRecords();
            ProcedureIndexer indexer = new ProcedureIndexer(CSV_THREAD_POOL, records);
            indexer.run();
        }
    }

    private static class DemographicsIndexer extends Thread {

        final List<CSVRecord> RECORDS;
        final AtomicInteger SENTINEL;
        private DateFormat DF = new SimpleDateFormat("yyyy-MM-dd");

        private DemographicsIndexer(List<CSVRecord> records, AtomicInteger sentinel) {
            this.RECORDS = records;
            this.SENTINEL = sentinel;
            this.DF.setTimeZone(TimeZone.getTimeZone("GMT")); // OMOP Indexer standardizes to this timezone
        }

        @Override
        public void run() {
            for (CSVRecord record : RECORDS) {
                JSONObject diag = new JSONObject();
                String personID = record.get(DEM_HEADERS.MCN.getIndex()).trim();
                diag.put("person_id", personID);
                String diagDateRaw = record.get(DEM_HEADERS.DOB.getIndex()).trim();
                Date diagDate;
                try {
                    diagDate = DF.parse(diagDateRaw);
                    diag.put("date_of_birth", diagDate.getTime());
                    PERSON_DOB_LOOKUP.put(personID.toLowerCase(), diagDate.getTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                diag.put("city", record.get(DEM_HEADERS.CITY.getIndex()).trim());
                diag.put("geo_code", record.get(DEM_HEADERS.GEO_CODE.getIndex()).trim());
                diag.put("gender", record.get(DEM_HEADERS.GENDER.getIndex()).trim());
                diag.put("ethnicity", record.get(DEM_HEADERS.ETHNICITY.getIndex()).trim());
                diag.put("race", record.get(DEM_HEADERS.RACE.getIndex()).trim());
                ElasticsearchIndexingThread.schedule(diag, "Person", null, null);
            }
            SENTINEL.decrementAndGet();
            synchronized (SENTINEL) {
                SENTINEL.notifyAll();
            }
        }
    }

    public static Double[] parseNumeric(String value) {
        Double[] ret = new Double[]{null, null};
        value = value.replaceAll("[\t ]", ""); // TODO doesn't handle negative values...like at all (probably not an issue but...)
        String[] splitRanges = value.split("-");
        if (splitRanges.length == 0) {
            return ret;
        }
        if (splitRanges.length > 1) { // Is possibly a range
            if (splitRanges[0].matches("[-+]?\\d+(\\.\\d+)?")) {
                ret[0] = Double.valueOf(splitRanges[0]);
                if (splitRanges[1].matches("[-+]?\\d+(\\.\\d+)?")) {
                    ret[1] = Double.valueOf(splitRanges[1]);
                } else {
                    Pattern p = Pattern.compile("([-+]?\\d+(\\.\\d+))?");
                    Matcher m = p.matcher(splitRanges[1]);
                    if (m.find()) {
                        ret[1] = Double.valueOf(m.group());
                    } else {
                        ret[1] = ret[0]; // TODO check validity
                    }
                }
            }
        } else { // Is possibly a single number TODO support LTE/GTE vs just LT/GT
            String withoutComp = splitRanges[0].replaceAll("[<>=]", "");
            if (splitRanges[0].startsWith("<")) {
                if (withoutComp.matches("[-+]?\\d+(\\.\\d+)?")) {
                    double val = Double.valueOf(withoutComp);
                    double min = Long.MIN_VALUE;
                    ret[0] = min;
                    ret[1] = val;
                }
            } else if (splitRanges[0].startsWith(">")) {
                if (withoutComp.matches("[-+]?\\d+(\\.\\d+)?")) {
                    double val = Double.valueOf(withoutComp);
                    double max = Long.MAX_VALUE;
                    ret[0] = val;
                    ret[1] = max;
                }
            } else {
                if (withoutComp.matches("[-+]?\\d+(\\.\\d+)?")) {
                    double val = Double.valueOf(withoutComp);
                    ret[0] = val;
                    ret[1] = val;
                }
            }
        }
        return ret;
    }

    private enum DEM_HEADERS {
        MCN(1),
        DOB(2),
        GENDER(3),
        CITY(7),
        GEO_CODE(13),
        RACE(27),
        ETHNICITY(29);

        private final int index;

        DEM_HEADERS(int i) {
            this.index = i;
        }

        public int getIndex() {
            return index;
        }
    }

}
