package edu.mayo.bsi.semistructuredir.csv.processing;

import edu.mayo.bsi.semistructuredir.csv.Main;
import edu.mayo.bsi.semistructuredir.csv.elasticsearch.ElasticsearchIndexingThread;
import edu.mayo.bsi.semistructuredir.csv.stream.StreamResultSynchronousScheduler;
import edu.mayo.bsi.umlsvts.UMLSLookup;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class DiagnosisIndexer extends StreamResultSynchronousScheduler<CSVRecord, Set<String>> {

    private final List<CSVRecord> RECORDS;
    private DateFormat DF = new SimpleDateFormat("yyyy-MM-dd");

    public DiagnosisIndexer(ExecutorService executor, List<CSVRecord> records) {
        super(executor);
        this.RECORDS = records;
        this.DF.setTimeZone(TimeZone.getTimeZone("GMT")); // OMOP Indexer standardizes to this timezone
    }

    @Override
    protected Set<String> getCachedResult(CSVRecord record) {
        return null; // Will never have a cached cTAKES result
    }

    @Override
    protected void complete(CSVRecord record, Set<String> codes) {
        JSONObject diag = new JSONObject();
        String personID = record.get(DIAG_HEADERS.MCN.getIndex()).trim();
        diag.put("person_id", personID);
        String diagDateRaw = record.get(DIAG_HEADERS.DATE.getIndex()).trim();
        Date diagDate;
        try {
            diagDate = DF.parse(diagDateRaw);
            diag.put("diagnosis_date", diagDate.getTime());
            if (Main.PERSON_DOB_LOOKUP.containsKey(personID.toLowerCase())) {
                diag.put("diagnosis_age", diagDate.getTime() - Main.PERSON_DOB_LOOKUP.get(personID.toLowerCase()));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        diag.put("diagnosis_raw", record.get(DIAG_HEADERS.DESC.getIndex()).trim());
        if (codes != null) {
            Main.SRC_CODE_TO_CUI_CACHE.get(Optional.<UMLSLookup.UMLSSourceVocabulary>empty()).put(record.get(DIAG_HEADERS.DESC.getIndex()).trim(), codes);
            HashSet<String> cuis = new HashSet<>();
            cuis.addAll(codes);
            HashSet<String> snomed = new HashSet<>();
            HashSet<String> snomedText = new HashSet<>();
            for (String s : codes) {
                try {
                    snomed.addAll(Main.CUI_TO_SRC_CODE.get(Optional.of(UMLSLookup.UMLSSourceVocabulary.SNOMEDCT_US)).get(s, () -> {
                        try {
                            return new HashSet<>(UMLSLookup.getSourceCodesForVocab(UMLSLookup.UMLSSourceVocabulary.SNOMEDCT_US, s));
                        } catch (SQLException e) {
                            e.printStackTrace();
                            return Collections.emptySet();
                        }
                    }));
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            for (String s : snomed) {
                try {
                    snomedText.addAll((Main.SRC_CODE_TO_SRC_VOCAB.get(Optional.of(UMLSLookup.UMLSSourceVocabulary.SNOMEDCT_US)).get(s, () -> {
                        try {
                            return new HashSet<>(UMLSLookup.getSourceTermPreferredText(UMLSLookup.UMLSSourceVocabulary.SNOMEDCT_US, s));
                        } catch (SQLException e) {
                            e.printStackTrace();
                            return Collections.emptySet();
                        }
                    })));
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            diag.put("diagnosis_cui", StringUtils.join(cuis, " "));
            diag.put("diagnosis_SNOMEDCT_US_code", StringUtils.join(snomed, " "));
            diag.put("diagnosis_SNOMEDCT_US_text", StringUtils.join(snomedText, " "));
        }
        diag.put("diagnosis_" + record.get(DIAG_HEADERS.CODE_VOCAB.getIndex()).trim().replaceAll("[- ]", "") + "_code", record.get(DIAG_HEADERS.CODE.getIndex()).trim());
        diag.put("diagnosis_source_coding_vocab", record.get(DIAG_HEADERS.CODE_VOCAB.getIndex()).trim());
        ElasticsearchIndexingThread.schedule(diag, "Diagnosis", personID, personID);
    }


    @Override
    protected String getCTakesDocumentText(CSVRecord record) {
        String ret = record.get(DIAG_HEADERS.DESC.getIndex());
        if (ret != null) {
            return ret.trim();
        } else {
            return "";
        }
    }

    @Override
    protected boolean hasNext() {
        return !RECORDS.isEmpty();
    }

    @Override
    protected CSVRecord getNext() {
        return RECORDS.remove(0);
    }

    private enum DIAG_HEADERS {
        MCN(0),
        DATE(1),
        CODE(2),
        DESC(3),
        CODE_VOCAB(4);

        private final int index;

        DIAG_HEADERS(int i) {
            this.index = i;
        }

        public int getIndex() {
            return index;
        }
    }

}
