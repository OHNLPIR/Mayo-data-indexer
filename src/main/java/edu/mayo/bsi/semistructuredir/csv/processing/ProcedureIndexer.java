package edu.mayo.bsi.semistructuredir.csv.processing;

import com.google.common.cache.Cache;
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

public class ProcedureIndexer extends StreamResultSynchronousScheduler<CSVRecord, Set<String>> {

    private final List<CSVRecord> RECORDS;
    private DateFormat DF = new SimpleDateFormat("yyyy-MM-dd");

    public ProcedureIndexer(ExecutorService executor, List<CSVRecord> records) {
        super(executor);
        this.RECORDS = records;
        this.DF.setTimeZone(TimeZone.getTimeZone("GMT")); // OMOP Indexer standardizes to this timezone
    }

    @Override
    protected Set<String> getCachedResult(CSVRecord record) {
        Set<String> codes;
        String procCodeCPT = record.get(PROC_HEADERS.CODE.getIndex()).trim();
        Cache<String, Set<String>> cuiLookupCache = Main.SRC_CODE_TO_CUI_CACHE.get(Optional.of(UMLSLookup.UMLSSourceVocabulary.CPT));
        try {
            codes = cuiLookupCache.get(procCodeCPT, () -> {
                try {
                    Collection<String> ret = UMLSLookup.getUMLSCuiForSourceVocab(UMLSLookup.UMLSSourceVocabulary.CPT, procCodeCPT);
                    if (ret.size() == 0) {
                        return Collections.emptySet();
                    } else {
                        return new HashSet<>(ret);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    return Collections.emptySet();
                }
            });
        } catch (ExecutionException e) {
            e.printStackTrace();
            return null;
        }
        if (codes == null || codes.size() == 0) {
            return null;
        } else {
            return codes;
        }
    }

    @Override
    protected void complete(CSVRecord record, Set<String> codes) {
        JSONObject proc = new JSONObject();
        String personID = record.get(PROC_HEADERS.MCN.getIndex()).trim();
        proc.put("person_id", personID);
        String procDateRaw = record.get(PROC_HEADERS.DATE.getIndex()).trim();
        Date procDate = null;
        try {
            procDate = DF.parse(procDateRaw);
            proc.put("procedure_date", procDate.getTime());
        } catch (ParseException e) {
            e.printStackTrace(); // TODO better error logging
        }

        String procDescCPT = record.get(PROC_HEADERS.DESC.getIndex()).trim();
        proc.put("procedure_raw", procDescCPT);
        if (codes != null) {
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
            proc.put("procedure_cui", StringUtils.join(cuis, " "));
            proc.put("procedure_SNOMEDCT_US_code", StringUtils.join(snomed, " "));
            proc.put("procedure_SNOMEDCT_US_text", StringUtils.join(snomedText, " "));
        }
        proc.put("procedure_" + record.get(PROC_HEADERS.CODE_VOCAB.getIndex()).trim().replaceAll("[- ]", "") + "_code", record.get(PROC_HEADERS.CODE.getIndex()).trim());
        proc.put("procedure_source_coding_vocab", record.get(PROC_HEADERS.CODE_VOCAB.getIndex()).trim());
        if (procDate != null && Main.PERSON_DOB_LOOKUP.containsKey(personID.toLowerCase())) {
            proc.put("procedure_age", procDate.getTime() - Main.PERSON_DOB_LOOKUP.get(personID.toLowerCase()));
        }
        ElasticsearchIndexingThread.schedule(proc, "Procedure", personID, personID);
    }

    @Override
    protected String getCTakesDocumentText(CSVRecord record) {
        String ret = record.get(PROC_HEADERS.DESC.getIndex());
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


    private enum PROC_HEADERS {
        MCN(0),
        DATE(1),
        CODE(2),
        DESC(3),
        CODE_VOCAB(4);

        private final int index;

        PROC_HEADERS(int i) {
            this.index = i;
        }

        public int getIndex() {
            return index;
        }
    }
}
