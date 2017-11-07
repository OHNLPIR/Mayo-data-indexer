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

public class LabIndexer extends StreamResultSynchronousScheduler<CSVRecord, Set<String>> {

    private UMLSLookup LOOKUP;
    private final List<CSVRecord> RECORDS;
    private DateFormat DF = new SimpleDateFormat("yyyy-MM-dd");

    public LabIndexer(ExecutorService executor, List<CSVRecord> records) {
        super(executor);
        this.RECORDS = records;
        this.LOOKUP = UMLSLookup.newLookup();
        this.DF.setTimeZone(TimeZone.getTimeZone("GMT")); // OMOP Indexer standardizes to this timezone
    }

    @Override
    protected Set<String> getCachedResult(CSVRecord record) {
        return null; // Will never have a cached result
    }

    @Override
    protected void complete(CSVRecord record, Set<String> codes) {
        JSONObject lab = new JSONObject();
        String personID = record.get(LAB_HEADERS.MCN.getIndex()).trim();
        lab.put("person_id", personID);
        String labDateRaw = record.get(LAB_HEADERS.RESULT_DATE.getIndex()).trim();
        Date labDate;
        try {
            labDate = DF.parse(labDateRaw);
            lab.put("lab_date", labDate.getTime());
            if (Main.PERSON_DOB_LOOKUP.containsKey(personID.toLowerCase())) {
                lab.put("lab_age", labDate.getTime() - Main.PERSON_DOB_LOOKUP.get(personID.toLowerCase()));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        lab.put("lab_code", record.get(LAB_HEADERS.CODE.getIndex()).trim());
        String labDesc = record.get(LAB_HEADERS.DESC.getIndex()).trim();
        lab.put("lab_raw", labDesc);
        if (codes != null) {
            HashSet<String> cuis = new HashSet<>();
            cuis.addAll(codes);
            HashSet<String> snomed = new HashSet<>();
            HashSet<String> snomedText = new HashSet<>();
            for (String s : codes) {
                try {
                    snomed.addAll(Main.CUI_TO_SRC_CODE.get(Optional.of(UMLSLookup.UMLSSourceVocabulary.SNOMEDCT_US)).get(s, () -> {
                        try {
                            return new HashSet<>(LOOKUP.getSourceCodesForVocab(UMLSLookup.UMLSSourceVocabulary.SNOMEDCT_US, s));
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
                            return new HashSet<>(LOOKUP.getSourceTermPreferredText(UMLSLookup.UMLSSourceVocabulary.SNOMEDCT_US, s));
                        } catch (SQLException e) {
                            e.printStackTrace();
                            return Collections.emptySet();
                        }
                    })));
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            lab.put("lab_cui", StringUtils.join(cuis, " "));
            lab.put("lab_SNOMEDCT_US_code", StringUtils.join(snomed, " "));
            lab.put("lab_SNOMEDCT_US_text", StringUtils.join(snomedText, " "));
        }
        String value = record.get(LAB_HEADERS.VALUE.getIndex()).trim();
        lab.put("lab_value_text", value);
        Double[] parsedValues = Main.parseNumeric(value);
        if (parsedValues[0] != null) lab.put("lab_value_low", parsedValues[0]);
        if (parsedValues[1] != null) lab.put("lab_value_high", parsedValues[0]);
        lab.put("unit", record.get(LAB_HEADERS.UNIT.getIndex()).trim());
        ElasticsearchIndexingThread.schedule(lab, "LabTest", personID, personID);
    }

    @Override
    protected String getCTakesDocumentText(CSVRecord record) {
        String ret = record.get(LAB_HEADERS.DESC.getIndex());
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


    private enum LAB_HEADERS {
        MCN(0),
        RESULT_DATE(1),
        CODE(2),
        DESC(3),
        VALUE(5),
        UNIT(6);

        private final int index;

        LAB_HEADERS(int i) {
            this.index = i;
        }

        public int getIndex() {
            return index;
        }
    }

}
