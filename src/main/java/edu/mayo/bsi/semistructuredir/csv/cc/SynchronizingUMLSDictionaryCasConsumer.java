package edu.mayo.bsi.semistructuredir.csv.cc;

import org.apache.ctakes.typesystem.type.refsem.UmlsConcept;
import org.apache.ctakes.typesystem.type.structured.DocumentID;
import org.apache.ctakes.typesystem.type.textsem.EntityMention;
import org.apache.ctakes.typesystem.type.textsem.EventMention;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SynchronizingUMLSDictionaryCasConsumer extends JCasConsumer_ImplBase {

    public static final ConcurrentHashMap<String, Set<String>> PROCESSED_CUIS = new ConcurrentHashMap<>(); // TODO cache expiry

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        HashSet<String> cuis = new HashSet<>();
        for (EventMention mention : JCasUtil.select(jCas, EventMention.class)) {
            for (FeatureStructure fs : mention.getOntologyConceptArr().toArray()) {
                if (!(fs instanceof UmlsConcept)) {
                    continue;
                }
                cuis.add(((UmlsConcept) fs).getCui());
            }
        }
        for (EntityMention mention : JCasUtil.select(jCas, EntityMention.class)) {
            for (FeatureStructure fs : mention.getOntologyConceptArr().toArray()) {
                if (!(fs instanceof UmlsConcept)) {
                    continue;
                }
                cuis.add(((UmlsConcept) fs).getCui());
            }
        }

        synchronized (PROCESSED_CUIS) {
            PROCESSED_CUIS.put(JCasUtil.selectSingle(jCas, DocumentID.class).getDocumentID(), cuis);
            PROCESSED_CUIS.notifyAll();
        }
    }
}
