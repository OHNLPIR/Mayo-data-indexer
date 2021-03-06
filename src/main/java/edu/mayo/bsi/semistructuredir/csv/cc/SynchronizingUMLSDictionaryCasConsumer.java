package edu.mayo.bsi.semistructuredir.csv.cc;

import edu.mayo.bsi.semistructuredir.csv.stream.NLPStreamResponse;
import edu.mayo.bsi.semistructuredir.csv.stream.NLPStreamResponseCache;
import edu.mayo.uima.streaming.StreamingMetadata;
import org.apache.ctakes.typesystem.type.refsem.UmlsConcept;
import org.apache.ctakes.typesystem.type.textsem.EntityMention;
import org.apache.ctakes.typesystem.type.textsem.EventMention;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;

import java.util.*;

public class SynchronizingUMLSDictionaryCasConsumer extends JCasConsumer_ImplBase {


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
        StreamingMetadata meta = JCasUtil.selectSingle(jCas, StreamingMetadata.class);
        if (meta == null) {
            throw new IllegalStateException("A job appeared in the NLP stream without being read through the appropriate " +
                    "stream collection reader");
        }
        NLPStreamResponse resp = NLPStreamResponseCache.CACHE.remove(UUID.fromString(meta.getJobID()));
        if (resp != null) { // Assume we somehow did duplicate work if resp is actually null so just skip over without doing anything
            try {
                //noinspection unchecked
                resp.setResp(Collections.unmodifiableSet(cuis), NLPStreamResponse.RESPONSE_STATES.COMPLETED_NORMALLY);
                resp.runFinalizers();
            } catch (Exception e) {
                throw new AssertionError("Unmatching types between job cache and job result!", e);
            }
        }
    }
}
