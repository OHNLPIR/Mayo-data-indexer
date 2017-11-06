package edu.mayo.bsi.semistructuredir.csv.pipelines;

import edu.mayo.bsi.semistructuredir.csv.cc.SynchronizingUMLSDictionaryCasConsumer;
import edu.mayo.bsi.semistructuredir.csv.cr.BlockingStreamCollectionReader;
import org.apache.ctakes.clinicalpipeline.ClinicalPipelineFactory;
import org.apache.ctakes.dictionary.lookup2.ae.DefaultJCasTermAnnotator;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;

/**
 * 3
 */
public class StreamingCTakesPipelineThread extends Thread {

    public StreamingCTakesPipelineThread() {
        super("SemiStructuredIR-cTAKES-Pipeline-Thread");
    }

    @Override
    public void run() {
        while (!BlockingStreamCollectionReader.shouldTerminateThread()) {
            try {
                AggregateBuilder builder = new AggregateBuilder();
                builder.add(ClinicalPipelineFactory.getTokenProcessingPipeline());
                builder.add(DefaultJCasTermAnnotator.createAnnotatorDescription());
                builder.add(AnalysisEngineFactory.createEngineDescription(SynchronizingUMLSDictionaryCasConsumer.class));
                CollectionReaderDescription cr = CollectionReaderFactory.createReaderDescription(BlockingStreamCollectionReader.class);
                SimplePipeline.runPipeline(cr, builder.createAggregateDescription());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void shutdown() {
        BlockingStreamCollectionReader.shutdownQueue();
    }
}
