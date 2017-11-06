package edu.mayo.bsi.semistructuredir.csv.cr;

import org.apache.ctakes.typesystem.type.structured.DocumentID;
import org.apache.uima.UIMAFramework;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.JCasCollectionReader_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.Level;
import org.apache.uima.util.Progress;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-Safe: A collection reader implementation for UIMA that supports streamed (live) input; will continuously wait until an
 * element is available for processing in a shared queue and block otherwise, <br>
 * jobs can be submitted via {@link #submitMessage(String, String)}. <br>
 * <br>
 * <p>
 * <p>
 * Streams will be shutdown upon calls to {@link #shutdownQueue()}, at which point the queue will cease accepting new items
 * and all consumer threads will quit after processing the final document in the queue.
 */
public class BlockingStreamCollectionReader extends JCasCollectionReader_ImplBase {

    private static final BlockingDeque<Job> PROCESSING_QUEUE = new LinkedBlockingDeque<>(1000);
    private static final AtomicBoolean STREAM_OPEN = new AtomicBoolean(true);
    private Job CURRENT_WORK = null;

    @Override
    public void getNext(JCas jCas) throws IOException, CollectionException {
        UIMAFramework.getLogger(BlockingStreamCollectionReader.class).log(Level.INFO, "Received Item");
        jCas.setDocumentText(CURRENT_WORK.text.trim());
        DocumentID id = new DocumentID(jCas);
        id.setDocumentID(CURRENT_WORK.id);
        id.addToIndexes();
        UIMAFramework.getLogger(BlockingStreamCollectionReader.class).log(Level.INFO, "Finished Receiving Item " + jCas.getDocumentText());
    }

    @Override
    public boolean hasNext() throws IOException, CollectionException {
        synchronized (PROCESSING_QUEUE) {
            while ((CURRENT_WORK = PROCESSING_QUEUE.pollFirst()) == null && STREAM_OPEN.get()) {
                try {
                    PROCESSING_QUEUE.wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return true;
        }
    }

    @Override
    public Progress[] getProgress() {
        return new Progress[0];
    }

    public static void submitMessage(String uID, String msg) {
        if (!STREAM_OPEN.get()) {
            throw new IllegalStateException("Trying to submit a message for processing to a closed queue");
        } else {
            boolean successfulSubmit = false;
            while (!successfulSubmit) {
                try {
                    successfulSubmit = PROCESSING_QUEUE.offer(new Job(msg, uID), 30, TimeUnit.MINUTES);
                    synchronized (PROCESSING_QUEUE) {
                        PROCESSING_QUEUE.notifyAll();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static boolean isShutdown() {
        return !STREAM_OPEN.get();
    }

    public static boolean shouldTerminateThread() {
        return isShutdown() && PROCESSING_QUEUE.size() == 0;
    }

    public static void shutdownQueue() {
        if (!STREAM_OPEN.getAndSet(false)) {
            throw new IllegalStateException("Shutting down an already shut down queue");
        }
        synchronized (PROCESSING_QUEUE) {
            PROCESSING_QUEUE.notifyAll();
        }
    }


    private static class Job {
        String text;
        String id;

        Job(String text, String id) {
            this.text = text;
            this.id = id;
        }
    }
}
