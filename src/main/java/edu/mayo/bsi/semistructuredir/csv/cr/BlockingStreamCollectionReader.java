package edu.mayo.bsi.semistructuredir.csv.cr;

import edu.mayo.bsi.semistructuredir.csv.stream.NLPStreamResponse;
import edu.mayo.bsi.semistructuredir.csv.stream.NLPStreamResponseCache;
import edu.mayo.uima.streaming.StreamingMetadata;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.JCasCollectionReader_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.Progress;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-Safe: A collection reader implementation for UIMA that supports streamed (live) input; will continuously wait until an
 * element is available for processing in a shared queue and block otherwise, <br>
 * jobs can be submitted via {@link #submitMessage(UUID, String)}. <br>
 * <br>
 * <p>
 * <p>
 * Streams will be shutdown upon calls to {@link #shutdownQueue()}, at which point the queue will cease accepting new items
 * and all consumer threads will quit after processing the final document in the queue.
 */
public class BlockingStreamCollectionReader extends JCasCollectionReader_ImplBase {

    private static final BlockingDeque<Job> PROCESSING_QUEUE = new LinkedBlockingDeque<>(1000);
    private static final AtomicBoolean STREAM_OPEN = new AtomicBoolean(true);
    private static final AtomicBoolean STREAM_READY = new AtomicBoolean(false);
    private Job CURRENT_WORK = null;

    @Override
    public void getNext(JCas jCas) throws IOException, CollectionException {
        jCas.setDocumentText(CURRENT_WORK.text.trim());
        StreamingMetadata meta = new StreamingMetadata(jCas);
        meta.setJobID(CURRENT_WORK.id.toString());
        meta.addToIndexes();
    }

    @Override
    public boolean hasNext() throws IOException, CollectionException {
        synchronized (STREAM_READY) {
            if (!STREAM_READY.get()) {
                STREAM_READY.set(true);
                STREAM_READY.notifyAll();
            }
        }
        synchronized (PROCESSING_QUEUE) {
            while ((CURRENT_WORK = PROCESSING_QUEUE.pollFirst()) == null && STREAM_OPEN.get()) {
                try {
                    PROCESSING_QUEUE.wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return !PROCESSING_QUEUE.isEmpty();
        }
    }

    @Override
    public Progress[] getProgress() {
        return new Progress[0];
    }

    public static void waitReady() {
        synchronized (STREAM_READY) {
            while (!STREAM_READY.get()) {
                try {
                    STREAM_READY.wait(1000);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    public static <T> NLPStreamResponse<T> submitMessage(UUID uID, String msg) {
        if (!STREAM_OPEN.get()) {
            throw new IllegalStateException("Trying to submit a message for processing to a closed queue");
        } else {
            boolean successfulSubmit = false;
            NLPStreamResponse<T> ret = new NLPStreamResponse<>(uID);
            Job<T> j = new Job<>(msg, uID, ret);
            while (!successfulSubmit) {
                try {
                    successfulSubmit = PROCESSING_QUEUE.offer(j, 1000, TimeUnit.MILLISECONDS);
                    synchronized (PROCESSING_QUEUE) {
                        PROCESSING_QUEUE.notifyAll();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (NLPStreamResponseCache.CACHE.put(uID, ret) != null) {
                throw new IllegalStateException("Duplicate UID!");
            }
            return ret;
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


    private static class Job<T> {
        String text;
        UUID id;
        NLPStreamResponse<T> future;

        Job(String text, UUID id, NLPStreamResponse<T> future) {
            this.text = text;
            this.id = id;
            this.future = future;
        }
    }
}
