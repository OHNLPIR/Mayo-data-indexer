package edu.mayo.bsi.semistructuredir.csv.stream;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import edu.mayo.bsi.semistructuredir.csv.cr.BlockingStreamCollectionReader;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO document
public abstract class StreamResultSynchronousScheduler<S, T> extends Thread {

    private final ExecutorService EXECUTOR;
    private final AtomicBoolean COMPLETE;

    protected StreamResultSynchronousScheduler(ExecutorService executor) {
        this.EXECUTOR = executor;
        this.COMPLETE = new AtomicBoolean(false);
    }

    @Override
    public void run() { // TODO: This works but is somewhat inefficient as you have to wait for entire collection to be read in due to caching, maybe best to skip
        LinkedList<NLPStreamResponse<T>> scheduledResults = new LinkedList<>();
        Map<UUID, S> uIDToRecordMap = new HashMap<>();
        // Responses can be associated with multiple jobs in the case of cached jobs
        Multimap<NLPStreamResponse<T>, UUID> streamRespToJobUIDs = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);
        Map<String, NLPStreamResponse<T>> localFutureCache = new HashMap<>();
        List<NLPStreamResponse<T>> BARRIERS = new LinkedList<>();
        while (hasNext()) {
            S nextRecord = getNext();
            UUID jobUID = UUID.randomUUID();
            uIDToRecordMap.put(jobUID, nextRecord);
            T cached = getCachedResult(nextRecord);
            if (cached != null) {
                NLPStreamResponse<T> future = new NLPStreamResponse<>(jobUID);
                future.addResponseConsumer((item) -> complete(nextRecord, item));
                future.setResp(cached, NLPStreamResponse.RESPONSE_STATES.COMPLETED_NORMALLY);
                BARRIERS.add(future);
                scheduledResults.add(future);
            } else {
                String data = getCTakesDocumentText(nextRecord);
                NLPStreamResponse<T> resultFuture;
                if (localFutureCache.containsKey(data)) {
                    resultFuture = localFutureCache.get(data);
                } else {
                    resultFuture = BlockingStreamCollectionReader.submitMessage(jobUID, data);
                    localFutureCache.put(data, resultFuture);
                }
                resultFuture.addResponseConsumer((item) -> complete(nextRecord, item));
                streamRespToJobUIDs.put(resultFuture, jobUID);
                scheduledResults.add(resultFuture);
            }
        }
        for (NLPStreamResponse<T> cachedResp : BARRIERS) {
            cachedResp.runFinalizers();
        }
        synchronized (COMPLETE) {
            COMPLETE.set(true);
            COMPLETE.notifyAll();
        }
    }

    protected abstract T getCachedResult(S record);

    protected abstract void complete(S record, T item);

    protected abstract String getCTakesDocumentText(S record);

    protected abstract boolean hasNext();

    protected abstract S getNext();

    public void waitComplete() throws InterruptedException {
        synchronized (COMPLETE) {
            while (!COMPLETE.get()) {
                COMPLETE.wait(1000);
            }
        }
    }
}
