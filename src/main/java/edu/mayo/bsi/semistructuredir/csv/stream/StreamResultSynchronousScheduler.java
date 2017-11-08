package edu.mayo.bsi.semistructuredir.csv.stream;

import edu.mayo.bsi.semistructuredir.csv.cr.BlockingStreamCollectionReader;

import java.util.*;
import java.util.concurrent.ExecutorService;
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
    public void run() {
        // Responses can be associated with multiple jobs in the case of cached jobs
        Map<String, NLPStreamResponse<T>> localFutureCache = new HashMap<>();
        List<NLPStreamResponse<T>> BARRIERS = new LinkedList<>();
        while (hasNext()) {
            S nextRecord = getNext();
            UUID jobUID = UUID.randomUUID();
            T cached = getCachedResult(nextRecord);
            if (cached != null) {
                NLPStreamResponse<T> future = new NLPStreamResponse<>(jobUID);
                future.addResponseConsumer(item -> EXECUTOR.submit(() -> StreamResultSynchronousScheduler.this.complete(nextRecord, item)));
                future.setResp(cached, NLPStreamResponse.RESPONSE_STATES.COMPLETED_NORMALLY);
                BARRIERS.add(future);
            } else {
                String data = getCTakesDocumentText(nextRecord);
                NLPStreamResponse<T> future;
                if (localFutureCache.containsKey(data)) {
                    future = localFutureCache.get(data);
                } else {
                    future = BlockingStreamCollectionReader.submitMessage(jobUID, data);
                    localFutureCache.put(data, future);
                }
                future.addResponseConsumer(item -> EXECUTOR.submit(() -> StreamResultSynchronousScheduler.this.complete(nextRecord, item)));
            }
        }
        for (NLPStreamResponse<T> cachedResp : BARRIERS) {
            EXECUTOR.submit(cachedResp::runFinalizers);
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
}
