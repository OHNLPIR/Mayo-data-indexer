package edu.mayo.bsi.semistructuredir.csv.stream;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import edu.mayo.bsi.semistructuredir.csv.cr.BlockingStreamCollectionReader;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO document
public abstract class StreamResultSynchronousScheduler<S, T> extends Thread {

    private final ExecutorService executor;
    private final AtomicBoolean COMPLETE;

    protected StreamResultSynchronousScheduler(ExecutorService executor) {
        this.executor = executor;
        this.COMPLETE = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        LinkedList<NLPStreamResponse<T>> results = new LinkedList<>();
        Map<UUID, S> uIDToRecordMap = new HashMap<>();
        // Responses can be associated with multiple jobs in the case of cached jobs
        Multimap<NLPStreamResponse<T>, UUID> streamRespToJobUIDs = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);
        Map<String, NLPStreamResponse<T>> localFutureCache = new HashMap<>();
        while (hasNext()) {
            S nextRecord = getNext();
            UUID jobUID = UUID.randomUUID();
            uIDToRecordMap.put(jobUID, nextRecord);
            T cached = getCachedResult(nextRecord);
            if (cached != null) {
                NLPStreamResponse<T> future = new NLPStreamResponse<>(jobUID);
                future.setResp(cached, NLPStreamResponse.RESPONSE_STATES.COMPLETED_NORMALLY);
                results.add(future);
            } else {
                String data = getCTakesDocumentText(nextRecord);
                NLPStreamResponse<T> resultFuture;
                if (localFutureCache.containsKey(data)) {
                    resultFuture = localFutureCache.get(data);
                } else {
                    resultFuture = BlockingStreamCollectionReader.submitMessage(jobUID, data);
                    localFutureCache.put(data, resultFuture);
                }
                streamRespToJobUIDs.put(resultFuture, jobUID);
                results.add(resultFuture);
            }
        }
        for (NLPStreamResponse<T> future : results) {
            for (UUID uid : streamRespToJobUIDs.get(future)) {
                executor.submit(() -> {
                    T item = future.getResp();
                    complete(uIDToRecordMap.remove(uid), item);
                });
            }
        }
        // TODO this doesn't wait for all subtasks to finish above, but is close enough for our purposes
        synchronized (COMPLETE) {
            COMPLETE.getAndSet(true);
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
