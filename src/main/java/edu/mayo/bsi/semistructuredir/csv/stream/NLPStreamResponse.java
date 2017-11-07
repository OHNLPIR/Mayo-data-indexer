package edu.mayo.bsi.semistructuredir.csv.stream;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

// TODO document
public class NLPStreamResponse<T> {
    private final UUID jobUID;
    private T resp;
    private final AtomicInteger SENTINEL;

    public NLPStreamResponse(UUID jobUID) {
        this.SENTINEL = new AtomicInteger(RESPONSE_STATES.NOT_COMPLETED.getStateId());
        this.resp = null;
        this.jobUID = jobUID;
    }

    public synchronized void setResp(T resp, RESPONSE_STATES state) {
        if (this.resp != null) {
            throw new IllegalStateException("Attempting to set a response when one is already set!");
        }
        this.resp = resp;
        synchronized (SENTINEL) {
            SENTINEL.set(state.getStateId());
            SENTINEL.notifyAll();
        }
    }

    public int waitReady() {
        synchronized (SENTINEL) {
            while (SENTINEL.get() == 0) {
                try {
                    SENTINEL.wait(1000);
                } catch (InterruptedException ignored) {}
            }
        }
        return SENTINEL.get();
    }

    public T getResp() {
        synchronized (SENTINEL) {
            while (SENTINEL.get() == 0) {
                try {
                    SENTINEL.wait(1000);
                } catch (InterruptedException ignored) {}
            }
        }
        return resp;
    }

    public enum RESPONSE_STATES {
        NOT_COMPLETED(0),
        COMPLETED_NORMALLY(1),
        COMPLETED_ERR(2);

        private final int stateId;

        RESPONSE_STATES(int i) {
            this.stateId = i;
        }

        public int getStateId() {
            return stateId;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NLPStreamResponse<?> that = (NLPStreamResponse<?>) o;

        return jobUID != null ? jobUID.equals(that.jobUID) : that.jobUID == null;
    }

    @Override
    public int hashCode() {
        return jobUID != null ? jobUID.hashCode() : 0;
    }
}
