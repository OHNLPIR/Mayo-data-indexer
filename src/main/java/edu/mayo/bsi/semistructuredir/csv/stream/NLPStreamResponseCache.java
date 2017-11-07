package edu.mayo.bsi.semistructuredir.csv.stream;


import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class NLPStreamResponseCache {
    public static final Map<UUID, NLPStreamResponse> CACHE = new ConcurrentHashMap<>();
}
