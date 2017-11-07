package edu.mayo.bsi.semistructuredir.csv.stream;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class NLPStreamResponseCache {
    public static final Map<UUID, NLPStreamResponse<Set<String>>> CACHE = new ConcurrentHashMap<>();
}
