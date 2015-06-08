package drafter.rdf;

import java.util.HashMap;
import java.util.Map;
import org.openrdf.model.URI;

public class URIMapper {
    private final Map<String, String> mapping;

    private URIMapper(Map<String, String> mapping) {
        this.mapping = mapping;
    }

    public String mapURIString(String uri) {
        String mapped = this.mapping.get(uri);
        return mapped == null ? uri : mapped;
    }

    public static <U extends URI> URIMapper create(Map<U, U> mapping) {
        Map<String, String> map = new HashMap<>();
        for(Map.Entry<U, U> kvp : mapping.entrySet()) {
            map.put(kvp.getKey().stringValue(), kvp.getValue().stringValue());
        }
        return new URIMapper(map);
    }
}
