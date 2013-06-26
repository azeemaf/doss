package doss.db;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryDb implements DALDb {
    Map<Long,Long> map = new ConcurrentHashMap<Long,Long>();
    Set<Long> deleted = Collections.newSetFromMap(new ConcurrentHashMap<Long,Boolean>());
    
    @Override
    public long locate(long blobId) throws NoSuchBlobException {
        Long offset = map.get(blobId);
        if (offset == null || deleted.contains(blobId)) {
            throw new NoSuchBlobException(blobId);
        } else {
            return offset;
        }
    }

    @Override
    public void remember(long blobId, long offset) {
        map.put(blobId, offset);
    }

    @Override
    public void delete(long blobId) {
        deleted.add(blobId);
    }

}
