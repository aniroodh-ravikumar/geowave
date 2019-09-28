package org.locationtech.geowave.datastore.foundationdb.util;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class FoundationDBClientCache {
    private static FoundationDBClientCache singletonInstance;

    public static synchronized FoundationDBClientCache getInstance() {
        if (singletonInstance == null) {
            singletonInstance = new FoundationDBClientCache();
        }
        return singletonInstance;
    }


    private static class ClientKey {
        private final String directory;
        private final boolean visibilityEnabled;
        private final boolean compactOnWrite;;
        private final int batchSize;

        public ClientKey(
                final String directory,
                final boolean visibilityEnabled,
                final boolean compactOnWrite,
                final int batchSize) {
            super();
            this.directory = directory;
            this.visibilityEnabled = visibilityEnabled;
            this.compactOnWrite = compactOnWrite;
            this.batchSize = batchSize;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = (prime * result) + batchSize;
            result = (prime * result) + (compactOnWrite ? 1231 : 1237);
            result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
            result = (prime * result) + (visibilityEnabled ? 1231 : 1237);
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ClientKey other = (ClientKey) obj;
            if (batchSize != other.batchSize) {
                return false;
            }
            if (compactOnWrite != other.compactOnWrite) {
                return false;
            }
            if (directory == null) {
                if (other.directory != null) {
                    return false;
                }
            } else if (!directory.equals(other.directory)) {
                return false;
            }
            if (visibilityEnabled != other.visibilityEnabled) {
                return false;
            }
            return true;
        }
    }
}