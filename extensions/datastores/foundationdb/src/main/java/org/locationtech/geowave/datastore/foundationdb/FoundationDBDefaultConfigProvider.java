package org.locationtech.geowave.datastore.foundationdb;

import java.util.Properties;
import org.locationtech.geowave.core.cli.spi.DefaultConfigProviderSpi;

public class FoundationDBDefaultConfigProvider implements DefaultConfigProviderSpi {
    private final Properties configProperties = new Properties();

    /**
     * Create the properties for the config-properties file
     */
    private void setProperties() {
        configProperties.setProperty("store.default-rocksdb.opts.gwNamespace", "default");
        configProperties.setProperty("store.default-rocksdb.type", "rocksdb");
    }

    @Override
    public Properties getDefaultConfig() {
        setProperties();
        return configProperties;
    }
}

