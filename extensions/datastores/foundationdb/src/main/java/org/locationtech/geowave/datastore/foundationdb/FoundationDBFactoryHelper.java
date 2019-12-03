package org.locationtech.geowave.datastore.foundationdb;

import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.foundationdb.config.FoundationDBRequiredOptions;
import org.locationtech.geowave.datastore.foundationdb.operations.FoundationDBOperations;

public class FoundationDBFactoryHelper implements StoreFactoryHelper {
  @Override
  public FoundationDBRequiredOptions createOptionsInstance() {
    return new FoundationDBRequiredOptions();
  }

  @Override
  public FoundationDBOperations createOperations(final StoreFactoryOptions options) {
    return new FoundationDBOperations((FoundationDBRequiredOptions) options);
  }
}
