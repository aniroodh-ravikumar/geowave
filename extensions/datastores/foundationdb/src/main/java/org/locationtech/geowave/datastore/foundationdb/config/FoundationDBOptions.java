package org.locationtech.geowave.datastore.foundationdb.config;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.foundationdb.FoundationDBStoreFactoryFamily;

public class FoundationDBOptions extends StoreFactoryOptions {
  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new FoundationDBStoreFactoryFamily();
  }

  // TODO: implement this
  @Override
  public DataStoreOptions getStoreOptions() {
    return null;
  }
}
