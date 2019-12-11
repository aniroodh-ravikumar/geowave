package org.locationtech.geowave.datastore.foundationdb.operations;

import java.io.Closeable;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.*;
import org.locationtech.geowave.datastore.foundationdb.config.FoundationDBRequiredOptions;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBClient;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBUtils;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;

public class FoundationDBOperations implements MapReduceDataStoreOperations, Closeable {
  private static final boolean READER_ASYNC = true;
  private final FoundationDBClient client;
  private final String directory;
  private final boolean visibilityEnabled;
  private final int batchWriteSize;

  public FoundationDBOperations(final FoundationDBRequiredOptions options) {
    this.directory =
        options.getDirectory()
            + File.separator
            + ((options.getGeoWaveNamespace() == null)
                || options.getGeoWaveNamespace().trim().isEmpty()
                || "null".equalsIgnoreCase(options.getGeoWaveNamespace()) ? "default"
                    : options.getGeoWaveNamespace());
    this.visibilityEnabled = options.getStoreOptions().isVisibilityEnabled();
    this.batchWriteSize = options.getBatchWriteSize();
    this.client = new FoundationDBClient(directory, visibilityEnabled, batchWriteSize);

    // this does not open the database
    // open the database with fdb.open()
  }

  @Override
  public void close() {
    client.close();
  }

  @Override
  public boolean indexExists(String indexName) {
    return client.indexTableExists(indexName);
  }

  @Override
  public boolean metadataExists(MetadataType type) {
    return client.metadataTableExists(type);
  }

  @Override
  public void deleteAll() throws Exception {
    close();
    FileUtils.deleteDirectory(new File(directory));
  }

  @Override
  public boolean deleteAll(
      String indexName,
      String typeName,
      Short adapterId,
      String... additionalAuthorizations) {
    return false;
  }

  @Override
  public boolean ensureAuthorizations(String clientUser, String... authorizations) {
    return false;
  }

  @Override
  public RowWriter createWriter(Index index, InternalDataAdapter<?> adapter) {
    return new FoundationDBWriter(
        this.client,
        adapter.getAdapterId(),
        adapter.getTypeName(),
        index.getName(),
        true);
  }

  @Override
  public MetadataWriter createMetadataWriter(MetadataType metadataType) {
    return new FoundationDBMetadataWriter(FoundationDBUtils.getMetadataTable(client, metadataType));
  }

  @Override
  public MetadataReader createMetadataReader(MetadataType metadataType) {
    return new FoundationDBMetadataReader(
        FoundationDBUtils.getMetadataTable(client, metadataType),
        metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(MetadataType metadataType) {
    return new FoundationDBMetadataDeleter(
        FoundationDBUtils.getMetadataTable(client, metadataType),
        metadataType);
  }

  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    return new FoundationDBReader(client, readerParams, READER_ASYNC);
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams readerParams) {
    return new FoundationDBReader<>(client, readerParams);
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    return new FoundationDBReader<>(client, readerParams);
  }

  @Override
  public RowDeleter createRowDeleter(
      String indexName,
      PersistentAdapterStore adapterStore,
      InternalAdapterStore internalAdapterStore,
      String... authorizations) {
    return new FoundationDBRowDeleter(client, adapterStore, internalAdapterStore, indexName);
  }
}
