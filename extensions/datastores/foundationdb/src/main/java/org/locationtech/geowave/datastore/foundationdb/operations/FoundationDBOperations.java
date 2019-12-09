package org.locationtech.geowave.datastore.foundationdb.operations;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides an interface for creating instances of classes used to execute FoundationDB operations.
 */
public class FoundationDBOperations implements MapReduceDataStoreOperations, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBOperations.class);
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

  }

  /**
   * Calls the client's close method.
   *
   * Postconditions: <ul> <li>  The database is cleared and then closed </li> </ul>
   */
  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public boolean indexExists(String indexName) throws IOException {
    return client.indexTableExists(indexName);
  }

  @Override
  public boolean metadataExists(MetadataType type) throws IOException {
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

  /**
   * Create an instance of a writer.
   */
  @Override
  public RowWriter createWriter(Index index, InternalDataAdapter<?> adapter) {
    return new FoundationDBWriter(
        this.client,
        adapter.getAdapterId(),
        adapter.getTypeName(),
        index.getName(),
        true);
  }

  /**
   * Create an instance of a metadata writer.
   */
  @Override
  public MetadataWriter createMetadataWriter(MetadataType metadataType) {
    return new FoundationDBMetadataWriter(FoundationDBUtils.getMetadataTable(client, metadataType));
  }

  /**
   * Create an instance of a metadata reader.
   */
  @Override
  public MetadataReader createMetadataReader(MetadataType metadataType) {
    return new FoundationDBMetadataReader(
        FoundationDBUtils.getMetadataTable(client, metadataType),
        metadataType);
  }

  /**
   * Create an instance of a metadata deleter.
   */
  @Override
  public MetadataDeleter createMetadataDeleter(MetadataType metadataType) {
    return new FoundationDBMetadataDeleter(
        FoundationDBUtils.getMetadataTable(client, metadataType),
        metadataType);
  }

  /**
   * Create an instance of a reader.
   * @param readerParams parameters of type ReaderParams<T> used to instantiate the reader
   */
  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    return new FoundationDBReader(client, readerParams, READER_ASYNC);
  }

  /**
   * Create an instance of a reader.
   * @param readerParams parameters of type RecordReaderParams<T> used to instantiate the reader
   */
  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams readerParams) {
    return new FoundationDBReader<>(client, readerParams);
  }

  /**
   * Create an instance of a reader.
   * @param readerParams parameters of type DataIndexReaderParams used to instantiate the reader
   */
  @Override
  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    return new FoundationDBReader<>(client, readerParams);
  }

  /**
   * Create an instance of a row deleter.
   */
  @Override
  public RowDeleter createRowDeleter(
      String indexName,
      PersistentAdapterStore adapterStore,
      InternalAdapterStore internalAdapterStore,
      String... authorizations) {
    return new FoundationDBRowDeleter(client, adapterStore, internalAdapterStore, indexName);
  }
}
