package org.locationtech.geowave.datastore.foundationdb.util;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

/**
 * This class provides an abstraction for FoundationDB metadata
 */
public class FoundationDBGeoWaveMetadata extends GeoWaveMetadata {
  private final byte[] originalKey;

  /**
   * @param primaryId The primary Id of the key.
   * @param secondaryId The secondary Id of the key.
   * @param visibility TODO
   * @param value The value that the key should map to
   * @param originalKey TODO
   */
  public FoundationDBGeoWaveMetadata(
      final byte[] primaryId,
      final byte[] secondaryId,
      final byte[] visibility,
      final byte[] value,
      final byte[] originalKey) {
    super(primaryId, secondaryId, visibility, value);
    this.originalKey = originalKey;
  }

  /**
   * This method is used to get the original key of this metadata instance
   *
   * @return the originalKey field
   */
  public byte[] getKey() {
    return originalKey;
  }

  /**
   * This method is used to get the hash code used for hashing keys
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = (prime * result) + getClass().hashCode();
    return result;
  }

  /**
   * This method is used to check whether an object is equal to this instance of FoundationDB
   * metadata
   *
   * @return true if equal, false otherwise
   */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return true;
  }
}
