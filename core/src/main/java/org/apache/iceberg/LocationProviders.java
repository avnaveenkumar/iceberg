/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;

public class LocationProviders {

  private LocationProviders() {
  }

  public static LocationProvider locationsFor(String location, Map<String, String> properties) {
    if (properties.containsKey(TableProperties.WRITE_LOCATION_PROVIDER_IMPL)) {
      String impl = properties.get(TableProperties.WRITE_LOCATION_PROVIDER_IMPL);
      DynConstructors.Ctor<LocationProvider> ctor;
      try {
        ctor = DynConstructors.builder(LocationProvider.class)
            .impl(impl, String.class, Map.class)
            .impl(impl).buildChecked(); // fall back to no-arg constructor
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(String.format(
            "Unable to find a constructor for implementation %s of %s. " +
                "Make sure the implementation is in classpath, and that it either " +
                "has a public no-arg constructor or a two-arg constructor " +
                "taking in the string base table location and its property string map.",
            impl, LocationProvider.class), e);
      }
      try {
        return ctor.newInstance(location, properties);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format("Provided implementation for dynamic instantiation should implement %s.",
                LocationProvider.class), e);
      }
    } else if (PropertyUtil.propertyAsBoolean(properties,
        TableProperties.OBJECT_STORE_ENABLED,
        TableProperties.OBJECT_STORE_ENABLED_DEFAULT)) {
      return new ObjectStoreLocationProvider(location, properties);
    } else if (PropertyUtil.propertyAsBoolean(properties,
            TableProperties.OBJECT_STORE_ENABLED_EXTENDED_PREFIX,
            TableProperties.OBJECT_STORE_ENABLED_EXTENDED_PREFIX_DEFAULT)) {
      return new ObjectStoreLocationProviderExtended(location, properties);
    } else {
      return new DefaultLocationProvider(location, properties);
    }
  }

  static class DefaultLocationProvider implements LocationProvider {
    private final String dataLocation;

    DefaultLocationProvider(String tableLocation, Map<String, String> properties) {
      this.dataLocation = stripTrailingSlash(dataLocation(properties, tableLocation));
    }

    private static String dataLocation(Map<String, String> properties, String tableLocation) {
      String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
      if (dataLocation == null) {
        dataLocation = properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
        if (dataLocation == null) {
          dataLocation = String.format("%s/data", tableLocation);
        }
      }
      return dataLocation;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      return String.format("%s/%s/%s", dataLocation, spec.partitionToPath(partitionData), filename);
    }

    @Override
    public String newDataLocation(String filename) {
      return String.format("%s/%s", dataLocation, filename);
    }
  }

  static class ObjectStoreLocationProvider implements LocationProvider {
    private static final Transform<String, Integer> HASH_FUNC = Transforms
        .bucket(Types.StringType.get(), Integer.MAX_VALUE);

    private final String storageLocation;
    private final String context;

    ObjectStoreLocationProvider(String tableLocation, Map<String, String> properties) {
      this.storageLocation = stripTrailingSlash(dataLocation(properties, tableLocation));
      this.context = pathContext(tableLocation);
    }

    private static String dataLocation(Map<String, String> properties, String tableLocation) {
      String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
      if (dataLocation == null) {
        dataLocation = properties.get(TableProperties.OBJECT_STORE_PATH);
        if (dataLocation == null) {
          dataLocation = properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
          if (dataLocation == null) {
            dataLocation = String.format("%s/data", tableLocation);
          }
        }
      }
      return dataLocation;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      return newDataLocation(String.format("%s/%s", spec.partitionToPath(partitionData), filename));
    }

    @Override
    public String newDataLocation(String filename) {
      int hash = HASH_FUNC.apply(filename);
      return String.format("%s/%08x/%s/%s", storageLocation, hash, context, filename);
    }

    private static String pathContext(String tableLocation) {
      Path dataPath = new Path(tableLocation);
      Path parent = dataPath.getParent();
      String resolvedContext;
      if (parent != null) {
        // remove the data folder
        resolvedContext = String.format("%s/%s", parent.getName(), dataPath.getName());
      } else {
        resolvedContext = dataPath.getName();
      }

      Preconditions.checkState(
          !resolvedContext.endsWith("/"),
          "Path context must not end with a slash.");

      return resolvedContext;
    }
  }

  static class ObjectStoreLocationProviderExtended implements LocationProvider {
    private static final Transform<String, Integer> HASH_FUNC = Transforms
            .bucket(Types.StringType.get(), Integer.MAX_VALUE);

    private final String storageLocation;
    private final String context;

    private static final Map<String, String> prefixMap = Maps.newHashMap();

    static {
      prefixMap.put("0", "0");
      prefixMap.put("1", "1");
      prefixMap.put("2", "2");
      prefixMap.put("3", "3");
      prefixMap.put("4", "4");
      prefixMap.put("5", "5");
      prefixMap.put("6", "6");
      prefixMap.put("7", "7");
      prefixMap.put("8", "8");
      prefixMap.put("9", "9");
      prefixMap.put("10", "a");
      prefixMap.put("11", "b");
      prefixMap.put("12", "c");
      prefixMap.put("13", "d");
      prefixMap.put("14", "e");
      prefixMap.put("15", "f");
      prefixMap.put("16", "g");
      prefixMap.put("17", "h");
      prefixMap.put("18", "i");
      prefixMap.put("19", "j");
      prefixMap.put("20", "k");
      prefixMap.put("21", "l");
      prefixMap.put("22", "m");
      prefixMap.put("23", "n");
      prefixMap.put("24", "o");
      prefixMap.put("25", "p");
      prefixMap.put("26", "q");
      prefixMap.put("27", "r");
      prefixMap.put("28", "s");
      prefixMap.put("29", "t");
      prefixMap.put("30", "u");
      prefixMap.put("31", "v");
      prefixMap.put("32", "w");
      prefixMap.put("33", "x");
      prefixMap.put("34", "y");
      prefixMap.put("35", "z");
      prefixMap.put("36", "A");
      prefixMap.put("37", "B");
      prefixMap.put("38", "C");
      prefixMap.put("39", "D");
      prefixMap.put("40", "E");
      prefixMap.put("41", "F");
      prefixMap.put("42", "G");
      prefixMap.put("43", "H");
      prefixMap.put("44", "I");
      prefixMap.put("45", "J");
      prefixMap.put("46", "K");
      prefixMap.put("47", "L");
      prefixMap.put("48", "M");
      prefixMap.put("49", "N");
      prefixMap.put("50", "O");
      prefixMap.put("51", "P");
      prefixMap.put("52", "Q");
      prefixMap.put("53", "R");
      prefixMap.put("54", "S");
      prefixMap.put("55", "T");
      prefixMap.put("56", "U");
      prefixMap.put("57", "V");
      prefixMap.put("58", "W");
      prefixMap.put("59", "X");
      prefixMap.put("60", "Y");
      prefixMap.put("61", "Z");
    }

    ObjectStoreLocationProviderExtended(String tableLocation, Map<String, String> properties) {
      this.storageLocation = stripTrailingSlash(dataLocation(properties, tableLocation));
      this.context = pathContext(tableLocation);
    }

    private static String dataLocation(Map<String, String> properties, String tableLocation) {
      String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
      if (dataLocation == null) {
        dataLocation = properties.get(TableProperties.OBJECT_STORE_PATH);
        if (dataLocation == null) {
          dataLocation = properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
          if (dataLocation == null) {
            dataLocation = String.format("%s/data", tableLocation);
          }
        }
      }
      return dataLocation;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      return newDataLocation(String.format("%s/%s", spec.partitionToPath(partitionData), filename));
    }

    @Override
    public String newDataLocation(String filename) {
      int hash = HASH_FUNC.apply(filename);
      String finalHash = deriveHash(hash);
      return String.format("%s/%s/%s/%s", storageLocation, finalHash, context, filename);
    }

    private static String pathContext(String tableLocation) {
      Path dataPath = new Path(tableLocation);
      Path parent = dataPath.getParent();
      String resolvedContext;
      if (parent != null) {
        // remove the data folder
        resolvedContext = String.format("%s/%s", parent.getName(), dataPath.getName());
      } else {
        resolvedContext = dataPath.getName();
      }

      Preconditions.checkState(
              !resolvedContext.endsWith("/"),
              "Path context must not end with a slash.");

      return resolvedContext;
    }

    private static String deriveHash(int hash) {

      // Derive 1st character
      String firstPrefixChar = prefixMap.get(String.format("%d", (hash % 62)));

      // Derive 2nd character
      int hashShifted = hash >> 8;
      String secondPrefixChar = prefixMap.get(String.format("%d", (hashShifted % 62)));

      String hexHash = String.format("%08x", hash);
      String hashFinal = firstPrefixChar + secondPrefixChar + hexHash;

      return hashFinal;
    }
  }

  private static String stripTrailingSlash(String path) {
    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }
}
