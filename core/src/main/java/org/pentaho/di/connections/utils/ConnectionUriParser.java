/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2024 Hitachi Vantara. All rights reserved.
 */

package org.pentaho.di.connections.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class to extract information from the
 * {@value org.pentaho.di.connections.vfs.provider.ConnectionFileProvider#SCHEME} URI.
 * This class handles special characters in the connection name section of the URI by simply parsing the connection name
 * as is.This class has limited functionality. Main focus is a temporary substitute for not being able to use directly
 * {@link java.net.URI} due to special characters in the URI.
 */
public class ConnectionUriParser {

  /**
   * Pattern to match a PVFS URI with scheme, connection name, and path.
   * <p/> Some Examples:
   * <ul>
   *   <li>For PVFS URI: "pvfs://connectionName/someFolderA/SomeFolderB/someFile.txt"</li>
   *      <ul>
   *        <li>scehme: "pvfs"</li>
   *        <li>connection Name: "connectionName"</li>
   *        <li>pvfs path: "/someFolderA/SomeFolderB/someFile.txt"</li>
   *
   *      </ul>
   * </ul>
   * Regex should be encompass {@link org.pentaho.di.connections.vfs.provider.ConnectionFileSystem#DOMAIN_ROOT}
   */
  public static final Pattern CONNECTION_URI_WITH_CONNECTION_NAME_PATH_PATTERN
    = Pattern.compile(  "(\\w+)://([^/]+)(/.+)" );

  /**
   * Pattern to match a URI with scheme and connection name or first path segment.
   * <p/> Some Examples:
   * <ul>
   *   <li>For PVFS URI: "pvfs://connectionName"</li>
   *   <li>For VFS URI: "xyz://firstSegment"</li>
   * </ul>
   * Regex should be encompass {@link org.pentaho.di.connections.vfs.provider.ConnectionFileSystem#DOMAIN_ROOT}
   */
  public static final Pattern CONNECTION_URI_WITH_CONNECTION_NAME_PATTERN
      = Pattern.compile(  "(\\w+)://([^/]+)/?" );

  /**
   * Pattern to match a URI with just a scheme.
   * <p/> Some Examples:
   * <ul>
   *   <li>For PVFS URI: "pvfs://"</li>
   *   <li>For VFS URI: "xyz://"</li>
   * </ul>
   * Regex should be encompass {@link org.pentaho.di.connections.vfs.provider.ConnectionFileSystem#DOMAIN_ROOT}
   */
  public static final Pattern CONNECTION_URI_NAME_PATTERN = Pattern.compile(  "(\\w+)://" );

  /**
   * {@link #scheme} index for {@link Matcher#group(int)}
   */
  private static final int GROUP_INDEX_SCHEME = 1;

  /**
   * {@link #connectionName} index for {@link Matcher#group(int)}
   */
  private static final int GROUP_INDEX_CONNECTION_NAME = 2;

  /**
   * {@link #pvfsPath} index for {@link Matcher#group(int)}
   */
  private static final int GROUP_INDEX_PVFS_PATH = 3;

  private final String vfsUri;

  /**
   * URI scheme or prefix
   */
  private String scheme;

  /**
   * URI connection name for PVFS URI or first path segment for VFS URI
   */
  private String connectionName;

  /**
   * URI path for PVFS URI
   */
  private String pvfsPath;


  public ConnectionUriParser( String vfsUri ) {
    this.vfsUri = vfsUri;
    executeMatchers();
  }

  /**
   * Call the matchers to determine the various variables:
   * <ul>
   *   <li>{@link #scheme}</li>
   *   <li>{@link #connectionName}</li>
   * </ul>
   */
  private void executeMatchers() {
    try {
      Matcher matcher;
      if ( ( matcher = CONNECTION_URI_WITH_CONNECTION_NAME_PATH_PATTERN.matcher( this.vfsUri ) ).find() ) {
        int groupCount = matcher.groupCount();
        this.scheme = matcher.group( GROUP_INDEX_SCHEME );
        this.connectionName = GROUP_INDEX_CONNECTION_NAME <= groupCount
            ? matcher.group( GROUP_INDEX_CONNECTION_NAME ) : null;
        this.pvfsPath = GROUP_INDEX_PVFS_PATH  <= groupCount ? matcher.group( GROUP_INDEX_PVFS_PATH  ) : null;
      } else if ( ( matcher = CONNECTION_URI_WITH_CONNECTION_NAME_PATTERN.matcher( this.vfsUri ) ).find() ) {
        int groupCount = matcher.groupCount();
        this.scheme = matcher.group( GROUP_INDEX_SCHEME );
        this.connectionName = GROUP_INDEX_CONNECTION_NAME <= groupCount
            ? matcher.group( GROUP_INDEX_CONNECTION_NAME ) : null;
      } else if ( connectionName == null && ( matcher = CONNECTION_URI_NAME_PATTERN.matcher( this.vfsUri ) ).find() ) {
        this.scheme = matcher.group( GROUP_INDEX_SCHEME );
      }
    } catch ( NullPointerException e ) {
      // do nothing
    }
  }

  /**
   * Get the scheme
   * @return scheme or null otherwise.
   */
  public String getScheme() {
    return scheme;
  }

  /**
   * Get the connection name or first segment of URI
   * @return connection name or null otherwise
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * Get path or PVFS URI, does not include the {@link #connectionName}
   * @return path or null otherwise
   */
  public String getPvfsPath() {
    return pvfsPath;
  }

}
