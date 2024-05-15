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
 * This class has limited functionality. Main focus is a temporary substitute for {@link java.net.URI} due to
 * special characters in the URI.
 */
public class ConnectionUriParser {

  public static final Pattern CONNECTION_URI_PATTERN = Pattern.compile(  "([\\w]+)://([^/]+)[/]?" ); // TODO re-use other class regex variables

  private final String vfsUri;

  private String scheme;

  private String connectionName;

  public ConnectionUriParser( String vfsUri ) {
    this.vfsUri = vfsUri;
    executeMatcher();
  }

  private Matcher executeMatcher() {
    Matcher matcher = CONNECTION_URI_PATTERN.matcher( this.vfsUri );
    if ( matcher.find() ) {
      int groupCount = matcher.groupCount(); // TODO throw URISytaxException
      this.scheme = matcher.group( 1 );
      this.connectionName = matcher.group( 2 );
//      System.out.println( String.format( "uri:\"%s\""
//        + "\n\tscheme: \"%s\""
//        + "\n\tconnectionName: \"%s\"", uri, scheme, connectionName ) );
//      System.out.println();
    }
    return matcher;
  }

  public String getScheme() {
    return scheme;
  }

  public String getConnectionName() {
    return connectionName;
  }

}
