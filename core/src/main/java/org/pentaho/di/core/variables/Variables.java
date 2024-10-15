/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.core.variables;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.version.BuildVersion;

import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is an implementation of VariableSpace
 *
 * @author Sven Boden
 */
public class Variables implements VariableSpace {
  private Map<String, String> properties;

  private VariableSpace parent;

  private Map<String, String> injection;

  private boolean initialized;

  public Variables() {
    properties = new ConcurrentHashMap<>();
    parent = null;
    injection = null;
    initialized = false;

    // The Kettle version
    properties.put( Const.INTERNAL_VARIABLE_KETTLE_VERSION, BuildVersion.getInstance().getVersion() );

    // The Kettle build version
    String revision = BuildVersion.getInstance().getRevision();
    if ( revision == null ) {
      revision = "";
    }
    properties.put( Const.INTERNAL_VARIABLE_KETTLE_BUILD_VERSION, revision );

    // The Kettle build date
    String buildDate = BuildVersion.getInstance().getBuildDate();
    if ( buildDate == null ) {
      buildDate = "";
    }
    properties.put( Const.INTERNAL_VARIABLE_KETTLE_BUILD_DATE, buildDate );
  }

  @Override
  public void copyVariablesFrom( VariableSpace space ) {
    if ( space != null && this != space ) {
      // If space is not null and this variable is not already
      // the same object as the argument.
      String[] variableNames = space.listVariables();
      for ( int idx = 0; idx < variableNames.length; idx++ ) {
        properties.put( variableNames[idx], space.getVariable( variableNames[idx] ) );
      }
    }
  }

  @Override
  public VariableSpace getParentVariableSpace() {
    return parent;
  }

  @Override
  public void setParentVariableSpace( VariableSpace parent ) {
    this.parent = parent;
  }

  @Override
  public String getVariable( String variableName, String defaultValue ) {
    String var = properties.get( variableName );
    if ( var == null ) {
      return defaultValue;
    }
    return var;
  }

  @Override
  public String getVariable( String variableName ) {
    return properties.get( variableName );
  }

  @Override
  public boolean getBooleanValueOfVariable( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaBase.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  @Override
  public void initializeVariablesFrom( VariableSpace parent ) {
    // only read these once. Don't overwrite values that have already been set on this variable space from other APIs
    if ( !initialized ) {
      // Clone the system properties to avoid ConcurrentModificationException while iterating
      // and then add all of them to properties variable.
      Set<String> systemPropertiesNames = System.getProperties().stringPropertyNames();
      for ( String key : systemPropertiesNames ) {
        String value = System.getProperties().getProperty( key );
        if ( value != null ) {
          getProperties().put( key, value );
        }
      }
    }

    if ( parent != null ) {
      this.parent = parent;
      copyVariablesFrom( parent );
    }
    if ( injection != null ) {
      properties.putAll( injection );
      injection = null;
    }
    initialized = true;
  }

  @Override
  public String[] listVariables() {
    Set<String> keySet = properties.keySet();
    return keySet.toArray( new String[0] );
  }

  @Override
  public void setVariable( String variableName, String variableValue ) {
    if ( variableValue != null ) {
      properties.put( variableName, variableValue );
    } else {
      properties.remove( variableName );
    }
  }

  @Override
  public String environmentSubstitute( String aString ) {
    if ( aString == null || aString.length() == 0 ) {
      return aString;
    }

    return StringUtil.environmentSubstitute( aString, properties );
  }

  @Override
  public String environmentSubstitute( String aString, boolean escapeHexDelimiter ) {
    if ( aString == null || aString.length() == 0 ) {
      return aString;
    }

    return StringUtil.environmentSubstitute( aString, properties, escapeHexDelimiter );
  }

  /**
   * Substitutes field values in <code>aString</code>. Field values are of the form "?{<field name>}". The values are
   * retrieved from the specified row. Please note that the getString() method is used to convert to a String, for all
   * values in the row.
   *
   * @param aString
   *          the string on which to apply the substitution.
   * @param rowMeta
   *          The row metadata to use.
   * @param rowData
   *          The row data to use
   *
   * @return the string with the substitution applied.
   * @throws KettleValueException
   *           In case there is a String conversion error
   */
  @Override
  public String fieldSubstitute( String aString, RowMetaInterface rowMeta, Object[] rowData )
    throws KettleValueException {
    if ( aString == null || aString.length() == 0 ) {
      return aString;
    }

    return StringUtil.substituteField( aString, rowMeta, rowData );
  }

  @Override
  public String[] environmentSubstitute( String[] string ) {
    String[] retval = new String[string.length];
    for ( int i = 0; i < string.length; i++ ) {
      retval[i] = environmentSubstitute( string[i] );
    }
    return retval;
  }

  @Override
  public void shareVariablesWith( VariableSpace space ) {
    // not implemented in here... done by pointing to the same VariableSpace
    // implementation
  }

  @Override
  public void injectVariables( Map<String, String> prop ) {
    if ( initialized ) {
      // variables are already initialized
      if ( prop != null ) {
        for ( Map.Entry<String, String> entry : prop.entrySet() ) {
          String value = entry.getValue();
          String key = entry.getKey();
          if ( !Utils.isEmpty( key ) ) {
            properties.put( key, Const.NVL( value, "" ) );
          }
        }
        injection = null;
      }
    } else {
      // We have our own personal copy, so changes afterwards
      // to the input properties don't affect us.
      injection = new Hashtable<String, String>();
      for ( Map.Entry<String, String> entry : prop.entrySet() ) {
        String value = entry.getValue();
        String key = entry.getKey();
        if ( !Utils.isEmpty( key ) ) {
          injection.put( key, Const.NVL( value, "" ) );
        }
      }
    }
  }

  /**
   * Get a default variable space as a placeholder. Everytime you will get a new instance.
   *
   * @return a default variable space.
   */
  public static VariableSpace getADefaultVariableSpace() {
    VariableSpace space = new Variables();

    space.initializeVariablesFrom( null );

    return space;
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  Map<String, String> getProperties() {
    return properties;
  }

  public static final String POC_DEBUG_VAR_NAME = "input_access_key";

  /**
   * POC need to implement custom hash code function that evaluates the variables value since it implements
   * since it <code>extends JobEntryBase</code> and that <code> implements VariablesSpace</code>
   * VariableSpace instances are used in Apace VFS FileSystemOptions logic, if options in our case variables haven't
   * changed then no need to re-create an new Filesystem object.
   *
   * Here is where Apache VFS retrieves previously created Filesystem.
   * If the options/values are the same (see compareTo snippet below):
   *  - fetch previously created one
   *  - else create new filesystem
   *
   *  <code>
   *    protected synchronized FileSystem getFileSystem(final FileName rootName, final FileSystemOptions fileSystemOptions)
   *             throws FileSystemException {
   *         FileSystem fs = findFileSystem(rootName, fileSystemOptions);
   *         if (fs == null) {
   *             // Need to create the file system, and cache it
   *             fs = doCreateFileSystem(rootName, fileSystemOptions);
   *             addFileSystem(rootName, fs);
   *         }
   *         return fs;
   *  </code>
   * url: https://github.com/apache/commons-vfs/blob/rel/commons-vfs-2.8.0/commons-vfs2/src/main/java/org/apache/commons/vfs2/provider/AbstractOriginatingFileProvider.java#L87-L95
   *
   * The call to
   * <code>
   *       protected FileSystem findFileSystem(final Comparable<?> key, final FileSystemOptions fileSystemOptions) {
   *         synchronized (fileSystemMap) {
   *             return fileSystemMap.get(new FileSystemKey(key, fileSystemOptions));
   *         }
   *     }
   * </code>
   *
   * url: https://github.com/apache/commons-vfs/blob/rel/commons-vfs-2.8.0/commons-vfs2/src/main/java/org/apache/commons/vfs2/provider/AbstractFileProvider.java#L116-L120
   *
   * (see compareTo snippet below)
   * Here is where the options/variables are  compared against one another
   * <code>
   *        final int hash = Arrays.deepHashCode(myOptions.values().toArray());
   *         final int hashFk = Arrays.deepHashCode(theirOptions.values().toArray());
   *         return Integer.compare(hash, hashFk);
   * </code>
   *
   * url: https://github.com/apache/commons-vfs/blob/rel/commons-vfs-2.8.0/commons-vfs2/src/main/java/org/apache/commons/vfs2/FileSystemOptions.java#L172-L174
   *
   *
   * Example PVFS of FileSystemOptions
   *   [1]
   *     key = "kettle.VariableSpace"
   *     value = instance of org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles
   *   [2]
   *     key = "kettle.bowl"
   *     value = instance of DefualtBowl
   *   [3]
   *     key = "org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder.USER_DIR_IS_ROOT"
   *     value = Boolean.class - false
   */
  @Override
  public int hashCode() {
    // FOR POC only worried about one variable, need to copy logic to all variables
    return Objects.hash(getVariable( POC_DEBUG_VAR_NAME ));  //
  }

  // TODO implement #equals and #hashCode apart of java standards - https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Object.html#hashCode()
  // TODO all classes that extend or impplement org.pentaho.di.core.variables need this logic. Maybe create have default implementation #hashCode()
  // TODO unit tests, previous code was getting a miss on TreeMap#get
  // NOTE: when fix is applied fileSystemMap won't grow with every call, due to cache miss from TreeMap#get
}
