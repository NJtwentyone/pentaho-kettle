/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.di.job.entries.copyfiles;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.vfs.VFSConnectionManagerHelper;
import org.pentaho.di.connections.vfs.provider.ConnectionFileSystem;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.steps.named.cluster.NamedClusterEmbedManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.core.util.Assert.assertNotNull;
import static org.pentaho.di.core.util.Assert.assertNull;

/**
 * Sample class to demonstrate the "caching" scenarios in https://hv-eng.atlassian.net/browse/BACKLOG-42482
 * and https://hv-eng.atlassian.net/browse/BACKLOG-42352?focusedCommentId=2085016
 */
public class PocVFSFileSystemHashcodeVariablesJobEntryCopyFilesTest {
  private JobEntryCopyFiles entry;
  private NamedClusterEmbedManager mockNamedClusterEmbedManager;

  public static final String JOB_ENTRY_NAME = "pocJobEntryCopyFiles";
  public static final String KEY_FILESYSTEM_OPTIONS_1 = "key1";
  public static final String KEY_FILESYSTEM_OPTIONS_2 = "key2";
  public static final String KEY_FILESYSTEM_OPTIONS_3 = "key3";
  public static final String KEY_FILESYSTEM_OPTIONS_4 = "key4_variableObj";
  public static final String POC_KEY_VAR_NAME = "input_access_key"; // set in variable space
  public static final Boolean VALUE_FILESYSTEM_OPTIONS_BOOLEAN = Boolean.FALSE;
  public static final String VALUE_FILESYSTEM_OPTIONS_STR = "test_string_value";
  public static final Integer VALUE_FILESYSTEM_OPTIONS_INT = 349;
  public static final String VALUE_VAR_ACCESS_ID = "<super_secret_access_key_id>";
  public static final String VALUE_VAR_UPDATE_ACCESS_ID = "<UPDATED_IN_THE_FUTURE_ACCESS_KEY_ID>";
  String PVFS_URI_1_SRC = "pvfs://someConn/somePath/AnotherDir/randomFile.txt";
  String PVFS_URI_2_SRC_COMPLETELY_DIFFERENT = "pvfs://DIFFERENT_CONNECTION/super_secret_path/hidden_folder/revenue.xlsx";
  String PVFS_URI_1_DEST = "pvfs://someConn/somePath/extraFolder/superSecretFile.csv";

  String PVFS_URI_2_DEST_COMPLETELY_DIFFERENT = "pvfs://DIFFERENT_CONNECTION/super_secret_path/hidden_folder/revenue.xlsx";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    KettleLogStore.init();
  }

  @Before
  public void setUp() {
    entry = new JobEntryCopyFiles();
    Job parentJob = new Job();
    entry.setParentJob( parentJob );
    JobMeta mockJobMeta = mock( JobMeta.class );
    mockNamedClusterEmbedManager = mock( NamedClusterEmbedManager.class );
    when( mockJobMeta.getNamedClusterEmbedManager() ).thenReturn( mockNamedClusterEmbedManager );
    entry.setParentJobMeta( mockJobMeta );
    entry = spy( entry );
  }


  @Test
  public void test_Assumptions_1_hashcode_Variables() throws Exception {

    VariableSpace variables1 = new Variables();
    variables1.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    VariableSpace variables2 = new Variables();
    variables2.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    // NOTE: should NOT EQUAL string to string value comparison #equals, #hasCode.
    assertNotEquals( variables1.getVariable( POC_KEY_VAR_NAME ), variables2.getVariable(  POC_KEY_VAR_NAME ) );
    assertNotEquals( variables1.getVariable( POC_KEY_VAR_NAME ).hashCode(), variables2.getVariable( POC_KEY_VAR_NAME ).hashCode() );

    // NOTE: obviously assert should fail, if  VariableSpace doesn't have custom #hashCode() impl to check contents instead java reference.
    assertNotEquals( variables1.hashCode(), variables2.hashCode() );

  }

  @Test
  public void test_Assumptions_2a_hashcode_JobEntryCopyFiles_Variables() throws Exception {

    JobEntryCopyFiles jobEntry = testInstance( JOB_ENTRY_NAME );
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    JobEntryCopyFiles jobEntry_sameName = testInstance( JOB_ENTRY_NAME );
    jobEntry_sameName.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID);

    assertEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ), jobEntry_sameName.getVariable(  POC_KEY_VAR_NAME ) );
    assertEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ).hashCode(), jobEntry_sameName.getVariable( POC_KEY_VAR_NAME ).hashCode() );

    // NOTE: sanity check
    assertEquals( jobEntry.hashCode(), jobEntry_sameName.hashCode() );

  }


  @Test
  public void test_Assumptions_2b_hashcode_JobEntryCopyFiles_Variables() throws Exception {

    JobEntryCopyFiles jobEntry = testInstance( JOB_ENTRY_NAME );
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    JobEntryCopyFiles jobEntry_sameName_differentVariablesValues = testInstance( JOB_ENTRY_NAME );
    jobEntry_sameName_differentVariablesValues.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_UPDATE_ACCESS_ID );

    // NOTE: should NOT EQUAL string to string value comparison #equals, #hasCode.
    assertNotEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ), jobEntry_sameName_differentVariablesValues.getVariable(  POC_KEY_VAR_NAME ) );
    assertNotEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ).hashCode(), jobEntry_sameName_differentVariablesValues.getVariable( POC_KEY_VAR_NAME ).hashCode() );

    // NOTE: obviously assert should fail, if  VariableSpace doesn't have custom #hashCode() impl to check contents instead java reference.
    /*
     WHAT if long running instance of PDI and the same name job id but with
     generic URI "pvfs://on-the-fly-connection/" is shared by different ktrs
     and but with vastly different VFS systems one Azure the other GCS
     */
    assertNotEquals( jobEntry.hashCode(), jobEntry_sameName_differentVariablesValues.hashCode() );

  }

  @Test
  public void test_Assumptions_2c_hashcode_JobEntryCopyFiles_Variables() throws Exception {

    JobEntryCopyFiles jobEntry = testInstance(  JOB_ENTRY_NAME );
    JobEntryCopyFiles jobEntry_sameName_completely_different = testInstanceCompletelyDifferent(  JOB_ENTRY_NAME ); // NOTE: same name

    // NOTE: Setting different value to variables
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );
    jobEntry_sameName_completely_different.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_UPDATE_ACCESS_ID );

    // NOTE: should NOT EQUAL string to string value comparison #equals, #hasCode.
    assertNotEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ), jobEntry_sameName_completely_different.getVariable(  POC_KEY_VAR_NAME ) );
    assertNotEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ).hashCode(), jobEntry_sameName_completely_different.getVariable( POC_KEY_VAR_NAME ).hashCode() );

    // NOTE: obviously assert should fail, if  VariableSpace doesn't have custom #hashCode() impl to check contents instead java reference.
    // TODO in current product, this is true, add more variables based on screenshot from live demo, to re-create hashcode improper logic
    // NOTE: current  JobEntryCopyFiles#hashCode -? JobEntryBase#hashCode -> based off JobEntryBase#name
    assertNotEquals( jobEntry.hashCode(), jobEntry_sameName_completely_different.hashCode() );

    // TODO: what class variables and class variables.hashcode() for a job or JobEntryCopyFiles "truly" them different besides JobEntryBase#name ?
    // NOTE: PDI UI prevents adding the same job with an existing job name, most likely Pentaho System design
  }

  @Test
  public void test_Assumptions_3_hashcode_JobEntryCopyFiles_Variables() throws Exception {

    JobEntryCopyFiles jobEntry = new JobEntryCopyFiles( JOB_ENTRY_NAME );
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    VariableSpace variables = new Variables();
    variables.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    // NOTE: should pass string to string value comparison #equals, #hasCode.
    assertEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ), variables.getVariable( POC_KEY_VAR_NAME ) );
    assertEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ).hashCode(), variables.getVariable( POC_KEY_VAR_NAME ).hashCode() );

    // NOTE: obviously assert should fail, if  VariableSpace doesn't have custom #hashCode() impl to check contents instead java reference.
    assertEquals( jobEntry.getVariables().hashCode(), variables.hashCode() );

  }

  @Test
  public void test_Assumptions_4_hashcode_JobEntryCopyFiles_Variables() throws Exception {

    JobEntryCopyFiles jobEntry = new JobEntryCopyFiles( JOB_ENTRY_NAME );
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    VariableSpace variables = new Variables();
    variables.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    // NOTE: should pass string to string value comparison #equals, #hasCode.
    assertEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ), variables.getVariable( POC_KEY_VAR_NAME ) );
    assertEquals( jobEntry.getVariable( POC_KEY_VAR_NAME ).hashCode(), variables.getVariable( POC_KEY_VAR_NAME ).hashCode() );

    // NOTE: again obviously assert should fail, ##hashCode() for different objects should fail
    assertEquals( jobEntry.hashCode(), variables.hashCode() );

  }


  @Test
  public void test_Assumptions_5_getters_JobEntryCopyFiles_Variables() throws Exception {

    PocFileSystemConfigBuilder fileSystemConfigBuilder = new PocFileSystemConfigBuilder();

    JobEntryCopyFiles jobEntry = new JobEntryCopyFiles( JOB_ENTRY_NAME );
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    VariableSpace variables = new Variables();
    variables.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    // NOTE: need to create a new instance for VariableSpace, JobEntry so logic for #equals #hashCode isn't short circuited
    FileSystemOptions fileSystemOptions_jobEntry = testInstance( jobEntry );
    FileSystemOptions fileSystemOptions_variables = testInstance( variables );

    // NOTE: verifying that each key/value are the same
    assertEquals( fileSystemConfigBuilder.getBoolean( fileSystemOptions_jobEntry, KEY_FILESYSTEM_OPTIONS_1 ),
      fileSystemConfigBuilder.getBoolean( fileSystemOptions_variables, KEY_FILESYSTEM_OPTIONS_1 ) );

    assertEquals( fileSystemConfigBuilder.getString( fileSystemOptions_jobEntry, KEY_FILESYSTEM_OPTIONS_2 ),
      fileSystemConfigBuilder.getString( fileSystemOptions_variables, KEY_FILESYSTEM_OPTIONS_2 ) );

    assertEquals( fileSystemConfigBuilder.getInteger( fileSystemOptions_jobEntry, KEY_FILESYSTEM_OPTIONS_3 ),
      fileSystemConfigBuilder.getInteger( fileSystemOptions_variables, KEY_FILESYSTEM_OPTIONS_3 ) );

    assertTrue( fileSystemConfigBuilder.hasParam( fileSystemOptions_jobEntry, KEY_FILESYSTEM_OPTIONS_4 ) );
    assertTrue( fileSystemConfigBuilder.hasParam( fileSystemOptions_variables, KEY_FILESYSTEM_OPTIONS_4 ) );

    VariableSpace variableSpace_jobEntry = fileSystemConfigBuilder.getParam( fileSystemOptions_jobEntry, KEY_FILESYSTEM_OPTIONS_4 );
    VariableSpace variableSpace_variable1 = fileSystemConfigBuilder.getParam( fileSystemOptions_jobEntry, KEY_FILESYSTEM_OPTIONS_4 );
    VariableSpace variableSpace_variable2 = fileSystemConfigBuilder.getParam( fileSystemOptions_variables, KEY_FILESYSTEM_OPTIONS_4 );

    // NOTE: casting to different types doesn't matter, due to #getVariable goes to same obj and returns the value
    assertEquals( variableSpace_variable1.getVariable( POC_KEY_VAR_NAME ), variableSpace_variable2.getVariable( POC_KEY_VAR_NAME ) );
    assertEquals( variableSpace_jobEntry.getVariable( POC_KEY_VAR_NAME ), variableSpace_variable2.getVariable( POC_KEY_VAR_NAME ) );

  }

  @Test
  public void test_Assumptions_6_fileSystemOptions_compareTo_JobEntryCopyFiles_Variables() throws Exception {

    JobEntryCopyFiles jobEntry = new JobEntryCopyFiles( JOB_ENTRY_NAME );
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    VariableSpace variables = new Variables();
    variables.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    // NOTE: need to create a new instance for VariableSpace, JobEntry so logic for #equals #hashCode isn't short circuited
    FileSystemOptions fileSystemOptions_jobEntry = testInstance( jobEntry );
    FileSystemOptions fileSystemOptions_variableSpace = testInstance( variables );

    // NOTE: this will fail due to Arrays.deepHashCode which calls hash code on each value in FileSystemOptions
    // FROM Pentaho logic standpoint these should be equal
    assertEquals(0,  fileSystemOptions_jobEntry.compareTo( fileSystemOptions_variableSpace ) );
  }

  @Test
  public void test_Scenario_1_FileProvider_findFileSystem_JobEntryCopyFiles_Variables() throws Exception {

    PocAbstractFileProvider fileProvider = new PocAbstractFileProvider();

    JobEntryCopyFiles jobEntry = new JobEntryCopyFiles( JOB_ENTRY_NAME );
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    VariableSpace variables = new Variables();
    variables.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    // NOTE: need to create a new instance for VariableSpace, JobEntry so logic for #equals #hashCode isn't short circuited
    FileSystemOptions fileSystemOptions_jobEntry = testInstance( jobEntry );
    FileSystemOptions fileSystemOptions_variables = testInstance( variables );

    FileSystem fileSystem_variables = testInstance( PVFS_URI_1_SRC, fileSystemOptions_variables );

    // NOTE: adding "values" that create a FileSystem
    fileProvider.addFileSystem( PVFS_URI_1_SRC, fileSystem_variables );

    // NOTE: ideally if the same "values" are passed to the system (even if different container class ?) should get back the previous FileSystem
    FileSystem fileSystem_ret = fileProvider.findFileSystem( PVFS_URI_1_SRC, fileSystemOptions_jobEntry );

    assertNotNull( fileSystem_ret );
    assertEquals( fileSystem_variables, fileSystem_ret );

  }

  @Test
  public void test_Scenario_1a_FileProvider_findFileSystem_JobEntryCopyFiles_Variables() throws Exception {

    PocAbstractFileProvider fileProvider = new PocAbstractFileProvider();

    JobEntryCopyFiles jobEntry = new JobEntryCopyFiles( JOB_ENTRY_NAME );
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    VariableSpace variables_from_original_jobEntryGetVariables = jobEntry.getVariables(); // getting variables from original job

    // NOTE: need to create a new instance for VariableSpace, JobEntry so logic for #equals #hashCode isn't short-circuited
    FileSystemOptions fileSystemOptions_jobEntry = testInstance( jobEntry );
    FileSystemOptions fileSystemOptions_variables_from_original_jobEntryGetVariables = testInstance( variables_from_original_jobEntryGetVariables );

    FileSystem fileSystem_variables = testInstance( PVFS_URI_1_SRC, fileSystemOptions_variables_from_original_jobEntryGetVariables );

    // NOTE: adding "values" that create a FileSystem
    fileProvider.addFileSystem( PVFS_URI_1_SRC, fileSystem_variables );

    // NOTE: ideally if the same "values" are passed to the system (even if different container class ?) should get back the previous FileSystem
    FileSystem fileSystem_ret = fileProvider.findFileSystem( PVFS_URI_1_SRC, fileSystemOptions_jobEntry );

    assertNotNull( fileSystem_ret );
    assertEquals( fileSystem_variables, fileSystem_ret );

  }



  @Test
  public void test_Scenario_2_FileProvider_findFileSystem_Variables() throws Exception {

    PocAbstractFileProvider fileProvider = new PocAbstractFileProvider();

    VariableSpace variables1 = new Variables();
    variables1.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    VariableSpace variables2 = new Variables();
    variables2.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    // NOTE: proven earlier BY XYZ these are equal in value

    // NOTE: need to create a new instance for VariableSpace, JobEntry so logic for #equals #hashCode isn't short circuited
    FileSystemOptions fileSystemOptions_variables1 = testInstance( variables1 );
    FileSystemOptions fileSystemOptions_variables2 = testInstance( variables2 );

    FileSystem fileSystem_variables1 = testInstance( PVFS_URI_1_SRC, fileSystemOptions_variables1 );

    // NOTE: adding "values" that create a FileSystem
    fileProvider.addFileSystem( PVFS_URI_1_SRC, fileSystem_variables1 );

    // NOTE: ideally if the same "values" are passed to the system (even when SAME container class ) should get back the previous FileSystem
    FileSystem fileSystem_ret = fileProvider.findFileSystem( PVFS_URI_1_SRC, fileSystemOptions_variables2 );

    assertNotNull( fileSystem_ret );
    assertEquals( fileSystem_variables1, fileSystem_ret );

  }

  @Test
  public void test_Scenario_3_FileProvider_findFileSystem_JobEntryCopyFiles() throws Exception {

    PocAbstractFileProvider fileProvider = new PocAbstractFileProvider();

    JobEntryCopyFiles jobEntry = testInstance( JOB_ENTRY_NAME );
    jobEntry.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_ACCESS_ID );

    // NOTE: same name job name but variable different values
    JobEntryCopyFiles jobEntry_sameName_completely_different = testInstanceCompletelyDifferent(  JOB_ENTRY_NAME );
    jobEntry_sameName_completely_different.setVariable( POC_KEY_VAR_NAME, VALUE_VAR_UPDATE_ACCESS_ID );

    // NOTE: proven earlier BY XYZ these are equal in value

    // NOTE: need to create a new instance for VariableSpace, JobEntry so logic for #equals #hashCode isn't short circuited
    FileSystemOptions fileSystemOptions_jobEntry = testInstance( jobEntry );
    FileSystemOptions fileSystemOptions_jobEntry_sameName_completely_different = testInstance( jobEntry_sameName_completely_different );

    FileSystem fileSystem_jobEntry = testInstance( PVFS_URI_1_SRC, fileSystemOptions_jobEntry );

    // NOTE: adding "values" that create a FileSystem
    fileProvider.addFileSystem( PVFS_URI_1_SRC, fileSystem_jobEntry );

    // NOTE: NOT THE SAME "values" are passed to the system, should get back nothing -> null, then  later logic will create the FileSystem
    FileSystem fileSystem_ret = fileProvider.findFileSystem( PVFS_URI_1_SRC, fileSystemOptions_jobEntry_sameName_completely_different );

    assertNull( fileSystem_ret, "not null filesystem for URI: " + fileSystem_ret.getRootURI() ); // will be fileSystem_jobEntry

  }

  /*************************** HELPER FUNCTION & CLASSES **************/
  public FileSystem testInstance( String uri, FileSystemOptions fileSystemOptions ) {
    AbstractFileName abstractFileName = mock( AbstractFileName.class );
    //  TreeMap#get uses interface Comparable, AbstractFileName#compareTo -> AbstractFileName#getKey -> AbstractFileName#getURI()
    when(  abstractFileName.getURI() ).thenReturn( uri );

    return new ConnectionFileSystem( abstractFileName,
      fileSystemOptions,
      mock( ConnectionManager.class ),
      mock( VFSConnectionManagerHelper.class ) );
  }

  public FileSystemOptions testInstance( VariableSpace variableSpace ) {
    PocFileSystemConfigBuilder fileSystemConfigBuilder = new PocFileSystemConfigBuilder();
    FileSystemOptions fileSystemOptions = new FileSystemOptions();

    fileSystemConfigBuilder.setParam( fileSystemOptions, KEY_FILESYSTEM_OPTIONS_1, VALUE_FILESYSTEM_OPTIONS_BOOLEAN );
    fileSystemConfigBuilder.setParam( fileSystemOptions, KEY_FILESYSTEM_OPTIONS_2, VALUE_FILESYSTEM_OPTIONS_STR );
    fileSystemConfigBuilder.setParam( fileSystemOptions, KEY_FILESYSTEM_OPTIONS_3, VALUE_FILESYSTEM_OPTIONS_INT );

    fileSystemConfigBuilder.setParam( fileSystemOptions, KEY_FILESYSTEM_OPTIONS_4, variableSpace ); // SETTING VARIABLE SPACE

    return fileSystemOptions;
  }

  public JobEntryCopyFiles testInstance( String name ) {
    return testInstanceHelper( name,
      new String[] { PVFS_URI_1_SRC },
      new String[] { PVFS_URI_1_DEST }
    );
  }

  public JobEntryCopyFiles testInstanceCompletelyDifferent( String name ) {
    return testInstanceHelper( name,
      new String[] { PVFS_URI_2_SRC_COMPLETELY_DIFFERENT },
      new String[] { PVFS_URI_2_DEST_COMPLETELY_DIFFERENT }
    );
  }

  public JobEntryCopyFiles testInstanceHelper( String name, String[] source_filefolder, String[] destination_filefolder ) {
    JobEntryCopyFiles jobEntryCopyFiles = new JobEntryCopyFiles( name );
    jobEntryCopyFiles.source_filefolder = source_filefolder;
    jobEntryCopyFiles.destination_filefolder = destination_filefolder;
    Map<String, String> configMappings = new HashMap<>() {{
      put( "config_1" , "featureA" );
      put( "config_2" , "featureB" );
    }};

    jobEntryCopyFiles.setConfigurationMappings( configMappings );

    // HACK-ish
    jobEntryCopyFiles.setParentJob( entry.getParentJob() );
    jobEntryCopyFiles.setParentJobMeta( entry.getParentJobMeta() );
    jobEntryCopyFiles.setMetaStore( entry.getMetaStore() );

    return jobEntryCopyFiles;
  }


  /**
   * POC class exposing some getters/setters
   */
  public class PocFileSystemConfigBuilder extends FileSystemConfigBuilder {

    @Override protected Class<? extends FileSystem> getConfigClass() {
      return FileSystem.class;
    }

    @Override
    public void setParam( FileSystemOptions opts, String name, Object value ) {
      super.setParam( opts, name, value );
    }

    @Override
    public String getString( final FileSystemOptions fileSystemOptions, final String name ) {
      return super.getString(fileSystemOptions, name, null );
    }
    @Override
    public Boolean getBoolean( final FileSystemOptions fileSystemOptions, final String name ) {
      return super.getBoolean( fileSystemOptions, name, null );
    }

    @Override
    public Integer getInteger( final FileSystemOptions fileSystemOptions, final String name ) {
      return super.getInteger( fileSystemOptions, name, null );
    }
    @Override
    public <T> T getParam( final FileSystemOptions fileSystemOptions, final String name) {
      return super.getParam( fileSystemOptions, name  );
    }
    @Override
    protected boolean hasParam( final FileSystemOptions fileSystemOptions, final String name ) {
      return super.hasParam( fileSystemOptions, name );
    }

  }

  public class PocAbstractFileProvider extends AbstractFileProvider {

    @Override
    public FileSystem findFileSystem( final Comparable<?> key, final FileSystemOptions fileSystemOptions ) {
      return super.findFileSystem( key, fileSystemOptions );
    }

    @Override
    public void addFileSystem( final Comparable<?> key, final FileSystem fs ) throws FileSystemException {
      super.addFileSystem( key, fs );
    }

    @Override public FileObject findFile( FileObject baseFile, String uri, FileSystemOptions fileSystemOptions )
      throws FileSystemException {
      throw new RuntimeException( "not implemented for POC" );
      // return null;
    }

    @Override public Collection<Capability> getCapabilities() {
      throw new RuntimeException( "not implemented for POC" );
      //return null;
    }
  }

}
