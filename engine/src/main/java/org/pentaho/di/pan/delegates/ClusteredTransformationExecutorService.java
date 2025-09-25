// TODO add headers
package org.pentaho.di.pan.delegates; // TODO consider moving to different or subpackage just for this interface and its implementations

import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.cluster.TransSplitter;

// TODO javadoc class and methods
class ClusteredTransformationExecutorService implements TransformationExecutorService {
  private static Class<?> pkg = ClusteredTransformationExecutorService.class;

  TransSplitterExecutionService transSplitterExecutionService;

  public ClusteredTransformationExecutorService() {
    this( new TransSplitterExecutionService());
  }

  public ClusteredTransformationExecutorService( TransSplitterExecutionService transSplitterExecutionService )  {
    this.transSplitterExecutionService = transSplitterExecutionService;
  }

  /**
   * Execute transformation in clustered mode.
   */
  // POC NOTE: not sure if we need to pass the LogChannelInterface, but it's here as 'extLog' in the current code
  @Override
  public Result execute( LogChannelInterface extLog, TransMeta transMeta,
                         TransExecutionConfiguration executionConfiguration, String[] arguments ) throws KettleException {

    extLog.logBasic( BaseMessages.getString( pkg, "PanTransformationDelegate.Log.ExecutingClustered" ) );

    try {
      final TransSplitter transSplitter = new TransSplitter( transMeta );
      transSplitter.splitOriginalTransformation();

      return executeClustered( extLog, transMeta, transSplitter, executionConfiguration );

    } catch ( Exception e ) { // FIXME rarely should you catch generic Exception and cast in KettleException, if there is a runtime exception it should be proop
      throw new KettleException( e );
    }
  }

  /**
   * Execute transformation in clustered mode.
   */ // FIXME FOCUS on testing this method
  protected Result executeClustered( LogChannelInterface extLog, TransMeta transMeta, TransSplitter transSplitter,
                                     TransExecutionConfiguration executionConfiguration ) throws KettleException {

    extLog.logBasic( BaseMessages.getString( pkg, "PanTransformationDelegate.Log.ExecutingClustered" ) );

    try {
      // Inject certain internal variables to make it more intuitive
      for ( String transVar : Const.INTERNAL_TRANS_VARIABLES ) {
        executionConfiguration.getVariables().put( transVar, transMeta.getVariable( transVar ) );
      }

      // Parameters override the variables
      TransMeta originalTransformation = transSplitter.getOriginalTransformation();
      for ( String param : originalTransformation.listParameters() ) {
        String value = Const.NVL( originalTransformation.getParameterValue( param ),
          Const.NVL( originalTransformation.getParameterDefault( param ),
            originalTransformation.getVariable( param ) ) );
        if ( !Utils.isEmpty( value ) ) {
          executionConfiguration.getVariables().put( param, value );
        }
      }

      // POC NOTE: all this logic was calls to static TransMethod with little interaction
      // NOW just test and verify behavior of transSplitterExecutionService#executeClustered
      return transSplitterExecutionService.executeClustered(
        extLog, transSplitter, null, executionConfiguration
      );

    } catch ( Exception e ) {
      throw new KettleException( e );
    }
  }

  // POC NOTE: commenting out to better compare with TransSplitterExecutionService
//  public void executeClustered( LogChannelInterface extLog, TransSplitter transSplitter, TransExecutionConfiguration executionConfiguration )
//    throws KettleException {
//    // Execute clustered transformation
//    try {
//      Trans.executeClustered( transSplitter, executionConfiguration );
//    } catch ( Exception e ) {
//      cleanupClusterAfterError( extLog, transSplitter, e );
//    }
//  }
//  public void cleanupClusterAfterError( LogChannelInterface extLog, TransSplitter transSplitter, Exception e ) throws KettleException {
//    // Clean up cluster in case of error
//    try {
//      Trans.cleanupCluster( extLog, transSplitter );
//    } catch ( Exception cleanupException ) {
//      throw new KettleException( "Error executing transformation and error cleaning up cluster", e );
//    }
//  }

  // POC NOTE: Wrapper class to encapsulate the Trans static methods
  // 1:1 on Trans static methods for easier mocking/testing
  // this logic should ideally be moved to TransExecutionConfiguration or a helper class
  // TODO move to separate file kept in same package for easier tracking
  // TODO use mockito mockstatic for testing - https://www.baeldung.com/mockito-mock-static-methods
  public static class TransSplitterExecutionService {

    public Result executeClustered( LogChannelInterface extLog, TransSplitter transSplitter, Job parentJob,
                                    TransExecutionConfiguration executionConfiguration ) throws KettleException {
      executeClustered( extLog, transSplitter, executionConfiguration );
      // Monitor clustered transformation
      Trans.monitorClusteredTransformation( extLog, transSplitter, parentJob ); // TODO should be able to test using Mockitos MockedStatic
      return Trans.getClusteredTransformationResult( extLog, transSplitter, parentJob );
    }

    protected void executeClustered(  LogChannelInterface extLog, TransSplitter transSplitter, TransExecutionConfiguration executionConfiguration )
      throws KettleException {
      // Execute clustered transformation
      try {
        Trans.executeClustered( transSplitter, executionConfiguration );
      } catch ( Exception e ) {
        cleanupClusterAfterError( extLog, transSplitter, e );
      }
    }

    protected void cleanupClusterAfterError(  LogChannelInterface extLog, TransSplitter transSplitter, Exception e ) throws KettleException {
      // Clean up cluster in case of error
      try {
        Trans.cleanupCluster( extLog, transSplitter );
      } catch ( Exception cleanupException ) {
        throw new KettleException( "Error executing transformation and error cleaning up cluster", e );
      }
    }

  }
}
