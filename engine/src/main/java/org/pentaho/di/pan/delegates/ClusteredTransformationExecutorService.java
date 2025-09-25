// TODO add headers
package org.pentaho.di.pan.delegates; // TODO consider moving to different or subpackage just for this interface and its implementations

import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.cluster.TransSplitter;

// TODO javadoc class and methods
class ClusteredTransformationExecutorService implements TransformationExecutorService {
  private static Class<?> pkg = ClusteredTransformationExecutorService.class;

  public Result execute( LogChannelInterface log, TransMeta transMeta,
                         TransExecutionConfiguration executionConfiguration, String[] arguments ) throws
    KettleException {
    log.logBasic(
      BaseMessages.getString( pkg, "PanTransformationDelegate.Log.ExecutingClustered" ) );

    try {
      final TransSplitter transSplitter = new TransSplitter( transMeta );
      transSplitter.splitOriginalTransformation();

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
      executeClustered( log, transSplitter, executionConfiguration );
      // Monitor clustered transformation
      Trans.monitorClusteredTransformation( log, transSplitter, null );
      Result result = Trans.getClusteredTransformationResult( log, transSplitter, null );

      //logClusteredResults( transMeta, result ); //  POC NOTE: moved this to back to pan delegate

      return result;

    } catch ( Exception e ) {
      throw new KettleException( e );
    }
  }

  public void executeClustered( LogChannelInterface log, TransSplitter transSplitter, TransExecutionConfiguration executionConfiguration )
    throws KettleException {
    // Execute clustered transformation
    try {
      Trans.executeClustered( transSplitter, executionConfiguration );
    } catch ( Exception e ) {
      cleanupClusterAfterError( log, transSplitter, e );
    }
  }
  public void cleanupClusterAfterError( LogChannelInterface log, TransSplitter transSplitter, Exception e ) throws KettleException {
    // Clean up cluster in case of error
    try {
      Trans.cleanupCluster( log, transSplitter );
    } catch ( Exception cleanupException ) {
      throw new KettleException( "Error executing transformation and error cleaning up cluster", e );
    }
  }
}
