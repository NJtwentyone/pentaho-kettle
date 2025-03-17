package org.pentaho.di.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;

// TODO logically there should be org.pentaho.di.job.JobFactory and related classes

/**
 * POC for Trans.java factory
 *
 */
public interface TransFactory {
  /*
    Decided on TransExecutionConfiguration instead of directly using RunConfiguration
     -  TransExecutionConfiguration is in same package as Trana,java, RunConfiguration is in different package
     -  Can get RunConfiguration from TransExecutionConfiguration#runConfiguration
     - TransExecutionConfiguration has more information
     - - However UI only presents RunConfiguration#getName() and RunConfiguration#getType() might be too much
   */
  /*
    TODO look into if KettleException is too generic or if we need a more specific one that extends it
        class TransFactory KettleException extends  KettleException
   */

  Trans create( TransMeta transMeta, LoggingObjectInterface log, TransExecutionConfiguration transExecutionConfiguration) throws
    KettleException;
}
