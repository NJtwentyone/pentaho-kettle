package org.pentaho.di.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;

public interface TransMananger  {
  Trans createTrans( TransMeta transMeta, LoggingObjectInterface log, TransExecutionConfiguration transExecutionConfiguration) throws
    KettleException;
  /*
   * TODO haven't decide to stick with string value or Class object for key
   */
  boolean registerFactory(String RunConfigurationType, TransFactory transFactory);

  TransFactory getTransFactory((String RunConfigurationType);
}
