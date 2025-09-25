// TODO headers
package org.pentaho.di.pan.delegates; // TODO consider moving to different or subpackage just for this interface and its implementations

import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
// TODO javadoc class and methods
// POC NOTE: creating interface, current code is basically set up to use a factory
// to create different execution services based on the execution type
// Created this class in this file to avoid creating multiple new files for the POC
// should refactor to separate files if we decide to go this route
interface TransformationExecutorService {
  Result execute( LogChannelInterface log, TransMeta transMeta, TransExecutionConfiguration executionConfiguration, String[] arguments ) throws
    KettleException;
}
