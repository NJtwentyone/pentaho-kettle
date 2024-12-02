package org.pentaho.di.connections.common.basic;

import org.pentaho.di.connections.vfs.BaseVFSConnectionDetails;
import org.pentaho.di.core.variables.VariableSpace;

abstract class TestBaseVFSConnectionDetails extends BaseVFSConnectionDetails {

  VariableSpace space;

  @Override public VariableSpace getSpace() {
    return space;
  }

  @Override public void setSpace( VariableSpace space ) {
    this.space = space;
  }

}
