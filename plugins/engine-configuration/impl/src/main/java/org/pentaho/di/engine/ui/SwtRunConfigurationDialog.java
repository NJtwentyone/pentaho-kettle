// Example: ui/api/src/main/java/org/pentaho/di/ui/api/widget/StringGroupRunConfigurationDialog.java
package org.pentaho.di.engine.ui;

import org.pentaho.di.engine.configuration.api.RunConfigurationDialog;
import org.pentaho.di.ui.core.widget.SwtButtonAdapter;
import org.pentaho.di.ui.core.widget.SwtGroupAdapter;
import org.pentaho.di.ui.core.widget.SwtTextAdapter;

public interface SwtRunConfigurationDialog extends RunConfigurationDialog {
  // You can add extra methods if needed, or just use as a type alias
  SwtTextAdapter getName();
  // Engine Dropdown ??
  SwtGroupAdapter getGroup();
  SwtButtonAdapter getOKButton(); //org.pentaho.di.ui.core.widget.InputButton
  SwtButtonAdapter getCancelButton();
}

