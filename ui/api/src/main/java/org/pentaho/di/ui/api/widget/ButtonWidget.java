package org.pentaho.di.ui.api.widget;

// ButtonWidget.java
public interface ButtonWidget<T> extends GenericWidget<T> {
  boolean isEnabled();
  void setEnabled( boolean enabled );
  /*
  TODO SWT uses .addSelectionListener or .addListener,
   not sure what click() would correspond to in SWT
   maybe it would just be a empty function called inside the listener
   */
  void click(); // SWT uses .addSelectionListener or .addListener,  not sure what click() would correspond to in SWT, maybe it ou
}
