package org.pentaho.di.ui.api.widget;

import org.eclipse.swt.widgets.Widget;

// GroupWidget.java
public interface GroupWidget<T, S> extends GenericWidget<S> {
  T getText();
  void setText( T label );
  S getGroup(); // FIXME duplicate of getUnderlyingWidget() ??
}
