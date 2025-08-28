package org.pentaho.di.ui.api.widget;

import org.eclipse.swt.widgets.Widget;

// TextWidget.java
public interface TextWidget<T,S> extends GenericWidget<S> {
  T getText();
  void setText(T text);
}
