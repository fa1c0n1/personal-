package com.apple.aml.stargate.pipeline.sdk.metrics.wrappers;

import freemarker.template.TemplateDirectiveModel;

public interface BaseTemplateDirectiveModel extends TemplateDirectiveModel {
    String type();
}
