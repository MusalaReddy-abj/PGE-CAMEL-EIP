package com.pge.krakencis.processors;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseProcessor implements Processor {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public abstract void process(Exchange exchange) throws Exception;
}
