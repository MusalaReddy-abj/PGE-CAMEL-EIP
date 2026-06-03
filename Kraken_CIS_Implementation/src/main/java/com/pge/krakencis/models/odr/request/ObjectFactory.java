package com.pge.krakencis.models.odr.request;

import jakarta.xml.bind.annotation.XmlRegistry;

/**
 * JAXB ObjectFactory required so that JAXBContext.newInstance(packageName)
 * can discover annotated classes in this package.
 */
@XmlRegistry
public class ObjectFactory {

    public OdrRequest         createOdrRequest()         { return new OdrRequest(); }
    public OdrHeader          createOdrHeader()          { return new OdrHeader(); }
    public OdrPayload         createOdrPayload()         { return new OdrPayload(); }
    public OdrMeterReadings   createOdrMeterReadings()   { return new OdrMeterReadings(); }
    public OdrEndDeviceAsset  createOdrEndDeviceAsset()  { return new OdrEndDeviceAsset(); }
    public OdrDateRange       createOdrDateRange()       { return new OdrDateRange(); }
}
