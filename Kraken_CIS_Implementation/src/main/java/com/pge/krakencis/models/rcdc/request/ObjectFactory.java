package com.pge.krakencis.models.rcdc.request;

import jakarta.xml.bind.annotation.XmlRegistry;

/**
 * JAXB ObjectFactory required so that JAXBContext.newInstance(packageName)
 * can discover annotated classes in this package.
 */
@XmlRegistry
public class ObjectFactory {

    public RcdcRequest          createRcdcRequest()          { return new RcdcRequest(); }
    public RcdcHeader           createRcdcHeader()           { return new RcdcHeader(); }
    public RcdcPayload          createRcdcPayload()          { return new RcdcPayload(); }
    public RcdcSwitchState      createRcdcSwitchState()      { return new RcdcSwitchState(); }
    public RcdcEndDeviceAsset   createRcdcEndDeviceAsset()   { return new RcdcEndDeviceAsset(); }
    public RcdcAcknowledgement  createRcdcAcknowledgement()  { return new RcdcAcknowledgement(); }
}
