/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.groar.processors.wwwformjson;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"www-form", "json"})
@CapabilityDescription("Transforms www-form data to JSON")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ConvertWWWFormToJSON extends AbstractProcessor {
    private final static String DEFAULT_CHARSET = "UTF-8";

    static final AllowableValue TARGET_FORMAT_JSON = new AllowableValue("json", "json", "Converts incoming data into JSON format");
    static final AllowableValue SOURCE_ATTRIBUTE = new AllowableValue("attribute", "Attribute", "Reads content from attribute");
    static final AllowableValue SOURCE_FLOWFILE = new AllowableValue("flowfile", "Flowfile", "Reads content from flowfile");

    public static final PropertyDescriptor TARGET_FORMAT = new PropertyDescriptor
            .Builder().name("TARGET_FORMAT")
            .displayName("Target format")
            .description("Specifies the target format")
            .allowableValues(TARGET_FORMAT_JSON)
            .defaultValue("json")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTENT_SOURCE = new PropertyDescriptor
            .Builder().name("CONTENT_SOURCE")
            .displayName("Content source")
            .description("Where to get the content form")
            .allowableValues(SOURCE_ATTRIBUTE, SOURCE_FLOWFILE)
            .defaultValue("flowfile")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONTENT_ATTRIBUTE = new PropertyDescriptor
            .Builder().name("CONTENT_ATTRIBUTE")
            .displayName("Content attribute")
            .description("Which attribute name holds the content")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Transformation was successful")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Transformation failed")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(TARGET_FORMAT);
        descriptors.add(CONTENT_SOURCE);
        descriptors.add(CONTENT_ATTRIBUTE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if ( original == null ) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        String[] jsonString = new String[1];
        final byte[] originalContent = new byte[(int) original.getSize()];

        session.read(original, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                String content = "";
                switch(context.getProperty(CONTENT_SOURCE).getValue()) {
                    case "attribute":
                        content = original.getAttribute(context.getProperty(CONTENT_ATTRIBUTE).getValue());
                        break;
                    case "flowfile":
                        StreamUtils.fillBuffer(in, originalContent, true);
                        content = new String(originalContent);
                        break;
                }
                try {
                    JSONObject json = new JSONObject();
                    List<NameValuePair> formDataObject = URLEncodedUtils.parse(new URI(null, null, null, content, null), "utf-8");
                    Iterator<NameValuePair> formDataObjectIterator = formDataObject.iterator();
                    while(formDataObjectIterator.hasNext()) {
                        NameValuePair value = formDataObjectIterator.next();
                        String strKey = value.getName();
                        String strValue = value.getValue();
                        try {
                            if (NumberUtils.isNumber(strValue)) {
                                json.put(strKey, Float.parseFloat(strValue));
                            } else {
                                json.put(strKey, strValue);
                            }
                        } catch (JSONException e) {
                            logger.warn("Caught exception while filling json data for key {}: {}", new Object[]{strKey, strValue});
                        }
                    }
                    jsonString[0] = json.toString();
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
            }
        });

        FlowFile transformed = session.write(original, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(jsonString[0].getBytes(DEFAULT_CHARSET));
            }
        });
        String targetFormat = "";
        switch(context.getProperty(TARGET_FORMAT).getValue()) {
            case "json":
                targetFormat = "application/json";
                break;
        }
        transformed = session.putAttribute(transformed, "mime.type", targetFormat);
        session.transfer(transformed, REL_SUCCESS);
        session.getProvenanceReporter().modifyContent(transformed,"www-data-urlencoded converted to json", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
    }
}
