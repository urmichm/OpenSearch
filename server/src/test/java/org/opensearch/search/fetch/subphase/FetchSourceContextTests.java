/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.fetch.subphase;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FetchSourceContextTests extends OpenSearchTestCase {

    // -------------------------------------------------------------------------
    // parseFromRestRequest – changed logic: `fetchSource == null || fetchSource`
    // -------------------------------------------------------------------------

    public void testParseFromRestRequest_noParamsReturnsNull() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
        assertNull(FetchSourceContext.parseFromRestRequest(request));
    }

    public void testParseFromRestRequest_sourceTrue() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "true");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
    }

    public void testParseFromRestRequest_sourceFalse() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "false");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertFalse(ctx.fetchSource());
    }

    /**
     * When only _source_includes is set (fetchSource param is null),
     * the changed expression `fetchSource == null || fetchSource` evaluates to true.
     */
    public void testParseFromRestRequest_onlyIncludesProvided_fetchSourceDefaultsToTrue() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_includes", "field1,field2");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue("fetchSource should default to true when not explicitly set", ctx.fetchSource());
        assertArrayEquals(new String[] { "field1", "field2" }, ctx.includes());
    }

    /**
     * When only _source_excludes is set (fetchSource param is null),
     * the changed expression `fetchSource == null || fetchSource` evaluates to true.
     */
    public void testParseFromRestRequest_onlyExcludesProvided_fetchSourceDefaultsToTrue() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_excludes", "field1");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue("fetchSource should default to true when not explicitly set", ctx.fetchSource());
        assertArrayEquals(new String[] { "field1" }, ctx.excludes());
    }

    /**
     * Explicit _source=false overrides the null-default, so fetchSource must be false
     * even when includes/excludes are also present.
     */
    public void testParseFromRestRequest_sourceFalseWithIncludes_fetchSourceIsFalse() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "false");
        params.put("_source_includes", "field1");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertFalse("Explicit false should keep fetchSource=false", ctx.fetchSource());
        assertArrayEquals(new String[] { "field1" }, ctx.includes());
    }

    public void testParseFromRestRequest_sourceTrueWithBothIncludesAndExcludes() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "true");
        params.put("_source_includes", "inc1,inc2");
        params.put("_source_excludes", "exc1");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertArrayEquals(new String[] { "inc1", "inc2" }, ctx.includes());
        assertArrayEquals(new String[] { "exc1" }, ctx.excludes());
    }

    // -------------------------------------------------------------------------
    // fromXContent – refactored to switch expression; VALUE_BOOLEAN returns
    // static instances FETCH_SOURCE / DO_NOT_FETCH_SOURCE
    // -------------------------------------------------------------------------

    public void testFromXContent_booleanTrue_returnsFetchSourceStaticInstance() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "true")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertSame(
                "true should return the FETCH_SOURCE static constant",
                FetchSourceContext.FETCH_SOURCE,
                ctx
            );
            assertTrue(ctx.fetchSource());
        }
    }

    public void testFromXContent_booleanFalse_returnsDoNotFetchSourceStaticInstance() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "false")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertSame(
                "false should return the DO_NOT_FETCH_SOURCE static constant",
                FetchSourceContext.DO_NOT_FETCH_SOURCE,
                ctx
            );
            assertFalse(ctx.fetchSource());
        }
    }

    public void testFromXContent_singleStringInclude() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "\"field1\"")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[] { "field1" }, ctx.includes());
            assertArrayEquals(new String[0], ctx.excludes());
        }
    }

    public void testFromXContent_arrayOfIncludes() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "[\"field1\",\"field2\"]")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[] { "field1", "field2" }, ctx.includes());
            assertArrayEquals(new String[0], ctx.excludes());
        }
    }

    public void testFromXContent_emptyArray() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "[]")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[0], ctx.includes());
        }
    }

    /** Unexpected token type (integer) should throw ParsingException */
    public void testFromXContent_unexpectedToken_throwsParsingException() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "42")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    // -------------------------------------------------------------------------
    // fromXContent with START_OBJECT – delegates to parseSourceObject()
    // -------------------------------------------------------------------------

    public void testFromXContent_objectWithIncludesArray() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"includes\":[\"f1\",\"f2\"]}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[] { "f1", "f2" }, ctx.includes());
            assertArrayEquals(new String[0], ctx.excludes());
        }
    }

    public void testFromXContent_objectWithExcludesArray() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"excludes\":[\"e1\",\"e2\"]}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[0], ctx.includes());
            assertArrayEquals(new String[] { "e1", "e2" }, ctx.excludes());
        }
    }

    public void testFromXContent_objectWithBothIncludesAndExcludes() throws IOException {
        String json = "{\"includes\":[\"inc1\",\"inc2\"],\"excludes\":[\"exc1\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[] { "inc1", "inc2" }, ctx.includes());
            assertArrayEquals(new String[] { "exc1" }, ctx.excludes());
        }
    }

    /** Single string value (not array) for includes inside object */
    public void testFromXContent_objectWithIncludesSingleString() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"includes\":\"field1\"}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[] { "field1" }, ctx.includes());
            assertArrayEquals(new String[0], ctx.excludes());
        }
    }

    /** Single string value for excludes inside object */
    public void testFromXContent_objectWithExcludesSingleString() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"excludes\":\"field2\"}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[0], ctx.includes());
            assertArrayEquals(new String[] { "field2" }, ctx.excludes());
        }
    }

    /** Empty object → includes and excludes both empty, fetchSource always true */
    public void testFromXContent_emptyObject_fetchSourceIsTrue() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[0], ctx.includes());
            assertArrayEquals(new String[0], ctx.excludes());
        }
    }

    // -------------------------------------------------------------------------
    // Deprecated field names: "include" / "exclude"
    // -------------------------------------------------------------------------

    public void testFromXContent_deprecatedIncludeFieldName() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"include\":[\"f1\"]}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[] { "f1" }, ctx.includes());
        }
    }

    public void testFromXContent_deprecatedExcludeFieldName() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"exclude\":[\"e1\"]}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertArrayEquals(new String[] { "e1" }, ctx.excludes());
        }
    }

    // -------------------------------------------------------------------------
    // Error cases for parseSourceObject (via fromXContent)
    // -------------------------------------------------------------------------

    /** Unknown field key inside object should throw ParsingException */
    public void testFromXContent_objectWithUnknownFieldKey_throwsParsingException() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"unknown\":[\"f1\"]}")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    /** Unknown string value field inside object should throw ParsingException */
    public void testFromXContent_objectWithUnknownStringField_throwsParsingException() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"unknown\":\"value\"}")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    /** An unexpected token type for a field value (e.g. boolean inside object) should throw */
    public void testFromXContent_objectWithBooleanFieldValue_throwsParsingException() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"includes\":true}")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    // -------------------------------------------------------------------------
    // Error cases for parseSourceArray (via fromXContent)
    // -------------------------------------------------------------------------

    /** A non-string element inside a top-level includes array should throw ParsingException */
    public void testFromXContent_arrayWithNonStringElement_throwsParsingException() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "[\"field1\",42]")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    /** A non-string element inside an includes array in an object should throw ParsingException */
    public void testFromXContent_objectIncludesArrayWithNonStringElement_throwsParsingException() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"includes\":[\"f1\",123]}")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    /** A non-string element inside an excludes array in an object should throw ParsingException */
    public void testFromXContent_objectExcludesArrayWithNonStringElement_throwsParsingException() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"excludes\":[true]}")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    // -------------------------------------------------------------------------
    // Boundary / regression cases
    // -------------------------------------------------------------------------

    /** Wildcard includes are preserved as-is through fromXContent array parsing */
    public void testFromXContent_wildcardIncludesPreserved() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "[\"field.*\",\"meta*\"]")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertArrayEquals(new String[] { "field.*", "meta*" }, ctx.includes());
        }
    }

    /** parseSourceObject always returns fetchSource=true regardless of content */
    public void testFromXContent_objectFetchSourceIsAlwaysTrue() throws IOException {
        // An object with only excludes should still have fetchSource=true
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"excludes\":[\"hidden\"]}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue("parseSourceObject must always produce fetchSource=true", ctx.fetchSource());
        }
    }

    /** Regression: ensure the null-or-boolean change in parseFromRestRequest
     *  doesn't accidentally set fetchSource=false when _source param is absent
     *  but _source_includes IS present (null || false == false is NOT the case here
     *  since the expression is `Boolean == null || Boolean`). */
    public void testParseFromRestRequest_nullFetchSourceWithExcludes_fetchSourceIsTrue() {
        // fetchSource is null (not passed), only excludes provided
        // `fetchSource == null || fetchSource` → `true || <anything>` → true
        Map<String, String> params = new HashMap<>();
        params.put("_source_excludes", "secret_field");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue("fetchSource must be true when _source param is absent", ctx.fetchSource());
        assertArrayEquals(new String[] { "secret_field" }, ctx.excludes());
    }
}