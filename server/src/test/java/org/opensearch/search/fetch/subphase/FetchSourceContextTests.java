/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class FetchSourceContextTests extends OpenSearchTestCase {

    // -----------------------------------------------------------------------
    // fromXContent – VALUE_BOOLEAN
    // -----------------------------------------------------------------------

    public void testFromXContentBooleanTrue() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "true")) {
            parser.nextToken(); // advance to VALUE_BOOLEAN
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertThat(ctx, sameInstance(FetchSourceContext.FETCH_SOURCE));
            assertTrue(ctx.fetchSource());
        }
    }

    public void testFromXContentBooleanFalse() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "false")) {
            parser.nextToken(); // advance to VALUE_BOOLEAN
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertThat(ctx, sameInstance(FetchSourceContext.DO_NOT_FETCH_SOURCE));
            assertFalse(ctx.fetchSource());
        }
    }

    // -----------------------------------------------------------------------
    // fromXContent – VALUE_STRING (single include pattern)
    // -----------------------------------------------------------------------

    public void testFromXContentSingleStringInclude() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "\"field1.*\"")) {
            parser.nextToken(); // advance to VALUE_STRING
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1.*"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentSingleStringWithWildcard() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "\"obj.*\"")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("obj.*"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    // -----------------------------------------------------------------------
    // fromXContent – START_ARRAY (multiple include patterns)
    // -----------------------------------------------------------------------

    public void testFromXContentArrayOfIncludes() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "[\"field1\", \"field2\"]")) {
            parser.nextToken(); // advance to START_ARRAY
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContainingInAnyOrder("field1", "field2"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentEmptyArray() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "[]")) {
            parser.nextToken(); // advance to START_ARRAY
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentArrayWithNonStringThrows() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "[\"field1\", 42]")) {
            parser.nextToken(); // advance to START_ARRAY
            ParsingException ex = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertTrue(ex.getMessage().contains("Unknown key for a"));
        }
    }

    // -----------------------------------------------------------------------
    // fromXContent – START_OBJECT with includes/excludes as arrays
    // -----------------------------------------------------------------------

    public void testFromXContentObjectWithIncludesArray() throws IOException {
        String json = "{\"includes\": [\"field1\", \"field2\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // advance to START_OBJECT
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContainingInAnyOrder("field1", "field2"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentObjectWithExcludesArray() throws IOException {
        String json = "{\"excludes\": [\"secret.*\", \"internal.*\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // advance to START_OBJECT
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), arrayContainingInAnyOrder("secret.*", "internal.*"));
        }
    }

    public void testFromXContentObjectWithBothIncludesAndExcludesArrays() throws IOException {
        String json = "{\"includes\": [\"field1\", \"field2\"], \"excludes\": [\"field3\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // advance to START_OBJECT
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContainingInAnyOrder("field1", "field2"));
            assertThat(ctx.excludes(), arrayContaining("field3"));
        }
    }

    // -----------------------------------------------------------------------
    // fromXContent – START_OBJECT with includes/excludes as single strings
    // -----------------------------------------------------------------------

    public void testFromXContentObjectWithIncludesString() throws IOException {
        String json = "{\"includes\": \"field1.*\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1.*"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentObjectWithExcludesString() throws IOException {
        String json = "{\"excludes\": \"sensitive.*\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), arrayContaining("sensitive.*"));
        }
    }

    public void testFromXContentObjectWithBothIncludesAndExcludesStrings() throws IOException {
        String json = "{\"includes\": \"path.inner.*\", \"excludes\": \"another.inner.*\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("path.inner.*"));
            assertThat(ctx.excludes(), arrayContaining("another.inner.*"));
        }
    }

    // -----------------------------------------------------------------------
    // fromXContent – START_OBJECT with deprecated field name aliases
    // -----------------------------------------------------------------------

    public void testFromXContentObjectWithDeprecatedIncludeAlias() throws IOException {
        String json = "{\"include\": [\"field1\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1"));
        }
    }

    public void testFromXContentObjectWithDeprecatedExcludeAlias() throws IOException {
        String json = "{\"exclude\": [\"field2\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.excludes(), arrayContaining("field2"));
        }
    }

    // -----------------------------------------------------------------------
    // fromXContent – START_OBJECT empty
    // -----------------------------------------------------------------------

    public void testFromXContentEmptyObject() throws IOException {
        String json = "{}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    // -----------------------------------------------------------------------
    // fromXContent – invalid / unknown tokens throw ParsingException
    // -----------------------------------------------------------------------

    public void testFromXContentUnknownTopLevelTokenThrows() throws IOException {
        // A numeric value is not a valid top-level token for FetchSourceContext
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "42")) {
            parser.nextToken();
            ParsingException ex = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertTrue(ex.getMessage().contains("Expected one of"));
        }
    }

    public void testFromXContentObjectUnknownFieldInArrayThrows() throws IOException {
        // An unknown field name inside the object with an array value
        String json = "{\"unknown_field\": [\"field1\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            ParsingException ex = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertTrue(ex.getMessage().contains("Unknown key for a"));
        }
    }

    public void testFromXContentObjectUnknownFieldInStringThrows() throws IOException {
        String json = "{\"unknown_field\": \"field1\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            ParsingException ex = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertTrue(ex.getMessage().contains("Unknown key for a"));
        }
    }

    public void testFromXContentObjectInvalidValueTypeThrows() throws IOException {
        // Boolean inside object value (not a valid value type)
        String json = "{\"includes\": true}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            ParsingException ex = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertTrue(ex.getMessage().contains("Unknown key for a"));
        }
    }

    public void testFromXContentArrayWithNonStringElementInObjectThrows() throws IOException {
        String json = "{\"includes\": [\"valid\", 123]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            ParsingException ex = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertTrue(ex.getMessage().contains("Unknown key for a"));
        }
    }

    // -----------------------------------------------------------------------
    // parseFromRestRequest – changed logic: fetchSource == null || fetchSource
    // -----------------------------------------------------------------------

    public void testParseFromRestRequestNoParams() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertThat(ctx, nullValue());
    }

    public void testParseFromRestRequestSourceTrue() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), emptyArray());
        assertThat(ctx.excludes(), emptyArray());
    }

    public void testParseFromRestRequestSourceFalse() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "false");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertFalse(ctx.fetchSource());
    }

    /**
     * When _source is not specified but includes are, fetchSource must default to true.
     * This tests the changed logic: fetchSource == null || fetchSource
     */
    public void testParseFromRestRequestNullFetchSourceWithIncludesDefaultsToTrue() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_includes", "field1,field2");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), arrayContainingInAnyOrder("field1", "field2"));
        assertThat(ctx.excludes(), emptyArray());
    }

    /**
     * When _source is not specified but excludes are, fetchSource must default to true.
     * This tests the changed logic: fetchSource == null || fetchSource
     */
    public void testParseFromRestRequestNullFetchSourceWithExcludesDefaultsToTrue() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_excludes", "secret.*");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.excludes(), arrayContaining("secret.*"));
        assertThat(ctx.includes(), emptyArray());
    }

    public void testParseFromRestRequestWithIncludesAndExcludes() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_includes", "field1");
        params.put("_source_excludes", "field2");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), arrayContaining("field1"));
        assertThat(ctx.excludes(), arrayContaining("field2"));
    }

    /**
     * When _source is a field pattern (not boolean), it is treated as an include list.
     * fetchSource must remain true.
     */
    public void testParseFromRestRequestSourceAsPattern() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "obj.*");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), arrayContaining("obj.*"));
    }

    /**
     * Regression test: fetchSource=true combined with includes/excludes still returns fetchSource=true.
     * Tests the changed boolean expression path where fetchSource is explicitly true.
     */
    public void testParseFromRestRequestExplicitTrueWithIncludes() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "true");
        params.put("_source_includes", "field1");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), arrayContaining("field1"));
    }

    /**
     * Boundary case: fetchSource=false with excludes — fetchSource must remain false.
     */
    public void testParseFromRestRequestExplicitFalseWithExcludes() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "false");
        params.put("_source_excludes", "field1");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertFalse(ctx.fetchSource());
        assertThat(ctx.excludes(), arrayContaining("field1"));
    }
}