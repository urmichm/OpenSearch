/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for the refactored {@link FetchSourceContext#fromXContent} and
 * {@link FetchSourceContext#parseFromRestRequest} methods introduced in the PR.
 */
public class FetchSourceContextTests extends OpenSearchTestCase {

    // -------------------------------------------------------------------------
    // fromXContent – VALUE_BOOLEAN branch
    // -------------------------------------------------------------------------

    public void testFromXContentBooleanTrue() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "true")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertSame(FetchSourceContext.FETCH_SOURCE, ctx);
            assertTrue(ctx.fetchSource());
        }
    }

    public void testFromXContentBooleanFalse() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "false")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertSame(FetchSourceContext.DO_NOT_FETCH_SOURCE, ctx);
            assertFalse(ctx.fetchSource());
        }
    }

    // -------------------------------------------------------------------------
    // fromXContent – VALUE_STRING branch (new in this PR)
    // -------------------------------------------------------------------------

    public void testFromXContentSingleString() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "\"field1\"")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentSingleStringWithWildcard() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "\"obj.*\"")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("obj.*"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    // -------------------------------------------------------------------------
    // fromXContent – START_ARRAY branch (new in this PR)
    // -------------------------------------------------------------------------

    public void testFromXContentArrayMultipleIncludes() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "[\"field1\",\"field2\",\"field3\"]")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContainingInAnyOrder("field1", "field2", "field3"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentArraySingleElement() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "[\"only\"]")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("only"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentArrayEmpty() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "[]")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentArrayWithNonStringThrows() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "[\"field1\", 42]")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    public void testFromXContentArrayWithObjectThrows() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "[{\"a\":\"b\"}]")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    // -------------------------------------------------------------------------
    // fromXContent – START_OBJECT branch (delegates to parseSourceObject)
    // -------------------------------------------------------------------------

    public void testFromXContentObjectEmpty() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentObjectIncludesArray() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"includes\":[\"a\",\"b\"]}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContainingInAnyOrder("a", "b"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentObjectExcludesArray() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"excludes\":[\"x\",\"y\"]}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), arrayContainingInAnyOrder("x", "y"));
        }
    }

    public void testFromXContentObjectIncludesAndExcludesArrays() throws IOException {
        try (
            XContentParser parser = createParser(
                XContentType.JSON.xContent(),
                "{\"includes\":[\"field1\",\"field2\"],\"excludes\":[\"secret\"]}"
            )
        ) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContainingInAnyOrder("field1", "field2"));
            assertThat(ctx.excludes(), arrayContaining("secret"));
        }
    }

    public void testFromXContentObjectIncludesAsSingleString() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"includes\":\"singleField\"}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("singleField"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentObjectExcludesAsSingleString() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"excludes\":\"hiddenField\"}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), arrayContaining("hiddenField"));
        }
    }

    public void testFromXContentObjectDeprecatedIncludeField() throws IOException {
        // "include" is a deprecated alias for "includes"
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"include\":[\"dep1\",\"dep2\"]}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContainingInAnyOrder("dep1", "dep2"));
        }
    }

    public void testFromXContentObjectDeprecatedExcludeField() throws IOException {
        // "exclude" is a deprecated alias for "excludes"
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"exclude\":[\"dep3\"]}")) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.excludes(), arrayContaining("dep3"));
        }
    }

    public void testFromXContentObjectUnknownFieldInArrayThrows() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"unknownField\":[\"a\"]}")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    public void testFromXContentObjectUnknownFieldAsStringThrows() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"unknownField\":\"value\"}")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    public void testFromXContentObjectNonStringInIncludesArrayThrows() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"includes\":[\"ok\", 123]}")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    public void testFromXContentObjectIncludesWithNumericValueThrows() throws IOException {
        // The "includes" field value must be a string or array; a numeric value falls to the default branch
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"includes\":42}")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    // -------------------------------------------------------------------------
    // fromXContent – DEFAULT branch (unsupported token type)
    // -------------------------------------------------------------------------

    public void testFromXContentUnsupportedTokenThrows() throws IOException {
        // A numeric value is not one of the supported tokens
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "42")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    public void testFromXContentNullTokenThrows() throws IOException {
        // A JSON null value triggers VALUE_NULL token which falls to default
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "null")) {
            parser.nextToken();
            expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        }
    }

    // -------------------------------------------------------------------------
    // parseFromRestRequest – changed logic: `fetchSource == null || fetchSource`
    // -------------------------------------------------------------------------

    public void testParseFromRestRequestAllNull() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(new HashMap<>()).build();
        assertThat(FetchSourceContext.parseFromRestRequest(request), is(nullValue()));
    }

    public void testParseFromRestRequestFetchSourceTrue() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "true");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
    }

    public void testParseFromRestRequestFetchSourceFalse() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "false");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertFalse(ctx.fetchSource());
    }

    /**
     * When _source is not a boolean but a field pattern, it is treated as includes,
     * so fetchSource should default to true (null || fetchSource shortcut).
     */
    public void testParseFromRestRequestFetchSourceNullWithIncludes() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_includes", "field1,field2");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue("fetchSource should be true when only includes are specified", ctx.fetchSource());
        assertThat(ctx.includes(), arrayContainingInAnyOrder("field1", "field2"));
        assertThat(ctx.excludes(), emptyArray());
    }

    /**
     * When _source=false but includes are also provided, fetchSource remains false.
     * This tests the `fetchSource == null || fetchSource` expression: false || false = false.
     */
    public void testParseFromRestRequestFetchSourceFalseWithIncludes() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "false");
        params.put("_source_includes", "field1");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertFalse("fetchSource should be false when _source=false even with includes", ctx.fetchSource());
        assertThat(ctx.includes(), arrayContaining("field1"));
    }

    public void testParseFromRestRequestExcludesOnly() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_excludes", "secret,password");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.excludes(), arrayContainingInAnyOrder("secret", "password"));
        assertThat(ctx.includes(), emptyArray());
    }

    public void testParseFromRestRequestSourceAsFieldPattern() {
        // When _source is not a boolean it is treated as an includes pattern;
        // fetchSource stays null, sourceIncludes is populated.
        // The expression `fetchSource == null || fetchSource` short-circuits to true (null == null is true).
        Map<String, String> params = new HashMap<>();
        params.put("_source", "field1,field2");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), arrayContainingInAnyOrder("field1", "field2"));
    }

    public void testParseFromRestRequestIncludesAndExcludes() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_includes", "a,b");
        params.put("_source_excludes", "c");
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), arrayContainingInAnyOrder("a", "b"));
        assertThat(ctx.excludes(), arrayContaining("c"));
    }
}