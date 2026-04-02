/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;

public class FetchSourceContextTests extends OpenSearchTestCase {

    // ----------------------------------------------------------------
    // fromXContent – VALUE_BOOLEAN
    // ----------------------------------------------------------------

    public void testFromXContentBooleanTrue() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value(true);
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertSame(FetchSourceContext.FETCH_SOURCE, ctx);
            assertTrue(ctx.fetchSource());
        }
    }

    public void testFromXContentBooleanFalse() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value(false);
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertSame(FetchSourceContext.DO_NOT_FETCH_SOURCE, ctx);
            assertFalse(ctx.fetchSource());
        }
    }

    // ----------------------------------------------------------------
    // fromXContent – VALUE_STRING (single include shorthand)
    // ----------------------------------------------------------------

    public void testFromXContentString() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value("field1");
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    // ----------------------------------------------------------------
    // fromXContent – START_ARRAY (array of includes)
    // ----------------------------------------------------------------

    public void testFromXContentArray() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startArray().value("field1").value("field2").endArray();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1", "field2"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentEmptyArray() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startArray().endArray();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentArrayWithNonStringElement() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startArray().value("field1").value(42).endArray();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            ParsingException e = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertThat(e.getMessage(), containsString("Unknown key for a"));
        }
    }

    // ----------------------------------------------------------------
    // fromXContent – START_OBJECT (delegates to parseSourceObject)
    // ----------------------------------------------------------------

    public void testFromXContentObjectWithIncludesArray() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .array("includes", "field1", "field2")
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1", "field2"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentObjectWithExcludesArray() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .array("excludes", "field3", "field4")
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), arrayContaining("field3", "field4"));
        }
    }

    public void testFromXContentObjectWithBothIncludesAndExcludes() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .array("includes", "field1", "field2")
            .array("excludes", "field3")
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1", "field2"));
            assertThat(ctx.excludes(), arrayContaining("field3"));
        }
    }

    public void testFromXContentObjectWithIncludesSingularAlias() throws IOException {
        // "include" is a deprecated alias for "includes"
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .array("include", "field1")
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1"));
        }
    }

    public void testFromXContentObjectWithExcludesSingularAlias() throws IOException {
        // "exclude" is a deprecated alias for "excludes"
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .array("exclude", "field3")
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.excludes(), arrayContaining("field3"));
        }
    }

    public void testFromXContentObjectWithIncludesAsString() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .field("includes", "field1")
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), arrayContaining("field1"));
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentObjectWithExcludesAsString() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .field("excludes", "field3")
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.excludes(), arrayContaining("field3"));
        }
    }

    public void testFromXContentEmptyObject() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            FetchSourceContext ctx = FetchSourceContext.fromXContent(parser);
            assertTrue(ctx.fetchSource());
            assertThat(ctx.includes(), emptyArray());
            assertThat(ctx.excludes(), emptyArray());
        }
    }

    public void testFromXContentObjectWithUnknownKey() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .field("unknown_field", "value")
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            ParsingException e = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertThat(e.getMessage(), containsString("Unknown key for a"));
            assertThat(e.getMessage(), containsString("unknown_field"));
        }
    }

    public void testFromXContentObjectWithUnknownArrayKey() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .array("unknown_array", "val1")
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            ParsingException e = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertThat(e.getMessage(), containsString("Unknown key for a"));
        }
    }

    public void testFromXContentObjectIncludesArrayWithNonStringElement() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("includes")
            .value("field1")
            .value(123)
            .endArray()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            ParsingException e = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertThat(e.getMessage(), containsString("Unknown key for a"));
        }
    }

    public void testFromXContentObjectExcludesArrayWithNonStringElement() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("excludes")
            .value(true)
            .endArray()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            ParsingException e = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertThat(e.getMessage(), containsString("Unknown key for a"));
        }
    }

    public void testFromXContentObjectWithInvalidTokenInsideObject() throws IOException {
        // A nested object inside the source object is not a valid value token
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("includes")
            .field("nested", "val")
            .endObject()
            .endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            ParsingException e = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertThat(e.getMessage(), containsString("Unknown key for a"));
        }
    }

    // ----------------------------------------------------------------
    // fromXContent – default / invalid token
    // ----------------------------------------------------------------

    public void testFromXContentInvalidTokenThrows() throws IOException {
        // VALUE_NUMBER is not a valid top-level token for fromXContent
        XContentBuilder builder = XContentFactory.jsonBuilder().value(42);
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            ParsingException e = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertThat(e.getMessage(), containsString("Expected one of"));
            assertThat(e.getMessage(), containsString("VALUE_BOOLEAN"));
            assertThat(e.getMessage(), containsString("VALUE_STRING"));
            assertThat(e.getMessage(), containsString("START_ARRAY"));
            assertThat(e.getMessage(), containsString("START_OBJECT"));
        }
    }

    // ----------------------------------------------------------------
    // parseFromRestRequest
    // ----------------------------------------------------------------

    public void testParseFromRestRequestNoParams() {
        RestRequest request = new FakeRestRequest(new HashMap<>());
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNull(ctx);
    }

    public void testParseFromRestRequestSourceTrue() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "true");
        RestRequest request = new FakeRestRequest(params);
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), emptyArray());
        assertThat(ctx.excludes(), emptyArray());
    }

    public void testParseFromRestRequestSourceFalse() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "false");
        RestRequest request = new FakeRestRequest(params);
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertFalse(ctx.fetchSource());
    }

    public void testParseFromRestRequestSourceAsIncludePattern() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "field1,field2");
        RestRequest request = new FakeRestRequest(params);
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        // When _source is not a boolean it is treated as include patterns; fetchSource should be true
        assertTrue(ctx.fetchSource());
        assertEquals(2, ctx.includes().length);
    }

    public void testParseFromRestRequestSourceIncludesOnly() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_includes", "field1,field2");
        RestRequest request = new FakeRestRequest(params);
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        // fetchSource should be true when only includes are set (null || null -> true via null==null)
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), arrayContaining("field1", "field2"));
        assertThat(ctx.excludes(), emptyArray());
    }

    public void testParseFromRestRequestSourceExcludesOnly() {
        Map<String, String> params = new HashMap<>();
        params.put("_source_excludes", "field3");
        RestRequest request = new FakeRestRequest(params);
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        // fetchSource null → fetchSource == null || fetchSource == null (true)
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), emptyArray());
        assertThat(ctx.excludes(), arrayContaining("field3"));
    }

    public void testParseFromRestRequestSourceFalseWithIncludes() {
        // When _source=false and _source_includes are set, fetchSource should be false
        // because: fetchSource(=false) == null is false, so result = false || false = false
        Map<String, String> params = new HashMap<>();
        params.put("_source", "false");
        params.put("_source_includes", "field1");
        RestRequest request = new FakeRestRequest(params);
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertFalse(ctx.fetchSource());
        assertThat(ctx.includes(), arrayContaining("field1"));
    }

    public void testParseFromRestRequestSourceTrueWithExcludes() {
        Map<String, String> params = new HashMap<>();
        params.put("_source", "true");
        params.put("_source_excludes", "field3,field4");
        RestRequest request = new FakeRestRequest(params);
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.excludes(), arrayContaining("field3", "field4"));
    }

    public void testParseFromRestRequestSourceNullWithIncludesAndExcludes() {
        // Both _source_includes and _source_excludes set, no explicit _source param
        // fetchSource == null || fetchSource should evaluate to true (null == null is true)
        Map<String, String> params = new HashMap<>();
        params.put("_source_includes", "field1");
        params.put("_source_excludes", "field2");
        RestRequest request = new FakeRestRequest(params);
        FetchSourceContext ctx = FetchSourceContext.parseFromRestRequest(request);
        assertNotNull(ctx);
        assertTrue(ctx.fetchSource());
        assertThat(ctx.includes(), arrayContaining("field1"));
        assertThat(ctx.excludes(), arrayContaining("field2"));
    }
}