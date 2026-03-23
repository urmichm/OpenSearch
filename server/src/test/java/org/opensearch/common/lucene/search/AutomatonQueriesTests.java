package org.opensearch.common.lucene.search;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.opensearch.test.OpenSearchTestCase;

public class AutomatonQueriesTests extends OpenSearchTestCase {

    public void testToCaseInsensitiveUkrainianChar() {
        {
            // Capital   1030   І   \u0406  https://everythingfonts.com/unicode/0x0406
            // Lower     1110   і   \u0456  https://everythingfonts.com/unicode/0x0456
            Automaton character = AutomatonQueries.toCaseInsensitiveChar(0x0406);
            CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(character);
            assertTrue("І і pair", runAutomaton.run("\u0456"));
        }
        {
            // Capital   1031   Ї   \u0407  https://everythingfonts.com/unicode/0x0407
            // Lower     1111   ї   \u0457  https://everythingfonts.com/unicode/0x0457
            Automaton character = AutomatonQueries.toCaseInsensitiveChar('\u0457');
            CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(character);
            assertTrue("Ї ї pair", runAutomaton.run("\u0407"));
        }
    }

    public void testToCaseInsensitiveStringMixed() {
        // Test a string with mixed ASCII and non-ASCII
        Automaton automaton = AutomatonQueries.toCaseInsensitiveString("İstanbul");
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);

        assertTrue("Should match original 'İstanbul'", runAutomaton.run("İstanbul"));
        assertTrue("Should match lowercase 'istanbul'", runAutomaton.run("istanbul"));
        assertTrue("Should match uppercase 'İSTANBUL'", runAutomaton.run("İSTANBUL"));
        assertTrue("Should match mixed 'İsTanBUl'", runAutomaton.run("İsTanBUl"));
        assertTrue("Should match mixed 'iStANbuL'", runAutomaton.run("iStANbuL"));

        assertFalse("Must not match english 'Istanbul'", runAutomaton.run("Istanbul"));
    }

    public void testToCaseInsensitiveEnglishChar() {
        Automaton capitalI = AutomatonQueries.toCaseInsensitiveChar('R');
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(capitalI);
        assertTrue("R r pair", runAutomaton.run("r"));
    }
}
