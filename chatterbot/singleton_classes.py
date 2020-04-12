from chatterbot import languages
import spacy
from spacy_hunspell import spaCyHunSpell
from spellchecker import SpellChecker
# loading spacy more than once slows down everything and makes it consume a lot of extra memory
# so having a single instance will save memory
class singleSpacy:
    _instance = None
    language = None
    spell = None
    @staticmethod
    def getInstance(language=None):
        if singleSpacy._instance is None:
            singleSpacy(language)
        return singleSpacy._instance

    def __init__(self, language=None):
        self.language = language or languages.ENG
        singleSpacy._instance = spacy.load(self.language.ISO_639_1.lower())
        hunspell = spaCyHunSpell(singleSpacy._instance, 'linux')
        singleSpacy._instance.add_pipe(hunspell)
        singleSpacy.spell = SpellChecker(distance=1)
        singleSpacy.spell.word_frequency.load_words(
            ["what's", "whitelist", "concat", "unsubscribe", "unsend", "faq", "url", "keepa"]
        )

    def checkSpell(doc):
        replaceWords = {}
        for token in doc:
            if not token._.hunspell_spell and "'" not in token.text:
                spellCorrection = singleSpacy.spell.correction(token.text).casefold()
                spellCandidates = singleSpacy.spell.candidates(token.text)
                hunSuggestions = [x.casefold() for x in token._.hunspell_suggest]
                if hunSuggestions and spellCorrection:
                    if spellCorrection == hunSuggestions[0]:
                        replaceWords[token.text] = spellCorrection
                    elif spellCorrection in set(hunSuggestions):
                        replaceWords[token.text] = spellCorrection
                    elif token.is_oov or hunSuggestions[0] in spellCandidates:
                        replaceWords[token.text] = hunSuggestions[0]
        newText = doc.text
        for word in replaceWords:
            newText = newText.replace(word, replaceWords[word], 1)
        return newText
