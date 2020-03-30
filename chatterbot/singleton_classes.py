from chatterbot import languages
import spacy

# loading spacy more than once slows down everything and makes it consume a lot of extra memory
# so having a single instance will save memory
class singleSpacy:
    _instance = None
    language = None
    @staticmethod
    def getInstance(language=None):
        if singleSpacy._instance is None:
            singleSpacy(language)
        return singleSpacy._instance

    def __init__(self, language=None):
        self.language = language or languages.ENG
        singleSpacy._instance = spacy.load(self.language.ISO_639_1.lower())
