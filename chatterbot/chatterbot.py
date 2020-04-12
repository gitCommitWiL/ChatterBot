import logging
from chatterbot.storage import StorageAdapter
from chatterbot.logic import LogicAdapter
from chatterbot.search import TextSearch, IndexedTextSearch
from chatterbot import utils
from chatterbot import singleton_classes
import datetime
import copy

class ChatBot(object):
    """
    A conversational dialog chat bot.
    """

    def __init__(self, name, **kwargs):
        self.name = name

        storage_adapter = kwargs.get('storage_adapter', 'chatterbot.storage.SQLStorageAdapter')

        logic_adapters = kwargs.get('logic_adapters', [
            'chatterbot.logic.BestMatch'
        ])

        # Check that each adapter is a valid subclass of it's respective parent
        utils.validate_adapter_class(storage_adapter, StorageAdapter)

        # Logic adapters used by the chat bot
        self.logic_adapters = []

        self.storage = utils.initialize_class(storage_adapter, **kwargs)

        primary_search_algorithm = IndexedTextSearch(self, **kwargs)
        text_search_algorithm = TextSearch(self, **kwargs)

        self.search_algorithms = {
            primary_search_algorithm.name: primary_search_algorithm,
            text_search_algorithm.name: text_search_algorithm
        }

        for adapter in logic_adapters:
            utils.validate_adapter_class(adapter, LogicAdapter)
            logic_adapter = utils.initialize_class(adapter, self, **kwargs)
            self.logic_adapters.append(logic_adapter)

        preprocessors = kwargs.get(
            'preprocessors', [
                'chatterbot.preprocessors.clean_whitespace'
            ]
        )

        self.preprocessors = []

        for preprocessor in preprocessors:
            self.preprocessors.append(utils.import_module(preprocessor))


        self.logger = kwargs.get('logger', logging.getLogger(__name__))

        # Allow the bot to save input it receives so that it can learn
        self.read_only = kwargs.get('read_only', False)

    def get_response(self, statement=None, **kwargs):
        """
        Return the bot's response based on the input.

        :param statement: An statement object or string.
        :returns: A response to the input.
        :rtype: Statement

        :param additional_response_selection_parameters: Parameters to pass to the
            chat bot's logic adapters to control response selection.
        :type additional_response_selection_parameters: dict

        :param persist_values_to_response: Values that should be saved to the response
            that the chat bot generates.
        :type persist_values_to_response: dict
        """
        Statement = self.storage.get_object('statement')

        additional_response_selection_parameters = kwargs.pop('additional_response_selection_parameters', {})

        persist_values_to_response = kwargs.pop('persist_values_to_response', {})

        checkSpelling = kwargs.pop('checkSpelling', True)

        if isinstance(statement, str):
            kwargs['text'] = statement

        if isinstance(statement, dict):
            kwargs.update(statement)

        if statement is None and 'text' not in kwargs:
            raise self.ChatBotException(
                'Either a statement object or a "text" keyword '
                'argument is required. Neither was provided.'
            )

        if hasattr(statement, 'serialize'):
            kwargs.update(**statement.serialize())

        tags = kwargs.pop('tags', [])

        bannedFromLearning = kwargs.pop('bannedFromLearning', False)

        text = kwargs.pop('text')

        # use spaCy early
        nlpText = self.storage.tagger.nlp(text)
        # do a quick spell check
        if checkSpelling:
            oldText = nlpText.text
            nlpText = self.storage.tagger.nlp(singleton_classes.singleSpacy.checkSpell(nlpText))
            if nlpText.text.casefold() != oldText.casefold():
                tags.append("skipLearning")

        ## store these 2 values
        kwargs['vector'] = nlpText.vector.tolist()
        kwargs['vector_norm'] = nlpText.vector_norm

        input_statement = Statement(text=nlpText.text, **kwargs)

        input_statement.add_tags(*tags)

        # Preprocess the input statement
        for preprocessor in self.preprocessors:
            input_statement = preprocessor(input_statement)

        # Make sure the input statement has its search text saved

        if not input_statement.search_text:
            input_statement.search_text = self.storage.tagger.get_text_index_string(input_statement.text)

        if not input_statement.search_in_response_to and input_statement.in_response_to:
            input_statement.search_in_response_to = self.storage.tagger.get_text_index_string(input_statement.in_response_to)

        response = self.generate_response(input_statement, additional_response_selection_parameters)

        # Update any response data that needs to be changed
        if persist_values_to_response:
            for response_key in persist_values_to_response:
                response_value = persist_values_to_response[response_key]
                if response_key == 'tags':
                    input_statement.add_tags(*response_value)
                    response.add_tags(*response_value)
                else:
                    setattr(input_statement, response_key, response_value)
                    setattr(response, response_key, response_value)

        # reset these values for learning
        input_statement.vector = []
        input_statement.vector_norm = 0

        # just in case need to prevent specific trolls
        if not bannedFromLearning and not self.read_only and "skipLearning" not in input_statement.tags:

            if "learnResponseOnly" not in input_statement.tags and "learnResponseOnly" not in response.tags:
                # learn that user's input is a valid response to bot's last response
                self.learn_response(input_statement)
            else:
                # remove the learnResponseOnly tag from input
                input_statement.tags.remove("learnResponseOnly")
            if "newResponse" not in input_statement.tags:
                # want to also learn that bot response is valid for user's input statement
                self.learn_response(response, input_statement)
                # empty tags for latestResponse collection
                response.tags = []
            else:
                ## don't learn input for new thing
                response.tags = ["newResponse"]
                
            # record/ update the latest reponse by bot in the conversation
            self.storage.update(response, useText=False, useStatementsCollection=False, setNewTags=True, useInResponseTo=False)
        return response

    def generate_response(self, input_statement, additional_response_selection_parameters=None):
        """
        Return a response based on a given input statement.

        :param input_statement: The input statement to be processed.
        """
        Statement = self.storage.get_object('statement')

        results = []
        result = None
        max_confidence = -1

        for adapter in self.logic_adapters:
            if adapter.can_process(input_statement):

                output = adapter.process(input_statement, additional_response_selection_parameters)
                results.append(output)

                self.logger.info(
                    '{} selected "{}" as a response with a confidence of {}'.format(
                        adapter.class_name, output.text, output.confidence
                    )
                )

                if output.confidence > max_confidence:
                    result = output
                    max_confidence = output.confidence
            else:
                self.logger.info(
                    'Not processing the statement using {}'.format(adapter.class_name)
                )

        class ResultOption:
            def __init__(self, statement, count=1):
                self.statement = statement
                self.count = count

        # If multiple adapters agree on the same statement,
        # then that statement is more likely to be the correct response
        if len(results) >= 3:
            result_options = {}
            for result_option in results:
                result_string = result_option.text + ':' + (result_option.in_response_to or '')

                if result_string in result_options:
                    result_options[result_string].count += 1
                    if result_options[result_string].statement.confidence < result_option.confidence:
                        result_options[result_string].statement = result_option
                else:
                    result_options[result_string] = ResultOption(
                        result_option
                    )

            most_common = list(result_options.values())[0]

            for result_option in result_options.values():
                if result_option.count > most_common.count:
                    most_common = result_option

            if most_common.count > 1:
                result = most_common.statement

        response = Statement(
            text=result.text,
            in_response_to=input_statement.text,
            conversation=input_statement.conversation,
            persona='bot:' + self.name
        )

        response.confidence = result.confidence

        return response

    def learn_response(self, statement, previous_statement=None, applyPreprocessors=True):
        """
        Learn that the statement provided is a valid response.
        """

        if not statement.search_text:
            statement.search_text = self.storage.tagger.get_text_index_string(statement.text)

        if not previous_statement:
            previous_statement = statement.in_response_to

        if not previous_statement:
            previous_statement = self.get_latest_response(statement.conversation, fromBot=True, useStatementsCollection=False)

        # if still nothing, then return; or if newResponse in tags skip (bot responded with default to this input)
        if not previous_statement or (previous_statement.tags and 'newResponse' in previous_statement.tags):
            return
        previous_statement_text = previous_statement.text

        if not isinstance(previous_statement, (str, type(None), )):
            statement.in_response_to = previous_statement.text
            if not statement.search_in_response_to:
                statement.search_in_response_to = previous_statement.search_text

        elif isinstance(previous_statement, str):
            statement.in_response_to = previous_statement
            if not statement.search_in_response_to:
                statement.search_in_response_to = self.storage.tagger.get_text_index_string(previous_statement)

        ## fill out the vector response information
        if not statement.vector:
            ## use spaCy early
            nlpText = self.storage.tagger.nlp(previous_statement.text)

            ## store these 2 values
            statement.vector = nlpText.vector.tolist()
            statement.vector_norm = nlpText.vector_norm

        self.logger.info('Adding "{}" as a response to "{}"'.format(
            statement.text,
            previous_statement_text
        ))
        if applyPreprocessors:
            for preprocessor in self.preprocessors:
                statement = preprocessor(statement)
        statementCopy = copy.copy(statement)
        # will upsert if doesn't exist, otherwise just update existing (tags and stuff); don't create duplicate
        return self.storage.update(statementCopy, useInResponseTo=True, useConversation=False)

    def get_latest_response(self, conversation, fromBot=True, recentMinutes=1, useStatementsCollection=True):
        """
        Returns the latest response in a conversation if it exists.
        Returns None if a matching conversation cannot be found.

        fromBot: True if want the latest response specifically from chatbot
        recentMinutes: make sure response retrieved is within the past recentMinutes minutes
        """

        arguments = {
            "conversation": conversation,
            "sort": {'created_at': -1},
            'statementsCollection': useStatementsCollection,
        }

        # find latest statement from bot
        if fromBot:
            arguments["persona"] = 'bot:' + self.name

        conversation_statements = list(self.storage.filter(**arguments))

        # Get the most recent statement in the conversation if one exists
        latest_statement = conversation_statements[0] if conversation_statements else None

        # if 0 or less, ignore minutes
        if recentMinutes <= 0:
            return latest_statement
        # get a recent response; otherwise reponse might not be related
        if latest_statement and latest_statement.created_at.replace(tzinfo=None) >= datetime.datetime.now() - datetime.timedelta(minutes=recentMinutes):
            return latest_statement
        return None

    class ChatBotException(Exception):
        pass
