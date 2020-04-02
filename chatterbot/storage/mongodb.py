import re
from chatterbot.storage import StorageAdapter
import datetime
class MongoDatabaseAdapter(StorageAdapter):
    """
    The MongoDatabaseAdapter is an interface that allows
    ChatterBot to store statements in a MongoDB database.

    :keyword database_uri: The URI of a remote instance of MongoDB.
                           This can be any valid
                           `MongoDB connection string <https://docs.mongodb.com/manual/reference/connection-string/>`_
    :type database_uri: str

    .. code-block:: python

       database_uri='mongodb://example.com:8100/'
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        from pymongo import MongoClient
        from pymongo.errors import OperationFailure

        self.database_uri = kwargs.get(
            'database_uri', 'mongodb://localhost:27017/chatterbot-database'
        )

        # Use the default host and port
        self.client = MongoClient(self.database_uri)

        # Increase the sort buffer to 42M if possible
        try:
            self.client.admin.command({'setParameter': 1, 'internalQueryExecMaxBlockingSortBytes': 44040192})
        except OperationFailure:
            pass

        # Specify the name of the database
        self.database = self.client.get_database()

        # The mongo collection of statement documents
        self.statements = self.database['statements']

        self.latestResponses = self.database['latestResponses']

    def get_statement_model(self):
        """
        Return the class for the statement model.
        """
        from chatterbot.conversation import Statement

        # Create a storage-aware statement
        statement = Statement
        statement.storage = self

        return statement

    def count(self):
        return self.statements.count()

    def mongo_to_object(self, statement_data):
        """
        Return Statement object when given data
        returned from Mongo DB.
        """
        Statement = self.get_model('statement')

        statement_data['id'] = statement_data['_id']

        return Statement(**statement_data)

    def filter(self, **kwargs):
        """
        Returns a list of statements in the database
        that match the parameters specified.
        """

        page_size = kwargs.pop('page_size', 1000)
        tags = kwargs.pop('tags', [])
        exclude_text = kwargs.pop('exclude_text', None)
        exclude_text_words = kwargs.pop('exclude_text_words', [])
        persona_not_startswith = kwargs.pop('persona_not_startswith', None)
        search_text_contains = kwargs.pop('search_text_contains', None)
        text_contains = kwargs.pop('text_contains', None)
        in_response_to_contains = kwargs.pop('in_response_to_contains', None)
        search_in_response_to_contains = kwargs.pop('search_in_response_to_contains', None)
        useStatementsCollection = kwargs.pop('statementsCollection', True)
        sortBy = kwargs.pop('sort', {})
        groupBy = kwargs.pop('group', "_id")

        collection = self.statements

        if not useStatementsCollection:
            collection = self.latestResponses

        if tags:
            kwargs['tags'] = {
                '$in': tags
            }

        if exclude_text:
            if 'text' not in kwargs:
                kwargs['text'] = {}
            elif 'text' in kwargs and isinstance(kwargs['text'], str):
                text = kwargs.pop('text')
                kwargs['text'] = {
                    '$eq': text
                }
            kwargs['text']['$nin'] = exclude_text

        if exclude_text_words:
            if 'text' not in kwargs:
                kwargs['text'] = {}
            elif 'text' in kwargs and isinstance(kwargs['text'], str):
                text = kwargs.pop('text')
                kwargs['text'] = {
                    '$eq': text
                }
            exclude_word_regex = '|'.join([
                '.*{}.*'.format(word) for word in exclude_text_words
            ])
            kwargs['text']['$not'] = re.compile(exclude_word_regex)

        if persona_not_startswith:
            if 'persona' not in kwargs:
                kwargs['persona'] = {}
            elif 'persona' in kwargs and isinstance(kwargs['persona'], str):
                persona = kwargs.pop('persona')
                kwargs['persona'] = {
                    '$eq': persona
                }
            kwargs['persona']['$not'] = re.compile('^bot:*')

        if search_text_contains:
            or_regex = '|'.join([
                '{}'.format(re.escape(word)) for word in search_text_contains.split(' ')
            ])
            # try matching whole words rather than part; for example 'hi' shouldn't match 'white'
            or_regex = '\\b' + or_regex + '\\b'
            kwargs['search_text'] = re.compile(or_regex, re.IGNORECASE)

        if text_contains:
            or_regex = '|'.join([
                '{}'.format(re.escape(word)) for word in text_contains.split(' ')
            ])
            # try matching whole words rather than part; for example 'hi' shouldn't match 'white'
            or_regex = '\\b' + or_regex + '\\b'
            kwargs['text'] = re.compile(or_regex, re.IGNORECASE)

        if in_response_to_contains:
            or_regex = '|'.join([
                '{}'.format(re.escape(word)) for word in in_response_to_contains.split(' ')
            ])
            # try matching whole words rather than part; for example 'hi' shouldn't match 'white'
            or_regex = '\\b' + or_regex + '\\b'
            kwargs['in_response_to'] = re.compile(or_regex, re.IGNORECASE)

        if search_in_response_to_contains:
            or_regex = '|'.join([
                '{}'.format(re.escape(word)) for word in search_in_response_to_contains.split(' ')
            ])
            # try matching whole words rather than part; for example 'hi' shouldn't match 'white'
            or_regex = '\\b' + or_regex + '\\b'
            kwargs['search_in_response_to'] = re.compile(or_regex, re.IGNORECASE)

        relatedStatements = []

        if sortBy:
            relatedStatements = collection.aggregate([{"$match": kwargs}, {'$sort': sortBy}, {"$group": {"_id": "$" + groupBy, "details": {"$first": '$$CURRENT'}}}, {'$limit': page_size}])
        else:
            relatedStatements = collection.aggregate([{"$match": kwargs}, {"$group": {"_id": "$" + groupBy, "details": {"$first": '$$CURRENT'}}}, {'$limit': page_size}])

        for match in relatedStatements:
            yield self.mongo_to_object(match['details'])

    def create(self, **kwargs):
        """
        Creates a new statement matching the keyword arguments specified.
        Returns the created statement.
        """
        Statement = self.get_model('statement')

        if 'tags' in kwargs:
            kwargs['tags'] = list(set(kwargs['tags']))

        if 'search_text' not in kwargs:
            kwargs['search_text'] = self.tagger.get_text_index_string(kwargs['text'])

        if 'search_in_response_to' not in kwargs:
            if kwargs.get('in_response_to'):
                kwargs['search_in_response_to'] = self.tagger.get_text_index_string(kwargs['in_response_to'])
        kwargs['count'] = 1
        # update instead of insert to prevent duplicates
        self.statements.update_one({"text": kwargs['text'], "in_response_to": kwargs['in_response_to']}, {"$setOnInsert": kwargs}, upsert=True)

        return Statement(**kwargs)

    def create_many(self, statements):
        """
        Creates multiple statement entries.
        """

        for statement in statements:
            statement_data = statement.serialize()
            tag_data = list(set(statement_data.pop('tags', [])))
            statement_data['tags'] = tag_data

            if not statement.search_text:
                statement_data['search_text'] = self.tagger.get_text_index_string(statement.text)

            if not statement.search_in_response_to and statement.in_response_to:
                statement_data['search_in_response_to'] = self.tagger.get_text_index_string(statement.in_response_to)
            statement_data['count'] = 1
            # update instead of insert to prevent duplicates
            self.statements.update_one({"text": statement_data['text'], "in_response_to": statement_data['in_response_to']}, {"$setOnInsert": statement_data}, upsert=True)

    def update(self, statement, useText=True, useStatementsCollection=True, setNewTags=False, useInResponseTo=False, useConversation=True):
        data = statement.serialize()
        data.pop('id', None)
        data.pop('tags', None)
        data.pop('count', None)
        data.pop('conversation', None)

        data['last_updated_at'] = datetime.datetime.now()
        if statement.search_text:
            data['search_text'] = statement.search_text
        else:
            data['search_text'] = self.tagger.get_text_index_string(data['text'])

        collection = self.statements

        if not useStatementsCollection:
            collection = self.latestResponses

        if statement.search_in_response_to:
            data['search_in_response_to'] = statement.search_in_response_to
        elif data.get('in_response_to'):
            data['search_in_response_to'] = self.tagger.get_text_index_string(data['in_response_to'])

        update_data = {'$setOnInsert': {"id": None, "conversation": statement.conversation}, "$inc": {"count": 1}}

        ## don't update: conversation, vectors, vector_norm, created_at
        if useStatementsCollection:
            data.pop('created_at')
            update_data['$setOnInsert']['created_at'] = statement.created_at
            data.pop('text')
            update_data['$setOnInsert']['text'] = statement.text
            data.pop('search_in_response_to')
            update_data['$setOnInsert']['search_in_response_to'] = statement.search_in_response_to
            data.pop('vector')
            update_data['$setOnInsert']['vector'] = statement.vector
            data.pop('vector_norm')
            update_data['$setOnInsert']['vector_norm'] = statement.vector_norm

        if setNewTags:
            data['tags'] = statement.tags
        elif statement.tags:
            update_data['$addToSet'] = {
                'tags': {
                    '$each': statement.tags
                }
            }
        else:
            update_data['$setOnInsert']['tags'] = []
        update_data['$set'] = data

        search_parameters = {}
        if statement.id is not None:
            search_parameters['_id'] = statement.id
        else:
            if useText:
                search_parameters['text'] = statement.text
            if useInResponseTo:
                search_parameters['in_response_to'] = statement.in_response_to
            if useConversation:
                search_parameters['conversation'] = statement.conversation

        update_operation = collection.update_one(
            search_parameters,
            update_data,
            upsert=True
        )

        if update_operation.acknowledged:
            statement.id = update_operation.upserted_id

        return statement

    def get_random(self):
        """
        Returns a random statement from the database
        """
        from random import randint

        count = self.count()

        if count < 1:
            raise self.EmptyDatabaseException()

        random_integer = randint(0, count - 1)

        statements = self.statements.find().limit(1).skip(random_integer)

        return self.mongo_to_object(list(statements)[0])

    def remove(self, statement_text):
        """
        Removes the statement that matches the input text.
        """
        self.statements.delete_one({'text': statement_text})

    def drop(self):
        """
        Remove the database.
        """
        self.client.drop_database(self.database.name)
