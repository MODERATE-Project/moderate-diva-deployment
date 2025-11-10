from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.relationship import Relationship

import re
import json
from genson import SchemaBuilder
from requests import get, post
import time


class SchemaValidator(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        # Customize as needed.
        version = '2.0.0-M2'
        description = """Schema Validator"""
        tags = ["schema", "validator", "links"]

        # IMPORTANT check that all dependencies are listed here.
        dependencies = ["genson==1.3.0","requests==2.32.3"]
        
    VALIDATOR_ID = PropertyDescriptor(
        name="Validator ID",
        description="ID of the Validator processor",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    
    KAFKA_URI = PropertyDescriptor(
        name="Kafka URI",
        description="Address and port of the machine where kafka is running",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        default_value="http://machine_url:8081",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    
    KAFKA_TOPIC = PropertyDescriptor(
        name="Kafka_topic",
        description="Name of the kafka topic",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    KAFKA_SCHEMA_IDS = PropertyDescriptor(
        name="Kafka schema ids",
        description="Comma separated list of kafka's schema ids",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    
    MINIMUM_THRESHOLD = PropertyDescriptor(
        name="Minimum Threshold",
        description="Minimum number of times a schema has to be seen before being marked as 'valid'",
        required=False,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="10",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    STRICT_CHECK = PropertyDescriptor(
        name="Strict Check",
        description="Check only the schemas from Kafka without accepting new ones ",
        required=False,
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value="False",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    MESSAGES_HISTORY = PropertyDescriptor(
        name="Messages History",
        description="Maximum number of messages accepted before cleaning the history of invalid messages",
        required=False,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="10000",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # IMPORTANT 'descriptors' should contain all property descriptors.
    descriptors = [VALIDATOR_ID, KAFKA_URI,KAFKA_TOPIC,KAFKA_SCHEMA_IDS, MINIMUM_THRESHOLD, STRICT_CHECK, MESSAGES_HISTORY]

    VALID_REL = Relationship(name="valid", description="Valid Flowfiles")
    INVALID_REL = Relationship(name="invalid", description="Invalid Flowfiles")
    
    relationships = [VALID_REL, INVALID_REL]


    def __init__(self, **kwargs):
        """ Use 'pass' even if official docs says otherwise.
        Maybe it's a bug.        
        """
        pass

    def onScheduled(self, context):
        """ Put here all the 'preloading' to improve processor performance.
        """
        validator_id = context.getProperty(self.VALIDATOR_ID).getValue()
        uri = context.getProperty(self.KAFKA_URI).getValue()
        kafka_topic = context.getProperty(self.KAFKA_TOPIC).getValue()
        schema_ids = context.getProperty(self.KAFKA_SCHEMA_IDS).getValue()
        min_thresh = context.getProperty(self.MINIMUM_THRESHOLD).getValue()
        strict_check = context.getProperty(self.STRICT_CHECK).getValue()
        max_messages = context.getProperty(self.MESSAGES_HISTORY).getValue()
        self.checker = Validator(validator_id, uri, kafka_topic, schema_ids, min_thresh, strict_check, max_messages)

    def getPropertyDescriptors(self):
        """ Do not change.
        """
        return self.descriptors
    
    def getRelationships(self):
        """ Do not change.
        """
        return self.relationships


    def transform(self, context, flowfile):
        """ Write here all of the processor logic.
        """
        try:
            input = json.loads(flowfile.getContentsAsBytes().decode('utf-8'))
            isValid = self.checker.validate(input['metricValue']) # ignore all the metadata added by the NiFi custom components
            output = {
                "validatorID": self.validatorID,
                "sample": input,
                "validations": [
                    {
                        "type": "schema",
                        "feature": input['dataItemID'],
                        "checks": [
                            isValid
                        ],
                        "result": isValid,
                        "description": ""
                    }

                ],
                "ts": time.time_ns()
            }
            if isValid is True:
                return FlowFileTransformResult(relationship = "valid", contents=json.dumps(output)) 
            else:
                return FlowFileTransformResult(relationship = "invalid", contents=json.dumps(output))    
        except Exception as e:
            self.logger.error(e)
            return FlowFileTransformResult(relationship = "failure")
    
class Validator:
    def __init__(self, validator_id: str, uri: str, topic_name: str, schema_ids: str = None, min_thresh: str = None, strick: str = None, max_messages: str = None) -> None:

        self.validator_id = validator_id
        self.url = uri
        
        # if min_thresh is not set, set the default value
        self.min = int(min_thresh) if min_thresh is not None and min_thresh.isdecimal() else 10

        # if max_messages is not set, set the default value
        self.max_messages = int(max_messages) if max_messages is not None and max_messages.isdecimal() else 10000

        # by default strict is set to False
        self.strict = True if strick is not None and strick == "True" else False

        # retrieve schemas from kafka schema registry
        self.ground_truth = {}
        if schema_ids is not None:
            ids = [id.strip() for id in schema_ids.split(',')]
            # schemas from kafka are valid by default
            self.ground_truth = dict.fromkeys([self.stringify(schema) for schema in self.api_calls(method='get-schema', ids=ids)], self.min)

        # progressive name for the new selected schemas
        self.schema_name = topic_name + '_auto_'
        self.identifier = self.get_updated_info()

        # keep track of the number of message read
        self.n_messages = 0

    def validate(self, input: str) -> bool:

        self.n_messages += 1

        # made it possible to be stored in a dict
        schema = self.to_schema(input)
        ss = self.stringify(schema)

        if ss in self.ground_truth and self.ground_truth[ss] >= self.min:
            # schema already seen a sufficient numbers of times!
            return True
        elif not self.strict:
            # if new schema can be found, increment the number of times it has appeared
            self.ground_truth[ss] = self.ground_truth.get(ss,0) +1 
            if self.ground_truth[ss] >= self.min:
                # push the schema to the kafka schema registry
                self.api_calls(method='post', schema=schema)
                self.identifier += 1

        if self.n_messages >= self.max_messages:
            # worst case scenario, every invalid message is different 
            self.cleanup()

        return False
        
    def stringify(self, input) -> str:
        return str(hash(json.dumps(input).replace(' ', '')))
    
    def to_schema(self, input):
        # create the schema
        builder = SchemaBuilder()
        builder.add_object(input)
        return builder.to_json()
    
    def api_calls(self, method: str, ids: list = None, schema: str = None) -> list[str]:

        # do not touch unless you clearly know what you are doing
        headers = {
        "Accept": "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"
        }

        if method == 'get-schema':
            query = self.url + '/schemas/ids/' if ids[0].isdecimal() else self.url + '/subjects/'
            terminator = '' if ids[0].isdecimal() else '/versions/1'
            # list of schemas, automatically rules out errors
            if not (res := [response['schema'] for id in ids if 'schema' in (response := get(query + id + terminator, headers=headers).json())]):
                # if the list is empty: explode 
                raise ValueError('No kafka schemas found with the given ids')
            else:
                return res
            
        if method == 'post':
            # schema to be added to the kafka registry
            payload = {
                "schema": schema,
                "schemaType": "JSON"
            }

            res = post(self.url + '/subjects/' + self.schema_name + str(self.identifier) + '/versions', headers=headers, data=json.dumps(payload))
            # in case of failure: explode
            if res.status_code != 200:
                raise ValueError(res.json())
        
        if method == 'get-subjects':
            res = get(self.url + '/subjects')
            if res.status_code != 200:
                raise ValueError(res.json())
            else:
                return res.json()

    def get_updated_info(self) -> int:
        subjects = self.api_calls(method='get-subjects')
        pattern = re.compile(self.schema_name + r"(\d+)$")
        max_n = 0
        for s in subjects:
            match = pattern.search(s)
            if match:
                found_schema = self.api_calls('get-schema', ids=[s])
                self.ground_truth.update({self.stringify(sc): self.min for sc in found_schema})
                max_n = nmb if (nmb := int(match.group(1))) > max_n else max_n
        return max_n + 1

    def cleanup(self) -> None:
        # in place deletion to avoid copying the entire dict
        for k in list(self.ground_truth):
            if self.ground_truth[k] <= 2:
                del self.ground_truth[k]