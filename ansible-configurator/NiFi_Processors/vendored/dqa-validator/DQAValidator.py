# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.relationship import Relationship
import json
import re
import jmespath
import yaml
import time
import functools
import traceback
from typing import Dict, List, Union

class DQAValidator(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        # Customize as needed.
        version = '2.0.0-M4'
        description = """DQA Validator"""
        tags = ["dqa", "validator", "links"]

        # IMPORTANT check that all dependencies are listed here.
        dependencies = ["jsonschema==4.22.0", "jmespath==1.0.1", "pyyaml==6.0.1"]
        
    VALIDATOR_ID = PropertyDescriptor(
        name="Validator ID",
        description="ID of the validator component",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    VALIDATION_RULES = PropertyDescriptor(
        name="Validation Rules",
        description="YAML formatted validation rules",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], # TODO in the future check how to use custom validator to support validating YAML
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # IMPORTANT 'descriptors' should contain all property descriptors.
    descriptors = [VALIDATOR_ID, VALIDATION_RULES]
    
    
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
        # Store descriptors so we can evaluate expressions against the FlowFile later.
        self.validator_id_prop = context.getProperty(self.VALIDATOR_ID)
        self.rules_prop = context.getProperty(self.VALIDATION_RULES)

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
            # Evaluate properties with FlowFile attributes so dynamic rules (e.g. dqa.rules) are resolved.
            validatorID = self.validator_id_prop.evaluateAttributeExpressions(flowfile).getValue()
            rules_text = self.rules_prop.evaluateAttributeExpressions(flowfile).getValue()
            validator = StandardValidator(validatorID, rules_text)

            input_data = json.loads(flowfile.getContentsAsBytes().decode('utf-8'))
            validationRes = validator.validate(input_data)
            output = json.dumps(validationRes)
            all_valid = all(v['result'] for v in validationRes['validations'])
            if all_valid:
                return FlowFileTransformResult(relationship="valid", contents=output)
            else:
                return FlowFileTransformResult(relationship = "invalid", contents=output) 
        except Exception as e:
            # Log stringified error to avoid py4j trying to serialize the Python exception object.
            self.logger.error(str(e))
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship = "failure")



class StandardValidator:
    """
    A class for validating data samples against a set of predefined rules.

    Parameters
    ----------
    validatorID : str
        The ID of the validator
    config : str
        The path to the YAML configuration file containing the validation rules.

    Attributes
    ----------
    config : dict
        The loaded YAML configuration dictionary.

    Methods
    -------
    validate(sample: Dict) -> Dict
        Validates a data sample against the configured rules and returns the
        validation results.
    check_domain(sample: Dict, feature_path: str, specs: Dict) -> Dict
        Checks if the value(s) of a feature are within a specified domain.
    check_strlen(sample: Dict, feature_path: str, specs: Dict) -> Dict
        Checks if the string length of a feature's value(s) meets a specified
        condition.
    check_datatype(sample: Dict, feature_path: str, specs: Dict) -> Dict
        Checks if the data type of a feature's value(s) matches a specified
        type.
    check_categorical(sample: Dict, feature_path: str, specs: Dict) -> Dict
        Checks if a feature's value(s) are within a list of allowed values.
    exists(sample: Dict, feature_path: str, specs: Dict) -> Dict
        Checks if a feature exists or is missing in the data sample.
    check_regex(sample: Dict, feature_path: str, specs: Dict) -> Dict
        Checks if a feature's value(s) match a specified regular expression.
    """

    def __init__(self, validatorID: str, config: str, from_string=True):
        
        self.validatorID = validatorID

        if from_string:
            self.config = yaml.safe_load(config)
        else:
            with open(config, "r") as fp:
                self.config = yaml.safe_load(fp) 

        if not isinstance(self.config, dict):
            raise ValueError("Validation rules must be a YAML mapping.")
        if "rules" not in self.config or not isinstance(self.config["rules"], list):
            raise ValueError("Validation rules must contain a 'rules' list.")

    def validate(self, sample: Dict) -> Dict:
        """
        Validates a data sample against the configured rules.

        Parameters
        ----------
        sample : Dict
            The data sample to be validated.

        Returns
        -------
        Dict
            A dictionary containing the input sample and the validation results.
        """
        validations = []
        for rule in self.config["rules"]:
            rule_name = rule["name"]
            feature_path = rule["feature"]
            specs = rule["specs"]

            if rule_name == "domain":
                validations.append(self.check_domain(sample, feature_path, specs))
            elif rule_name == "strlen":
                validations.append(self.check_strlen(sample, feature_path, specs))
            elif rule_name == "datatype":
                validations.append(self.check_datatype(sample, feature_path, specs))
            elif rule_name == "categorical":
                validations.append(self.check_categorical(sample, feature_path, specs))
            elif rule_name == "exists":
                validations.append(self.exists(sample, feature_path, specs))
            elif rule_name == "regex":
                validations.append(self.check_regex(sample, feature_path, specs))

        return {
            "validatorID": self.validatorID,
            "sample": sample,
            "validations": validations,
            "ts": time.time_ns()
        }

    def check_domain(self, sample: Dict, feature_path: str, specs: Dict) -> Dict:
        """
        Checks if the value(s) of a feature are within a specified domain.

        Parameters
        ----------
        sample : Dict
            The data sample to be validated.
        feature_path : str
            The path to the feature in the sample.
        specs : Dict
            The specifications for the domain check, including the minimum
            and maximum values.

        Returns
        -------
        Dict
            A dictionary containing the check type, feature path, and the
            results of the checks.
        """
        checks = []
        values = self._get_values(sample, feature_path)
        min_value = specs.get("min")
        max_value = specs.get("max")
        coerce_numeric_strings = specs.get("coerce_numeric_strings", False)

        for value in values:
            # Skip comparisons when value is None; treat as failing the check.
            if value is None:
                checks.append(False)
                continue
            if coerce_numeric_strings and isinstance(value, str):
                try:
                    value = float(value) if "." in value or "e" in value.lower() else int(value)
                except Exception:
                    checks.append(False)
                    continue
            if min_value is not None and max_value is not None:
                checks.append(min_value <= value <= max_value)
            elif min_value is not None:
                checks.append(value >= min_value)
            elif max_value is not None:
                checks.append(value <= max_value)

        return {
            "type": "domain",
            "feature": feature_path,
            "checks": checks,
            "result": all(checks),
            "description": ""
        }

    def check_strlen(self, sample: Dict, feature_path: str, specs: Dict) -> Dict:
        """
        Checks if the string length of a feature's value(s) meets a specified
        condition.

        Parameters
        ----------
        sample : Dict
            The data sample to be validated.
        feature_path : str
            The path to the feature in the sample.
        specs : Dict
            The specifications for the string length check, including the
            length type (EXACT, LOWER, or UPPER) and the target length.

        Returns
        -------
        Dict
            A dictionary containing the check type, feature path, and the
            results of the checks.
        """
        checks = []
        values = self._get_values(sample, feature_path)
        len_type = specs["lenType"]
        length = specs["len"]

        for value in values:
            value_str = str(value)
            if len_type == "EXACT":
                checks.append(len(value_str) == length)
            elif len_type == "LOWER":
                checks.append(len(value_str) < length)
            elif len_type == "UPPER":
                checks.append(len(value_str) > length)

        return {
            "type": "strlen",
            "feature": feature_path,
            "checks": checks,
            "result": all(checks),
            "description": ""
        }

    def check_datatype(self, sample: Dict, feature_path: str, specs: Dict) -> Dict:
        """
        Checks if the data type of a feature's value(s) matches a specified
        type.

        Parameters
        ----------
        sample : Dict
            The data sample to be validated.
        feature_path : str
            The path to the feature in the sample.
        specs : Dict
            The specifications for the data type check, including the expected
            data type (STRING, INTEGER, FLOAT, or BOOLEAN).

        Returns
        -------
        Dict
            A dictionary containing the check type, feature path, and the
            results of the checks.
        """
        values = self._get_values(sample, feature_path)
        data_type = specs["type"]
        coerce_numeric_strings = specs.get("coerce_numeric_strings", False)
        checks = []

        for value in values:
            if data_type == "STRING":
                checks.append(isinstance(value, str))
            elif data_type == "INTEGER":
                if isinstance(value, int):
                    checks.append(True)
                    continue
                if coerce_numeric_strings and isinstance(value, str):
                    try:
                        int(value)
                        checks.append(True)
                        continue
                    except Exception:
                        pass
                checks.append(False)
            elif data_type == "FLOAT":
                if isinstance(value, (int, float)):
                    checks.append(True)
                    continue
                if coerce_numeric_strings and isinstance(value, str):
                    try:
                        float(value)
                        checks.append(True)
                        continue
                    except Exception:
                        pass
                checks.append(False)
            elif data_type == "BOOLEAN":
                checks.append(isinstance(value, bool))

        return {
            "type": "datatype",
            "feature": feature_path,
            "checks": checks,
            "result": all(checks),
            "description": ""
        }

    def check_categorical(self, sample: Dict, feature_path: str, specs: Dict) -> Dict:
        """
        Checks if a feature's value(s) are within a list of allowed values.

        Parameters
        ----------
        sample : Dict
            The data sample to be validated.
        feature_path : str
            The path to the feature in the sample.
        specs : Dict
            The specifications for the categorical check, including the list
            of allowed values.

        Returns
        -------
        Dict
            A dictionary containing the check type, feature path, and the
            results of the checks.
        """
        checks = []
        values = self._get_values(sample, feature_path)
        allowed_values = specs["values"]

        for value in values:
            checks.append(value in allowed_values)

        return {
            "type": "categorical",
            "feature": feature_path,
            "checks": checks,
            "result": all(checks),
            "description": ""
        }

    def exists(self, sample: Dict, feature_path: str, specs: Dict) -> Dict:
        """
        Checks if a feature exists or is missing in the data sample.

        Parameters
        ----------
        sample : Dict
            The data sample to be validated.
        feature_path : str
            The path to the feature in the sample.
        specs : DictThe specifications for the existence check, indicating whether
            the feature should exist or be missing.

        Returns
        -------
        Dict
            A dictionary containing the check type, feature path, and the
            results of the checks.
        """

        def split_on_last_dot(s):
            # Find the position of the last '.'
            last_dot_index = s.rfind('.')
            
            if last_dot_index == -1:
                # No dot found, return the original string and an empty string
                return s, ''
            
            # Split the string into before and after the last dot
            before_last_dot = s[:last_dot_index]
            after_last_dot = s[last_dot_index + 1:]
            
            return before_last_dot, after_last_dot

        feature_path, key = split_on_last_dot(feature_path)

        print(sample)
        values = self._get_values(sample, feature_path)
        should_exist = specs['exists']
        checks = []
        print(values)
        for value in values:
            if should_exist:
                try:
                    checks.append(key in value)
                except TypeError:
                    checks.append(False)
            else:
                try:
                    checks.append(not key in value)
                except TypeError:
                    checks.append(True)

        return {
            "type": "missing",
            "feature": feature_path + '.' + key,
            "checks": checks,
            "result": all(checks),
            "description": ""
        }

    def check_regex(self, sample: Dict, feature_path: str, specs: Dict) -> Dict:
        """
        Checks if a feature's value(s) match a specified regular expression.

        Parameters
        ----------
        sample : Dict
            The data sample to be validated.
        feature_path : str
            The path to the feature in the sample.
        specs : Dict
            The specifications for the regular expression check, including the
            regular expression pattern.

        Returns
        -------
        Dict
            A dictionary containing the check type, feature path, and the
            results of the checks.
        """
        checks = []
        values = self._get_values(sample, feature_path)
        regex_pattern = specs["regex"]

        for value in values:
            checks.append(bool(re.fullmatch(regex_pattern, str(value))))

        return {
            "type": "regex",
            "feature": feature_path,
            "checks": checks,
            "result": all(checks),
            "description": ""
        }

    def _get_values(self, sample: Dict, feature_path: str) -> List[Union[str, int, float, bool]]:
        """
        Retrieves the value(s) of a feature from the data sample using the
        given feature path.

        Parameters
        ----------
        sample : Dict
            The data sample from which to retrieve the feature value(s).
        feature_path : str
            The path to the feature in the sample. If the path is '*', all
            values in the sample are returned.

        Returns
        -------
        List[Union[str, int, float, bool]]
            A list of values corresponding to the specified feature path.
        """
       
        def extract_values(data):
            if isinstance(data, dict):
                values = []
                for key, value in data.items():
                    values.extend(extract_values(value))
                return values
            elif isinstance(data, list):
                values = []
                for item in data:
                    values.extend(extract_values(item))
                return values
            else:
                return [data]
               
       
        
        if feature_path == '*':
            return extract_values(sample)
        else:
            values = jmespath.search(feature_path, sample)
            if isinstance(values, List):
                return values
            else:
                return [values]
