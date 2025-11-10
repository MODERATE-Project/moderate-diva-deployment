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
import json
import time

class UnifiedDataModelEncapsulator(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        # Customize as needed.
        version = '2.0.0-M4'
        description = """Unified Data Model Encapsulator"""
        tags = ["links"]


    # Define Property Descriptors
    SOURCE_TYPE = PropertyDescriptor(
        name="sourceType",
        description="Same as in topic specs",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    SOURCE_ID = PropertyDescriptor(
        name="sourceID",
        description="Same as in topic specs",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    INFO_TYPE = PropertyDescriptor(
        name="infoType",
        description="Same as in topic specs",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    DATA_TYPE = PropertyDescriptor(
        name="dataType",
        description="Same as in topic specs",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    DATA_ITEM_ID = PropertyDescriptor(
        name="dataItemID",
        description="Same as in topic specs",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    METRIC_TYPE_ID = PropertyDescriptor(
        name="metricTypeID",
        description="Identifies how to interpret the metricValue",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR], 
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # Add all descriptors to the list
    descriptors = [
        SOURCE_TYPE, 
        SOURCE_ID, 
        INFO_TYPE, 
        DATA_TYPE, 
        DATA_ITEM_ID, 
        METRIC_TYPE_ID
    ]


    def __init__(self, **kwargs):
        """ Use 'pass' even if official docs says otherwise.
        Maybe it's a bug.        
        """
        pass

    def onScheduled(self, context):
        """ Put here all the 'preloading' to improve processor performance.
        """
        pass
    
    def getPropertyDescriptors(self):
        """ Do not change.
        """
        return self.descriptors

    def transform(self, context, flowfile):
        """ Write here all of the processor logic.
        """
        try:
            input_data = json.loads(flowfile.getContentsAsBytes().decode('utf-8'))
            output = { 
                "timestamp": f"{time.time_ns()}",
                "sourceType": context.getProperty(self.SOURCE_TYPE).evaluateAttributeExpressions(flowfile).getValue(), 
                "sourceID": context.getProperty(self.SOURCE_ID).evaluateAttributeExpressions(flowfile).getValue(), 
                "infoType": context.getProperty(self.INFO_TYPE).evaluateAttributeExpressions(flowfile).getValue(), 
                "dataType": context.getProperty(self.DATA_TYPE).evaluateAttributeExpressions(flowfile).getValue(), 
                "dataItemID": context.getProperty(self.DATA_ITEM_ID).evaluateAttributeExpressions(flowfile).getValue(), 
                "metricTypeID": context.getProperty(self.METRIC_TYPE_ID).evaluateAttributeExpressions(flowfile).getValue(),
                "metricValue": input_data
            }
            output = json.dumps(output)
            return FlowFileTransformResult(relationship = "success", contents=output) 
        except Exception as e:
            self.logger.error(e)
            return FlowFileTransformResult(relationship = "failure")

