# Unified Data Model Encapsulator

## Overview

The **Unified Data Model Encapsulator** is a custom processor designed for Apache NiFi. It transforms incoming data by encapsulating it into a unified data model. This model enriches the incoming data with additional metadata, such as `sourceType`, `sourceID`, `infoType`, `dataType`, `dataItemID`, and `metricTypeID`.

## Usage

1. **Properties**: Define the following required properties within your NiFi flow:
   - `sourceType`: Type of the data source.
   - `sourceID`: Identifier of the data source.
   - `infoType`: Type of information the data represents.
   - `dataType`: Type of the data.
   - `dataItemID`: Identifier for the data item.
   - `metricTypeID`: Identifies how to interpret the `metricValue`.
