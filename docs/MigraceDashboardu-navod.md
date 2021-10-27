# Transfer dashboards

## Export

In Kibana go to ``side menu > Stack Management > Save Objects (under Kibana)`` and select dashboard you want.
Click ``Export`` (select Include related objects) and ``Export``. 

## Import

In Kibana go to ``side menu > Stack Management > Save Objects (under Kibana)``.  Click on ``Import``.
Select file by clicking on Import rectangle or mouse over the file (*export.ndjson*) on Import rectangle. Select Import option and click ``Import``.

# Transfer index

### Prerequirements:

 - Install elasticdump tool (https://github.com/elasticsearch-dump/elasticsearch-dump).

	    npm install elasticdump -g

 - Created directory for export (e.g. ``backup``)

## Export

    multielasticdump --includeType=data,mapping --match=index-name-regex --input=http://localhost:9200 --output=backup

## Inport

    multielasticdump --direction=load --includeType=data,mapping --input=backup --output=http://localhost:9200