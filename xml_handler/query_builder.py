
from lxml import etree


def build(query):
    root = etree.Element('Query', client="biomartclient", processor="json", limit="-1", header="1")
    doc = etree.ElementTree(root)
    datasetElement = etree.SubElement(root, "Dataset", name=query["dataset"], config=query["config"])
    for f in query["filters"]:
        filter = etree.SubElement(datasetElement, name=f["name"], value=f["value"], filter_list="")
    for a in query["attributes"]:
        attribute = etree.SubElement(datasetElement, "Attribute", name=a["name"])
    return etree.tostring(doc, doctype="<!DOCTYPE Query>")



