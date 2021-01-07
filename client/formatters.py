from xml.etree import ElementTree
import json

def get_binding_type(value):
    # literal case
    if value.startswith("\""):
        extra_label, extra_value = None, None
        if "\"^^<http" in value:
            index = value.rfind("\"^^<http")
            extra_label, extra_value = "datatype", value[index + 4:len(value) - 1]
            value = value[0:index + 1]
        elif "\"^^http" in value:
            index = value.rfind("\"^^http")
            extra_label, extra_value = "datatype", value[index + 3:]
            value = value[0:index + 1]
        elif "\"@" in value:
            index = value.rfind("\"@")
            extra_label, extra_value = "xml:lang", value[index + 2:]
            value = value[0:index + 1]
        return value[1:len(value) - 1], "literal", extra_label, extra_value
    else:
        # as the dataset is blank-node free, all other values are uris
        return value, "uri", None, None

def binding_to_json(binding):
    """Format a set of solutions bindings in the W3C SPARQL JSON format"""
    json_binding = dict()
    for variable, value in binding.items():
        variable = variable[1:]
        json_binding[variable] = dict()
        value, type, extra_label, extra_value = get_binding_type(value.strip())
        json_binding[variable]["value"] = value
        json_binding[variable]["type"] = type
        if extra_label is not None:
            json_binding[variable][extra_label] = extra_value
    return json_binding

def skolemize_one(value):
    return "http://example.org/bnode#{}".format(value[2:]) if type(value) is str and value.startswith("_:") else value

def skolemize(bindings):
    """Skolemize blank nodes"""
    res = list()
    for b in bindings:
        elt = dict()
        for key, value in b.items():
            elt[key] = skolemize_one(value)
        res.append(elt)
    return res

def sparql_xml(bindings_list):
    """Formats a set of bindings into SPARQL results in JSON formats."""

    def convert_binding(b, root):
        result_node = ElementTree.SubElement(root, "result")
        for variable, value in b.items():
            v_name = variable[1:]
            b_node = ElementTree.SubElement(result_node, "binding", name=v_name)
            value, type, extra_label, extra_value = get_binding_type(value.strip())
            if type == "uri":
                uri_node = ElementTree.SubElement(b_node, "uri")
                uri_node.text = value
            elif type == "literal":
                literal_node = literal_node = ElementTree.SubElement(b_node, "literal")
                literal_node.text = value
                if extra_label is not None:
                    literal_node.set(extra_label, extra_value)
        return result_node

    variables = list(map(lambda x: x[1:], bindings_list[0].keys()))
    root = ElementTree.Element("sparql", xmlns="http://www.w3.org/2005/sparql-results#")
    # build head
    head = ElementTree.SubElement(root, "head")
    for variable in variables:
        ElementTree.SubElement(head, "variable", name=variable)
    # build results
    results = ElementTree.SubElement(root, "results")
    for binding in bindings_list:
        convert_binding(binding, results)
    return ElementTree.tostring(root, encoding="utf-8").decode("utf-8")

def sparql_json(bindings):
    """Creates a page of SaGe results in the W3C SPARQL JSON results format, compatible with Flask streaming API"""
    variables = list(map(lambda x: x[1:], bindings[0].keys()))
    resutls = ""
    # generate headers
    resutls += "{\"head\":{\"vars\":["
    resutls += ",".join(map(lambda x: "\"{}\"".format(x), variables))
    resutls += "] },\"results\":{\"bindings\":["
    # generate results
    formated_bindings = [json.dumps(binding_to_json(x)) for x in skolemize(bindings)]
    resutls += ','.join(formated_bindings)
    resutls += "]}}"
    return resutls
