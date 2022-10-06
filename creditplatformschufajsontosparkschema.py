import json
import os
import sys

from pyspark.sql.types import StructType,StructField, StringType, IntegerType


def read_json(path):
    with open(path) as f:
        return json.load(f)


def convert_property(n, t, a):
    return {
        "name": n,
        "type": convert(t, a),
        "nullable": True
    }


def convert_object(i, a):
    return {
        "type": "struct",
        "fields":
            [convert_property(n, t, a) for (n, t) in i["properties"].items()]
    }


def convert_array(i, a):
    return {
        "type": "array",
        "elementType": convert(i["items"], a),
        "containsNull": True
    }


def convert_internal_ref(ref, a):
    return convert(a["definitions"][os.path.basename(ref)], a)


def convert_external_ref(ref):
    owd = os.getcwd()
    (nwd, fn) = os.path.split(ref)
    if nwd != "":
        os.chdir(nwd)
    if fn[-1] == "#":
        fn = fn[:-1]
    a = read_json(fn)
    r = convert(a, a)
    os.chdir(owd)
    return r


def convert_ref(ref, a):
    if ref[0] == "#":
        return convert_internal_ref(ref, a)
    else:
        return convert_external_ref(ref)


def convert(i, a):
    print(
        "converting " + json.dumps(i, separators=(',', ':')),
        file=sys.stderr
    )
    if "$ref" in i:
        return convert_ref(i["$ref"], a)

    t = i["type"]
    if t == "object":
        return convert_object(i, a)
    if t == "array":
        return convert_array(i, a)
    if t == "integer":
        return "integer"
    if t == "string":
        return "string"
    if t == "boolean":
        return "boolean"
    if t == "number":
        return "double"
    if t == ["number", "null"]:
        return "double"
    if t == ["integer", "null"]:
        return "integer"
    if t == ["string", "null"]:
        return "string"
    if t == ["boolean", "null"]:
        return "boolean"
    if t == ["object", "null"]:
        return convert_object(i, a)
    if t == ["array", "null"]:
        return convert_array(i, a)
    else:
        raise Exception(f"Unkonwn type: {t}")



#working one

print(json.dumps(convert_external_ref("C:/Users/KPUVVAD/Desktop/Project/Project_Modules/MBLD/creditPlatform/schufa/JSON-SCHEMA/schufa-processes/schufa-nachmeldungen/response.json"), separators=(',', ':')))

#print(json.dumps(convert_external_ref("C:/Users/KPUVVAD/Desktop/Project/Project_Modules/MBLD/creditPlatform/newCreditPlatformJSON/JSON-SCHEMA/CreditPlatformResponse.json"), separators=(',', ':')))





