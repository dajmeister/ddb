from argparse import ArgumentParser, Namespace
from decimal import Decimal

from boto3 import Session
from boto3.dynamodb.types import NUMBER, STRING, TypeDeserializer, TypeSerializer
from pydantic import BaseModel, ConfigDict, Field, create_model

session = Session()
dynamodb = session.client("dynamodb")
serializer = TypeSerializer()
deserializer = TypeDeserializer()

type_map = {
    STRING: str,
    NUMBER: Decimal,
}


class Attribute(BaseModel):
    name: str = Field(alias="AttributeName")
    dynamodb_type: str = Field(alias="AttributeType")


class KeySchema(BaseModel):
    partition: Attribute = Field(alias="HASH")
    sort: Attribute | None = Field(alias="RANGE", default=None)


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("table_name")
    parser.add_argument("parameters", nargs="*")
    return parser.parse_args()


def get_key_schema(table_name: str) -> KeySchema:
    table = dynamodb.describe_table(TableName=table_name)["Table"]
    attributes = {
        attribute["AttributeName"]: attribute
        for attribute in table["AttributeDefinitions"]
    }
    keys = {
        key["KeyType"]: attributes[key["AttributeName"]] for key in table["KeySchema"]
    }
    return KeySchema.model_validate(keys)


def serialize_item(item: dict) -> dict:
    return {key: serializer.serialize(value) for key, value in item.items()}


def deserialize_item(item: dict) -> dict:
    return {key: deserializer.deserialize(value) for key, value in item.items()}


def main():
    args = parse_args()
    keys = get_key_schema(args.table_name)
    partition_field = {keys.partition.name: type_map[keys.partition.dynamodb_type]}
    ItemKeys = create_model("ItemKeys", **partition_field)
    Item = create_model("Item", __base__=ItemKeys, __config__=ConfigDict(extra="allow"))
    input_keys = ItemKeys.model_validate({keys.partition.name: args.parameters[0]})

    get_args = {
        "TableName": args.table_name,
        "Key": serialize_item(input_keys.model_dump()),
    }
    get_result = dynamodb.get_item(**get_args)
    item = Item.model_validate(deserialize_item(get_result["Item"]))

    print(item.model_dump_json(indent=4))


if __name__ == "__main__":
    main()
