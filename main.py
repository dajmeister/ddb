from argparse import ArgumentParser, Namespace
from boto3 import client
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer, STRING, NUMBER
from pydantic import BaseModel, Field

dynamodb = client("dynamodb")


class Attribute(BaseModel):
    name: str = Field(alias="AttributeName")
    dynamodb_type: str = Field(Alias="AttributeType")


class KeySchema(BaseModel):
    partition: Attribute = Field(alias="HASH")
    sort: Attribute | None = Field(alias="RANGE", default=None)


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("table_name")
    parser.add_argument("parameters", nargs="*")
    return parser.parse_args()


def get_key_schema(table_name: str) -> KeySchema:
    table = dynamodb.describeTable(TableName=table_name)["Table"]
    attributes = {
        attribute["AttributeName"]: attribute
        for attribute in table["AttributeDefinitions"]
    }
    keys = {
        key["KeyType"]: attributes[key["AttributeName"]] for key in table["KeySchema"]
    }
    return KeySchema.validate(keys)


def main():
    print("Hello from ddb!")
    args = parse_args()
    keys = get_key_schema(args.table_name)
    print(args.table_name)
    print(keys.partition.name)
    if keys.partition.dynamodb_type == STRING:
        print("is a string")
    elif keys.partition.dynamodb_type == NUMBER:
        print("is a number")
    else:
        print(f"partition key type: {keys.partition.dynamodb_type} unsupported")
    print(args.parameters)


if __name__ == "__main__":
    main()
