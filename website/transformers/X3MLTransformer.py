import io
import xml.etree.ElementTree as ET
from itertools import chain
from typing import Dict, List, Literal, Union
from xml.dom import minidom

from pyairtable.api.types import RecordDict
from pyairtable.formulas import EQUAL, OR, STR_VALUE, match

from website.db import decrypt, dict_gen_one, generate_airtable_schema, get_db
from website.transformers.Transformer import Transformer
from ZellijData.AirTableConnection import AirTableConnection


class X3MLTransformer(Transformer):
    collection_cache: Dict[str, RecordDict] = {}
    literal_uris = {
        "xsd:date": "http://www.w3.org/2001/XMLSchema#dateTime",
        "xsd:dateTime": "http://www.w3.org/2001/XMLSchema#dateTime",
        "rdf:literal": "http://www.w3.org/2001/XMLSchema#string",
    }

    def _fetch_scraper_definition(self):
        database = get_db()
        c = database.cursor()
        c.execute(
            "SELECT * FROM AirTableDatabases WHERE dbaseapikey=%s", (self.api_key,)
        )
        self.scraper_definition = dict_gen_one(c)
        c.close()

    def __init__(
        self, api_key: str, pattern: str, model_id: str, field_id: Union[str, None]
    ):
        self.model_id = model_id
        self.field_id = field_id
        self.api_key = api_key
        self.field: Union[RecordDict, None] = None

        schemas, secretkey = generate_airtable_schema(api_key)
        self.airtable = AirTableConnection(decrypt(secretkey), api_key)
        self.pattern = pattern
        self._fetch_scraper_definition()

        schema = schemas[pattern]
        for tablename, fieldlist in schema.items():
            if not isinstance(fieldlist, dict):
                continue
            if "GroupBy" in fieldlist:
                self.field_table = tablename
                self.field_table_group_by = fieldlist["GroupBy"]
            else:
                self.pattern = tablename

        if field_id:
            self.field = self.airtable.get_record_by_formula(
                "Field", match({"ID": field_id})
            ) or self.airtable.get_record_by_id("Field", field_id)
            if self.field:
                self.file_name = (
                    self.field.get("fields", {})
                    .get("System_Name", "")
                    .replace(" ", "_")
                )

        self.model = None
        if model_id and not field_id:
            self._fetch_model()
            if self.model:
                self.file_name = (
                    self.model.get("fields", {})
                    .get("System_Name", "")
                    .replace(" ", "_")
                )

    def _fetch_model(self):
        models = self.get_records(self.model_id, self.pattern)
        if len(models) == 1:
            self.model = models[0]

    def _fetch_model_fields(self):
        if not self.model:
            return []

        searchtext = self.model["fields"]["ID"]
        model_fields_ids = list(
            map(
                lambda x: x["fields"]["Field"][0]
                if len(x["fields"]["Field"][0]) > 0
                else x["fields"]["Field"],
                sorted(
                    self.airtable.get_multiple_records_by_formula(
                        self.field_table,
                        f'SEARCH("{searchtext}",{{{self.field_table_group_by}}})',
                    ),
                    key=lambda x: x["fields"]["Canonical_Field_Order"]
                    if "Canonical_Field_Order" in x["fields"]
                    else x["fields"]["Model_Specific_Field_Order"],
                ),
            )
        )

        fields = list(
            filter(
                lambda field: len(
                    field.get("fields", {}).get("Collection_Deployed", "")
                )
                > 0,
                self.airtable.get_multiple_records_by_formula(
                    "Field",
                    OR(
                        *list(
                            map(
                                lambda x: EQUAL(STR_VALUE(x), "RECORD_ID()"),
                                model_fields_ids,
                            )
                        )
                    ),
                ),
            )
        )

        return sorted(fields, key=lambda x: model_fields_ids.index(x["id"]))

    def get_major_number_of_part(self, part: str) -> str:
        if "[" not in part:
            return part

        return part.split("[")[-1].split("]")[0].split("_")[0]

    def _parse_xml(self, root: ET.Element) -> str:
        rough_string = ET.tostring(root, "utf-8")
        return minidom.parseString(rough_string).toprettyxml(indent=4 * " ")

    def _create_export_file(self, document: str) -> io.BytesIO:
        file = io.BytesIO()
        file.name = f"{self.file_name}.x3ml"
        file.write(document.encode("utf-8"))
        file.seek(0)

        return file

    def upload(self, form: Union[Literal["a"], Literal["b"]]):
        column_name = "x3ml_a" if form == "a" else "x3ml_b"
        table_name = "Field" if self.field_id else self.pattern
        record_id = self.field_id if self.field_id else self.model_id

        base_api_key = self.api_key
        if self.scraper_definition is not None and self.scraper_definition["fieldbase"]:
            base_api_key = self.scraper_definition["fieldbase"]

        base_field = None
        try:
            base_field = self.airtable.airtable.table(
                base_id=base_api_key, table_name=table_name
            ).first(formula=match({"ID": record_id}))
        except Exception as e:
            print("Error getting Field: ", e)

        if base_field is None:
            table = self.airtable.airtable.table(
                base_id=self.api_key, table_name=table_name
            )
            base_field = table.first(
                formula=match({"ID": record_id})
            ) or self.airtable.get_record_by_id("Field", record_id)

            if base_field is not None:
                base_api_key = self.api_key
            else:
                raise ValueError(
                    "Field already exists in Field table, but not in Field table in the Field Base"
                )

        try:
            self.airtable.airtable.table(
                base_id=base_api_key, table_name=table_name
            ).update(base_field.get("id"), {column_name: self.content})
        except Exception as e:
            print("Error uploading X3ML: ", e)
            raise e

    def _parse_ontological_path(self, path: str) -> List[str]:
        long_discriminator = "-->"
        short_discriminator = "->"
        stripped_path = path.strip(short_discriminator).strip(long_discriminator)

        parts = stripped_path.split(long_discriminator)
        parts = list(map(lambda x: x.split(short_discriminator), parts))

        return list(chain.from_iterable(parts))

    def _extract_entity_variable(self, entity: str) -> str:
        if "[" not in entity:
            return ""

        return entity.split("[")[-1].split("]")[0]

    def _populate_namespaces(self, root: ET.Element) -> None:
        records = self.airtable.get_all_records_from_table("Ontology")
        namespaces = ET.SubElement(root, "namespaces")

        for record in records:
            namespace = ET.SubElement(namespaces, "namespace")
            namespace.attrib["prefix"] = record["fields"]["Prefix"]
            namespace.attrib["uri"] = record["fields"]["Namespace"]

    def _get_collection_name(self, collection_id: str) -> str:
        cache_collection_id = (
            collection_id[0] if isinstance(collection_id, list) else collection_id
        )
        if cache_collection_id in self.collection_cache:
            return (
                self.collection_cache[cache_collection_id]
                .get("fields", {})
                .get("ID", "")
            )

        if self.scraper_definition and self.scraper_definition["collectionbase"]:
            collection_fields = self.get_records(
                collection_id,
                "Collection",
                api_key=self.scraper_definition["collectionbase"],
            )
        else:
            collection_fields = self.get_records(collection_id, "Collection")
        if len(collection_fields) == 1:
            self.collection_cache[cache_collection_id] = collection_fields[0]
            return collection_fields[0].get("fields", {}).get("ID", "")

        return ""

    def _add_mapping_domain(
        self, root: ET.Element, template: str, parts: Union[List[str], None] = None
    ) -> None:
        domain = ET.SubElement(root, "domain")
        domain.attrib["template"] = template
        source_node = ET.SubElement(domain, "source_node")
        target_node = ET.SubElement(domain, "target_node")
        entity = ET.SubElement(target_node, "entity")
        entity_type = ET.SubElement(entity, "type")
        entity_type.text = template

        if parts:
            relationship = ET.SubElement(source_node, "relationship")
            relationship.text = parts[0]
            self._self_populate_entity_node(entity, parts[-1])
        else:
            instance_generator = ET.SubElement(entity, "instance_generator")
            instance_generator.attrib["name"] = "UUID"

    def _create_instance_generator_arg(
        self, parent: ET.Element, name: str, type: str, content: str
    ) -> None:
        arg = ET.SubElement(parent, "arg")
        arg.attrib["name"] = name
        arg.attrib["type"] = type
        arg.text = content

    def _self_populate_entity_node(self, parent: ET.Element, part: str) -> None:
        entity = ET.SubElement(parent, "entity")
        entity_type = ET.SubElement(entity, "type")
        entity_instance_generator = ET.SubElement(entity, "instance_generator")

        if part in ["rdf:literal", "xsd:date", "xsd:dateTime", "xsd:time"]:
            entity_type.text = self.literal_uris[part]
            entity_instance_generator.attrib["name"] = "Literal"
            self._create_instance_generator_arg(
                entity_instance_generator, "text", "xpath", "text()"
            )
            self._create_instance_generator_arg(
                entity_instance_generator, "language", "constant", "en"
            )
        else:
            entity.attrib["variable"] = self._extract_entity_variable(part)
            entity_type.text = part.split("[")[0] if "[" in part else part
            entity_instance_generator.attrib["name"] = "UUID"

    def _add_field(
        self,
        parent: ET.Element,
        field_id: Union[str, RecordDict],
        form: Union[Literal["a"], Literal["b"]],
        first_part: bool = True,
    ) -> List[str]:
        field: Union[RecordDict, None] = None
        if field_id == self.field_id:
            field = self.field
        elif not isinstance(field_id, str):
            field = field_id
        else:
            records = self.get_records(field_id, "Field")
            field = records[0] if len(records) >= 1 else None

        if field is None:
            return []

        link = ET.SubElement(parent, "link")
        if form == "a":
            link.attrib["template"] = field.get("fields", {}).get("ID", "")
        elif form == "b" and first_part:
            link.attrib["template"] = self._get_collection_name(
                field.get("fields", {}).get("Collection_Deployed", "")
            )
        elif form == "b" and not first_part:
            link.attrib["template"] = field.get("fields", {}).get("ID", "")

        path = ET.SubElement(link, "path")

        source_relation = ET.SubElement(path, "source_relation")
        ET.SubElement(source_relation, "relation")

        target_relation = ET.SubElement(path, "target_relation")
        total_path = field.get("fields", {}).get("Ontological_Long_Path") or field.get(
            "fields", {}
        ).get("Ontological_Path")

        if not total_path:
            return []

        parts = self._parse_ontological_path(total_path)
        # for form b take only the last two parts of the path
        if form == "b":
            parts = parts[0:2] if first_part else parts[2:]

        # handles mappings the in the form of "a 'integer' -> rdf:literal"
        parts = list(map(lambda x: x.split(" ")[0], parts))

        for idx, part in enumerate(parts):
            # if it is even then it is a relation else is an entity
            if idx % 2 == 0:
                relationship = ET.SubElement(target_relation, "relationship")
                relationship.text = part
            else:
                if idx == len(parts) - 1:
                    range = ET.SubElement(link, "range")
                    ET.SubElement(range, "source_node")
                    target_node = ET.SubElement(range, "target_node")
                    self._self_populate_entity_node(target_node, part)
                else:
                    self._self_populate_entity_node(target_relation, part)

        return parts

    def _populate_mappings(
        self, root: ET.Element, form: Union[Literal["a"], Literal["b"]]
    ) -> None:
        mappings = ET.SubElement(root, "mappings")
        mapping = ET.SubElement(mappings, "mapping")

        if form == "a":
            if self.field_id and self.field:
                self._add_mapping_domain(
                    mapping,
                    self._get_collection_name(
                        self.field.get("fields", {}).get("Collection_Deployed", "")
                    ),
                )
                self._add_field(mapping, self.field_id, form)
            elif self.model_id and self.model:
                self._add_mapping_domain(
                    mapping, self.model.get("fields", {}).get("ID", "")
                )

                for field in self._fetch_model_fields():
                    self._add_field(mapping, field, form)
            else:
                print("No model or field found")
        else:
            if self.model is None:
                self._fetch_model()

            assert self.model is not None

            if self.field and self.field_id:
                self._add_mapping_domain(
                    mapping, self.model.get("fields", {}).get("ID", "")
                )
                domain_path_parts = self._add_field(mapping, self.field_id, form, True)

                model_mapping = ET.SubElement(mappings, "mapping")
                self._add_mapping_domain(
                    model_mapping,
                    self._get_collection_name(
                        self.field.get("fields", {}).get("Collection_Deployed", "")
                    ),
                    domain_path_parts,
                )
                self._add_field(model_mapping, self.field_id, form, False)
            else:
                self._add_mapping_domain(
                    mapping, self.model.get("fields", {}).get("ID", "")
                )
                inserted_collections: Dict[str, ET.Element] = {}

                for field in self._fetch_model_fields():
                    collection_name = self._get_collection_name(
                        field.get("fields", {}).get("Collection_Deployed", "")
                    )

                    if collection_name not in inserted_collections:
                        domain_path_parts = self._add_field(mapping, field, form, True)

                    if collection_name in inserted_collections:
                        model_mapping = inserted_collections[collection_name]
                    else:
                        model_mapping = ET.SubElement(mappings, "mapping")
                        self._add_mapping_domain(
                            model_mapping,
                            collection_name,
                            domain_path_parts,
                        )
                        inserted_collections[collection_name] = model_mapping

                    self._add_field(model_mapping, field, form, False)

    def transform(self, form: Union[Literal["a"], Literal["b"]]) -> io.BytesIO:
        root = ET.Element("x3ml")

        if form != "a" and form != "b":
            raise ValueError("Form must be either 'a' or 'b'")

        self._populate_namespaces(root)
        self._populate_mappings(root, form)

        self.content = self._parse_xml(root)

        return self._create_export_file(self.content)
