from __future__ import annotations


import json
import os
import subprocess
from pathlib import Path
from textwrap import dedent
from typing import Literal
from urllib.parse import urlparse
from rich import print as pprint

import django
from django.apps.registry import Apps
from django.conf import settings
from django.core.management import call_command
from django.db import models
from django.db.migrations import recorder
from django.utils.functional import classproperty
from django.utils.timezone import now
from pydantic import BaseModel, model_validator

DATATYPE_TO_DJANGO = {
    "int": "IntegerField",
    "float": "FloatField",
    "string": "TextField",
}

DATATYPE_TO_RUST = {
    "int": "i64",
    "float": "f64",
    "string": "String",
}


class DataType(BaseModel):
    raw_str: str

    @model_validator(mode="before")
    def parse_raw_string(cls, values):
        if isinstance(values, str):
            return {"raw_str": values}
        return values

    @property
    def rust(self):
        return DATATYPE_TO_RUST[self.raw_str]

    def __str__(self):
        return DATATYPE_TO_DJANGO[self.raw_str]


class SelField(BaseModel):
    raw_str: str
    entity: Entity = None

    def __hash__(self) -> int:
        return hash(self.raw_str)

    def __eq__(self, other) -> bool:
        return self.raw_str == other.raw_str

    def __lt__(self, other) -> bool:
        return self.raw_str < other.raw_str

    @property
    def is_time(self) -> bool:
        return self.raw_str == "Time"

    @property
    def var_name(self) -> str:
        return self.raw_str[0].lower()

    @property
    def rust(self) -> str:
        if self.is_time:
            return "Time"
        return f"{self.entity.rust_ref}"

    @model_validator(mode="before")
    def parse_raw_string(cls, values):
        if isinstance(values, str):
            return {"raw_str": values}
        return values

    def __str__(self):
        if self.raw_str == "Time":
            return "Time"
        return f"Id{self.raw_str}Def"

    def field_definition(self) -> str:
        if self.is_time:
            return "models.DateTimeField()"
        return f'models.ForeignKey({self.entity.table_name}, db_column="{str(self)}", on_delete=models.CASCADE)'


class Column(BaseModel):
    name: str
    data_type: DataType


class Entity(BaseModel):
    name: str
    columns: list[Column]

    @property
    def rust_ref(self) -> str:
        return f"{self.name}"

    @property
    def table_name(self):
        return f"{self.name}Def"


class Selector(BaseModel):
    fields: list[SelField]

    def rust_variant(self) -> str:
        fields_no_time = self.fields[:-1]
        if fields_no_time == []:
            return "Unit"
        variant_name = "".join(f"{f.raw_str}" for f in fields_no_time)
        return variant_name

    def rust_variant_def(self) -> str:
        fields_no_time = self.fields[:-1]
        if fields_no_time == []:
            return "Unit(())"
        fields = ", ".join(f"{f.rust}" for f in fields_no_time)
        return f"{self.rust_variant()}({fields})"

    @model_validator(mode="before")
    def parse_fields(cls, values):
        if isinstance(values, list):
            return {"fields": values}
        return values

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Selector):
            return False
        if len(self.fields) != len(value.fields):
            return False
        for a, b in zip(self.fields, value.fields):
            if a != b:
                return False
        return True

    def __hash__(self) -> int:
        return hash(tuple(self.fields))

    def __lt__(self, other) -> bool:
        return self.fields < other.fields


class Table(BaseModel):
    name: str
    selector: Selector
    time_repr: Literal["Changes"] | Literal["Dense"] | Literal["Interval"]
    columns: list[Column]


class QuantityDb(BaseModel):
    """Database of quantities."""
    
    entities: list[Entity]
    tables: list[Table]


def load_db(quantities_path: Path) -> QuantityDb:
    with open(quantities_path) as f:
        quantities = json.load(f)

    db = QuantityDb(**quantities)
    entity_by_name = {entity.name: entity for entity in db.entities}
    for table in db.tables:
        for sel in table.selector.fields:
            if sel.is_time:
                continue
            sel.entity = entity_by_name[sel.raw_str]

    return db


def write_migrations(path: Path, db: QuantityDb):
    lines = [
        "from __future__ import annotations",
        "",
        "from django.db import models",
        "",
        "",
    ]
    for entity in db.entities:
        lines.extend(serialize_entity(entity))
        lines.append("")
    for table in db.tables:
        lines.extend(serialize_table(table))
        lines.append("")

    models_path = path / "ampiatomigrations" / "models.py"
    models_path.parent.mkdir(parents=True, exist_ok=True)
    with open(models_path, "w") as f:
        f.write("\n".join(lines))


def serialize_entity(entity: Entity) -> list[str]:
    lines = []
    lines.append(f"class {entity.table_name}(models.Model):")
    for column in entity.columns:
        lines.append(f"    {column.name} = models.{column.data_type}()")
    lines += [
        "",
        "    class Meta:",
        '        db_table = "' + entity.table_name + '"',
        "",
    ]
    return lines


def serialize_table(table: Table) -> list[str]:
    lines = []
    lines.append(f"class {table.name}(models.Model):")
    for sel in table.selector.fields:
        lines.append(f"    {sel} = {sel.field_definition()}")
    for column in table.columns:
        lines.append(f"    {column.name} = models.{column.data_type}()")
    lines += [
        "",
        "    class Meta:",
        '        db_table = "' + table.name + '"',
        "        unique_together = ["
        + ", ".join(f'"{s}"' for s in table.selector.fields)
        + "]",
        "",
    ]
    return lines


def execute_migration(path: Path):
    subprocess.run(
        ["python", "manage.py", "makemigrations"],
        cwd=path,
        check=True,
        env={
            **os.environ,
            "DJANGO_SETTINGS_MODULE": "ampiatomigrations.settings_overwrite",
        },
    )
    subprocess.run(
        ["python", "manage.py", "migrate"],
        cwd=path,
        check=True,
        env={
            **os.environ,
            "DJANGO_SETTINGS_MODULE": "ampiatomigrations.settings_overwrite",
        },
    )


def setup_django():
    database_url = os.environ.get("DATABASE_URL", None)
    if not database_url:
        raise Exception("DATABASE_URL not set")
    if not database_url.startswith("postgres://"):
        raise Exception("DATABASE_URL must start with postgres://")
    database_url_parts = urlparse(database_url)

    @classproperty
    def Migration(cls):
        """
        Lazy load to avoid AppRegistryNotReady if installed apps import
        MigrationRecorder.
        """
        if cls._migration_class is None:

            class Migration(models.Model):
                app = models.CharField(max_length=255)
                name = models.CharField(max_length=255)
                applied = models.DateTimeField(default=now)

                class Meta:
                    apps = Apps()
                    app_label = "migrations"
                    db_table = "ampiato_migrations"

                def __str__(self):
                    return "Migration %s for %s" % (self.name, self.app)

            cls._migration_class = Migration
        return cls._migration_class

    recorder.MigrationRecorder.Migration = Migration

    settings.configure(
        DEBUG=True,
        INSTALLED_APPS=(
            "django.contrib.contenttypes",
            "ampiatomigrations",
        ),
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": database_url_parts.path[1:],
                "USER": database_url_parts.username,
                "PASSWORD": database_url_parts.password,
                "HOST": database_url_parts.hostname,
                "PORT": database_url_parts.port,
            }
        },
        USE_TZ=False,
    )
    django.setup()


def render_selector(db: QuantityDb) -> list[str]:
    lines = [
        "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]",
        "pub enum Selector {",
    ]
    selectors = list(sorted({t.selector for t in db.tables}))
    for sel in selectors:
        lines += [
            f"    {sel.rust_variant_def()},",
        ]
    lines += [
        "}",
        "",
    ]

    return lines


def rust_storage_type(table: Table, column: Column) -> str:
    time_series_type = f"TimeSeries{table.time_repr}<{column.data_type.rust}>"
    return f"HashMap<Selector, {time_series_type}>"


def render_entity(entity: Entity) -> list[str]:
    lines = [
        "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Type)]",
        f"pub struct {entity.name}(i64);",
        "",
        f"impl pgoutput::EntityRef for {entity.name} {{",
        "    fn id(&self) -> i64 {",
        "        self.0",
        "    }",
        "",
        "    fn from_entity_id(id: i64) -> Self {",
        "        Self(id)",
        "    }",
        "}",

        "",
        f"pub struct {entity.name}Def {{",
    ]
    for column in entity.columns:
        lines += [
            f"    pub {column.name}: {column.data_type.rust},",
        ]
    lines += [
        "}",
    ]
    return lines


def create_table_query(table: Table, indent: int | None = None) -> str:
    selector_fields = []
    for s in table.selector.fields[:-1]:
        selector_fields.append(f'"{s}",\n        ')
    selector_fields = "".join(selector_fields)
    column_fields = ",\n        ".join(f'"{c.name}"' for c in table.columns)
    query = dedent(f"""
    SELECT
        {selector_fields}EXTRACT(EPOCH FROM "Time")::BIGINT AS "Time",
        {column_fields}
    FROM
        "{table.name}"
    """)
    if indent is not None:
        query = "\n".join(indent * " " + s for s in query.split("\n"))
    return query


def render_table(table: Table) -> list[str]:
    # Struct def
    lines = [
        f"#[derive(Debug, Clone)]" f"pub struct {table.name} {{",
        "     // Selectors",
    ]

    for sel in table.selector.fields:
        lines += [
            f"    pub {sel.raw_str}: {sel.rust},",
        ]

    lines += [
        "",
        "    // Columns",
    ]

    for column in table.columns:
        lines += [
            f"    pub {column.name}: {column.data_type.rust},",
        ]

    selector_no_time = table.selector.fields[:-1]
    if selector_no_time == []:
        selector_fields = "()"
    else:
        selector_fields = ", ".join(
            f"self.{sel.raw_str}" for sel in table.selector.fields[:-1]
        )
    lines += [
        "}",
    ]

    lines += [
        f"impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for {table.name} {{",
        "    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {",
        "        Ok(Self {",
    ]
    for sel in table.selector.fields:
        if sel.is_time:
            lines += [
                f'            {sel.raw_str}: Time(row.try_get("{sel.raw_str}")?),',
            ]
        else:
            lines += [
                f'            {sel.raw_str}: row.try_get("{sel.raw_str}")?,',
            ]
    for column in table.columns:
        lines += [
            f'            {column.name}: row.try_get("{column.name}")?,',
        ]
    lines += [
        "        })",
        "    }",
        "}",
        "",
    ]

    # impl TableValues
    lines += [
        f"impl super::TableMetadata for {table.name} {{",
        "    fn query() -> &'static str {",
        f'        r#"{create_table_query(table, indent=12)}"#',
        "    }",
        "",
        "    fn selector_names() -> Vec<&'static str> {",
        "        vec![",
    ]
    for sel in selector_no_time:
        lines += [
            f'            "{sel.raw_str}",',
        ]
    lines += [
        "        ]",
        "    }",
        "",
        "    fn column_names() -> Vec<&'static str> {",
        "        vec![",
    ]
    for column in table.columns:
        lines += [
            f'            "{column.name}",',
        ]
    lines += [
        "        ]",
        "    }",
        "",
        "    fn table_name() -> &'static str {",
        f'        "{table.name}"',
        "    }",
        "}",
        "",
        f"impl FromTupleData for {table.name} {{",
        "    fn from_tuple_data(tuple_data: &pgoutput::TupleData) -> Result<Self, Error> {",
        "        if tuple_data.number_of_columns != 3 {",
        "            return Err(Error::UnexpectedNumberOfColumns {",
        "                actual: tuple_data.number_of_columns as usize,",
        "                expected: 3,",
        "            });",
        "        }",
        "",
        "        Ok(Self {",
    ]
    i = 0
    for sel in table.selector.fields:
        lines += [
            f'            {sel.raw_str}: Decode::decode(&tuple_data.columns[{i}])?,',
        ]
        i += 1
    for column in table.columns:
        lines += [
            f'            {column.name}: Decode::decode(&tuple_data.columns[{i}])?,',
        ]
        i += 1
    lines += [
        "        })",
        "    }",
        "}",
        "",
        f"impl super::TableValues<Selector> for {table.name} {{",
        "    fn time(&self) -> Time {",
        "        self.Time",
        "    }",
        "",
        "    fn selector(&self) -> Selector {",
        f"        Selector::{table.selector.rust_variant()}({selector_fields})",
        "    }",
        "",
        "    fn values(&self) -> Vec<(&'static str, &f64)> {",
        "        vec![",
    ]
    for column in table.columns:
        lines += [
            f'            ("{table.name}{column.name}", &self.{column.name}),',
        ]
    lines += [
        "        ]",
        "    }",
        "",
        "}",
    ]

    # if tuple_data.number_of_columns != 3 {
    #         return Err(Box::new(Error::UnexpectedNumberOfColumns {
    #             actual: tuple_data.number_of_columns as usize,
    #             expected: 3,
    #         }));
    #     }

    return [4 * " " + l for l in lines]


def render_table_enum(db: QuantityDb) -> list[str]:
    lines = []
    lines += [
        "#[derive(Debug)]",
        "pub enum Table {",
    ]

    for table in db.tables:
        lines += [
            f"    {table.name}(tables::{table.name}),",
        ]

    lines += [
        "}",
        "",
        "impl TableFromTupleData for Table {",
        "    fn from_tuple_data(relation_name: &str, tuple_data: &pgoutput::TupleData) -> Result<Self, Error> {",
        "        match relation_name {",
    ]
    for table in db.tables:
        lines += [
            f'            "{table.name}" => Ok(Table::{table.name}(tables::{table.name}::from_tuple_data(tuple_data)?)),',
        ]
    lines += [
        '            table_name => Err(Error::UnknownTable { table_name: table_name.to_string() }),',
        "        }",
        "    }",
        "}",
        "",
        "impl TableValues<Selector> for Table {",
        "    fn time(&self) -> Time {",
        "        match self {",
    ]
    for table in db.tables:
        lines += [
            f"            Table::{table.name}(t) => t.time(),",
        ]
    lines += [
        "        }",
        "    }",
        "",
        "    fn selector(&self) -> Selector {",
        "        match self {",
    ]
    for table in db.tables:
        lines += [
            f"            Table::{table.name}(t) => t.selector(),",
        ]
    lines += [
        "        }",
        "    }",
        "",
        "    fn values(&self) -> Vec<(&'static str, &f64)> {",
        "        match self {",
    ]
    for table in db.tables:
        lines += [
            f"            Table::{table.name}(t) => t.values(),",
        ]
    lines += [
        "        }",
        "    }",
        "}",
        "",
    ]


    return lines


def render_table_mod(table: Table) -> list[str]:
    lines = []

    lines += [
        f"pub mod {table.name} {{",
    ]

    lines += [
        "    use super::{Selector, Db, Blok};",
        "    use ampiato::Time;",
        "",
    ]

    for column in table.columns:
        selector_non_time = table.selector.fields[:-1]
        if selector_non_time == []:
            selector_args = ""
            selector_variant_args = "()"
        else:
            selector_args = " ".join(
                f"{sel.var_name}: {sel.rust}," for sel in selector_non_time
            )
            selector_variant_args = ", ".join(
                f"{sel.var_name}" for sel in selector_non_time
            )
        lines += [
            f"    pub fn {column.name}(db: &Db, {selector_args} t: Time) -> {column.data_type.rust} {{",
            f'        db.get_value("{table.name}{column.name}", Selector::{table.selector.rust_variant()}({selector_variant_args}), t)',
            "    }",
            "",
        ]

    lines += ["}", ""]

    return lines


def render_prelude(db: QuantityDb) -> list[str]:
    lines = [
        "pub mod prelude {",
        "    pub use ampiato::prelude::*;",
        "    pub use super::{Db, Selector, Table, ValueProvider, load_value_provider};",
        "    pub use ampiato::ast::*;",
        "    pub use ampiato::Time;",
    ]

    for table in db.tables:
        qtys = ", ".join(column.name for column in table.columns)
        lines += [
            f"    pub use super::{table.name}::{{{qtys}}};",
        ]
    entities = ", ".join(entity.name for entity in db.entities)
    lines += [
        f"    pub use super::{{{entities}}};",
    ]

    lines += ["}", ""]
    return lines


def render_value_provider_class(db: QuantityDb) -> list[str]:
    lines = []

    lines += [
        "#[derive(Debug)]",
        "pub struct ValueProvider {",
    ]
    for table in db.tables:
        for column in table.columns:
            lines.append(
                f"    {table.name}{column.name}: {rust_storage_type(table, column)},"
            )
    lines += [
        "}",
        "",
    ]

    lines += [
        "impl ValueProvider {",
        "    pub fn new() -> Self {",
        "        Self {",
    ]
    for table in db.tables:
        for column in table.columns:
            lines += [
                f"            {table.name}{column.name}: HashMap::new(),",
            ]
    lines += [
        "        }",
        "    }",
        "",
        "    fn _get_value_impl(&self, name: &'static str, selector: &Selector, t: &Time) -> Option<f64> {",
        "        match name {",
    ]
    for table in db.tables:
        for column in table.columns:
            lines += [
                f'            "{table.name}{column.name}" => self.{table.name}{column.name}.get(selector)?.get(t),',
            ]
    lines += [
        '            _ => panic!("Unknown quantity {}", name),',
        "        }",
        "    }",
        "}",
        "",
    ]

    lines += [
        "impl ampiato::ValueProvider<Selector> for ValueProvider {",
        "    async fn from_pool(pool: &sqlx::PgPool) -> Self {",
        "        let vp = load_value_provider(pool).await;",
        "        vp",
        "    }",
        "",
        "    fn set_value(&mut self, name: &'static str, selector: Selector, t: Time, value: f64) {",
        "        match name {",
    ]

    for table in db.tables:
        for column in table.columns:
            lines += [
                f'            "{table.name}{column.name}" => self.{table.name}{column.name}.entry(selector).or_default().set(&t, value),',
            ]

    lines += [
        '            name => panic!("Unknown quantity {}", name),',
        "        }",
        "    }",
        "",
    ]

    additional_code = """
    fn get_value(&self, name: &'static str, selector: &Selector, t: &Time) -> f64 {
        match self._get_value_impl(name, selector, t) {
            Some(v) => v,
            None => panic!("Value not found: {}({:?})", name, selector),
        }
    }

    fn get_value_opt(&self, name: &'static str, selector: &Selector, t: &Time) -> Option<f64> {
        self._get_value_impl(name, selector, t)
    }
    """
    lines += additional_code.split("\n")

    lines += [
        "}",
        "",
    ]

    return lines


def render_load_value_provider(db: QuantityDb) -> list[str]:
    lines = []

    lines += [
        "pub async fn load_value_provider(pool: &sqlx::PgPool) -> ValueProvider {",
        "    let mut vp = ValueProvider::new();",
    ]
    for table in db.tables:
        lines += [
            f"    let rows = sqlx::query_as::<_, tables::{table.name}>(&tables::{table.name}::query()).fetch_all(pool).await.unwrap();",
            "     for row in rows {",
            "         let sel = row.selector();",
            "         for (name, value) in row.values() {",
            "             vp.set_value(name, sel, row.Time, *value);",
            "         }",
            "     }",
        ]

    lines += [
        "    vp",
        "}",
        "",
    ]

    return lines


def render_value_provider(path: Path, db: QuantityDb) -> list[str]:
    lines = []

    intro_code = dedent("""
    #![allow(unused_imports, dead_code, non_snake_case)]

    use std::collections::HashMap;

    use ampiato::replication::pgoutput;
    use ampiato::core::BoxDynError;
    use ampiato::replication::pgoutput::Decode;
    use ampiato::replication::TableFromTupleData;
    use ampiato::{Time, TimeSeriesChanges, TimeSeriesDense, ValueProvider as _};
    use ampiato::{Error, TableMetadata, TableValues};
    use ampiato::FromTupleData;
    use sqlx::Row;

    pub type Db = ampiato::Db<Selector, Table, ValueProvider>;

    """)

    lines += intro_code.split("\n")

    for entity in db.entities:
        lines += render_entity(entity)
        lines.append("")

    lines += ["pub mod tables {", "    use super::*;", ""]
    for table in db.tables:
        lines += render_table(table)
        lines.append("")
    lines += ["}"]

    lines += render_table_enum(db)

    lines += render_selector(db)
    lines += render_value_provider_class(db)

    for table in db.tables:
        lines += render_table_mod(table)

    lines += render_load_value_provider(db)

    lines += render_prelude(db)

    return lines


def write_value_provider(path: Path, db: QuantityDb) -> list[str]:
    value_provider = render_value_provider(path, db)
    value_provider_path = path / "src" / "value_provider.rs"
    with open(value_provider_path, "w") as f:
        f.write("\n".join(value_provider))

    subprocess.run(
        ["rustfmt", "--edition", "2021", "src/value_provider.rs"],
        cwd=path,
        check=True,
    )

def write_quantities_schema(path: Path):
    main_model_schema = QuantityDb.model_json_schema()  # (1)!
    with open(path / "quantities.schema.json", "w") as f:
        f.write(json.dumps(main_model_schema, indent=2))



def main():
    print("Ampiato")

    db = load_db(Path("quantities.json"))
    pprint(db)

    write_quantities_schema(Path("."))

    write_migrations(Path("."), db)
    write_value_provider(Path("."), db)

    setup_django()
    call_command("makemigrations", "ampiatomigrations")
    call_command("migrate", "ampiatomigrations")


if __name__ == "__main__":
    main()
