#![allow(dead_code)]

use sqlx::PgPool;
use colored::Colorize;
use prettytable::{
    format::{self},
    row, Cell, Row,
};


#[derive(sqlx::FromRow, Debug)]
pub struct RowData {
    pub lsn: String,
    pub xid: String,
    pub data: Option<Vec<u8>>,
}

#[derive(sqlx::FromRow, Debug)]
struct ReplicationSlotRow {
    pub slot_name: Option<String>,
    pub plugin: Option<String>,
    pub slot_type: Option<String>,
    pub datoid: Option<i32>,
    pub database: Option<String>,
    pub temporary: Option<bool>,
    pub active: Option<bool>,
    pub active_pid: Option<i32>,
    pub xmin: Option<i32>,
    pub catalog_xmin: Option<i32>,
    pub restart_lsn: Option<i64>,
    pub confirmed_flush_lsn: Option<i64>,
    pub wal_status: Option<String>,
    pub safe_wal_size: Option<i64>,
    pub two_phase: Option<bool>,
    pub conflicting: Option<bool>,
}

#[derive(sqlx::FromRow, Debug)]
struct PublicationRow {
    pub oid: i32,
    pub pubname: String,
    pub pubowner: i32,
    pub puballtables: bool,
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
    pub pubviaroot: bool,
}


fn get_db_name(pool: &PgPool) -> Option<String> {
    pool.connect_options().get_database().map(|s| s.to_string())
}



pub async fn print_replication_slots(pool: &PgPool) -> Result<(), sqlx::Error> {
    let existing_slots = sqlx::query_as!(
        ReplicationSlotRow,
        r#"
        SELECT 
            slot_name,
            plugin,
            slot_type,
            datoid as "datoid: i32",
            database,
            temporary,
            active,
            active_pid,
            xmin as "xmin: i32",
            catalog_xmin as "catalog_xmin: i32",
            restart_lsn as "restart_lsn: i64",
            confirmed_flush_lsn as "confirmed_flush_lsn: i64",
            wal_status,
            safe_wal_size,
            two_phase,
            conflicting
        FROM 
            pg_replication_slots;
        "#
    )
    .fetch_all(pool)
    .await?;

    let db_name = get_db_name(pool);

    let mut table = prettytable::Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    table.set_titles(row![
        "slot_name",
        "plugin",
        "slot_type",
        "database",
        "temporary"
    ]);
    for slot in existing_slots.iter() {
        let is_same_db = db_name.as_deref() == slot.database.as_deref();
        let db_style = if is_same_db { "By" } else { "" };
        table.add_row(Row::new(vec![
            Cell::new(slot.slot_name.as_ref().unwrap_or(&"NULL".to_string())).style_spec("bFg"),
            Cell::new(slot.plugin.as_ref().unwrap_or(&"NULL".to_string())),
            Cell::new(slot.slot_type.as_ref().unwrap_or(&"NULL".to_string())),
            Cell::new(slot.database.as_ref().unwrap_or(&"NULL".to_string())).style_spec(db_style),
            Cell::new(match slot.temporary {
                Some(true) => "true",
                Some(false) => "false",
                None => "NULL",
            }),
        ]));
    }
    println!("{}", "Existing slots:".green().bold());
    table.printstd();
    println!("\n");

    Ok(())
}

pub async fn print_publications(pool: &PgPool) -> Result<(), sqlx::Error> {
    let existing_publications = sqlx::query_as!(
        PublicationRow,
        r#"
        SELECT 
            oid as "oid: i32",
            pubname,
            pubowner as "pubowner: i32",
            puballtables,
            pubinsert,
            pubupdate,
            pubdelete,
            pubtruncate,
            pubviaroot
        FROM 
            pg_publication;
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut table = prettytable::Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    table.set_titles(row![
        "pubname",
        "puballtables",
        "pubinsert",
        "pubupdate",
        "pubdelete",
        "pubtruncate",
        "pubviaroot"
    ]);
    for publ in existing_publications.iter() {
        table.add_row(Row::new(vec![
            Cell::new(&publ.pubname.to_string()).style_spec("bFg"),
            Cell::new(&publ.puballtables.to_string()),
            Cell::new(&publ.pubinsert.to_string()),
            Cell::new(&publ.pubupdate.to_string()),
            Cell::new(&publ.pubdelete.to_string()),
            Cell::new(&publ.pubtruncate.to_string()),
            Cell::new(&publ.pubviaroot.to_string()),
        ]));
    }
    println!("{}", "Existing publications:".green().bold());
    table.printstd();
    println!("\n");

    Ok(())
}
