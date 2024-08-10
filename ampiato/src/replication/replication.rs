use crate::core::Error;
use sqlx::{PgConnection, PgPool};

use crate::replication::{
    pgoutput::{self, LogicalReplicationMessage},
    print::RowData,
};

pub struct Replication {
    db_connection: PgConnection,
    replication_slot_name: String,
    is_closed: bool,
}

impl Replication {
    pub fn close(&mut self) {
        if self.is_closed {
            return;
        }

        self.is_closed = true;
    }

    pub async fn close_and_cleanup(&mut self) -> Result<(), Error> {
        if self.is_closed {
            return Ok(());
        }

        sqlx::query(r#"DROP PUBLICATION IF EXISTS ampiato;"#)
            .execute(&mut self.db_connection)
            .await?;

        self.close();

        Ok(())
    }

    pub async fn grab_changes(&mut self) -> Result<Vec<LogicalReplicationMessage>, Error> {
        let res = sqlx::query_as!(
            RowData,
            r#"
                SELECT
                    lsn::TEXT as "lsn!",
                    xid::TEXT as "xid!",
                    data
                FROM
                    pg_logical_slot_get_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', 'ampiato');
            "#,
            self.replication_slot_name
        ).fetch_all(&mut self.db_connection).await.unwrap();
        let changes = res
            .iter()
            .map(|row| pgoutput::decode(&row.data.as_deref().unwrap()).unwrap())
            .collect::<Vec<_>>();

        Ok(changes)
    }
}

impl Drop for Replication {
    fn drop(&mut self) {
        self.close();
    }
}

impl Replication {
    pub async fn from_pool(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let mut db_connection = pool.acquire().await?.detach();

        let replication_slot_name = format!("ampiato_slot_{}", rand::random::<u32>());

        sqlx::query(r#"DROP PUBLICATION IF EXISTS ampiato;"#)
            .execute(&mut db_connection)
            .await?;

        sqlx::query!(
            r#"SELECT pg_create_logical_replication_slot($1, 'pgoutput', temporary := true);"#,
            replication_slot_name
        )
        .fetch_one(&mut db_connection)
        .await?;

        sqlx::query(r#"CREATE PUBLICATION ampiato FOR ALL TABLES;"#)
            .execute(&mut db_connection)
            .await?;

        Ok(Replication {
            db_connection,
            replication_slot_name,
            is_closed: false,
        })
    }
}
