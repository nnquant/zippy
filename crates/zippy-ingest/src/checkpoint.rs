//! Durable receipt index and atomic WAL reclamation checkpoint.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use zippy_types::{
    ErrorCode, OffsetWatermark, ProducerId, RequestId, Result, ServerError, TableId,
};

use crate::native::CachedWrite;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WalCheckpoint {
    pub table_id: TableId,
    pub schema_version: u64,
    pub producer_epoch: u64,
    pub producer_id: ProducerId,
    pub last_sequence: u64,
    pub log_offset: OffsetWatermark,
    pub through_file: String,
}

pub(crate) struct ReceiptArchive {
    connection: Connection,
}

impl ReceiptArchive {
    pub fn open(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory).map_err(archive_error)?;
        let connection =
            Connection::open(directory.join("receipts.sqlite3")).map_err(archive_error)?;
        connection
            .execute_batch(concat!(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-2048;",
                "CREATE TABLE IF NOT EXISTS receipts (",
                "epoch TEXT NOT NULL, sequence TEXT NOT NULL, request TEXT NOT NULL, ",
                "payload BLOB NOT NULL, PRIMARY KEY(epoch, sequence), UNIQUE(epoch, request));",
                "CREATE TABLE IF NOT EXISTS checkpoint (id INTEGER PRIMARY KEY CHECK(id=1), ",
                "payload BLOB NOT NULL);"
            ))
            .map_err(archive_error)?;
        #[cfg(unix)]
        {
            // A new table directory must also be durable in its parent before a WAL
            // acknowledgement can rely on the receipt archive after a crash.
            let parent = directory
                .parent()
                .ok_or_else(|| archive_corruption("receipt archive directory has no parent"))?;
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(archive_error)?;
            std::fs::File::open(directory)
                .and_then(|directory| directory.sync_all())
                .map_err(archive_error)?;
        }
        Ok(Self { connection })
    }

    pub fn lookup(&self, epoch: u64, sequence: u64) -> Result<Option<CachedWrite>> {
        let encoded: Option<Vec<u8>> = self
            .connection
            .query_row(
                "SELECT payload FROM receipts WHERE epoch=?1 AND sequence=?2",
                params![number_key(epoch), number_key(sequence)],
                |row| row.get(0),
            )
            .optional()
            .map_err(archive_error)?;
        encoded
            .map(|bytes| serde_json::from_slice(&bytes).map_err(archive_corruption))
            .transpose()
    }

    pub fn request_sequence(&self, epoch: u64, request_id: RequestId) -> Result<Option<u64>> {
        let sequence: Option<String> = self
            .connection
            .query_row(
                "SELECT sequence FROM receipts WHERE epoch=?1 AND request=?2",
                params![number_key(epoch), request_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(archive_error)?;
        sequence
            .map(|value| value.parse().map_err(archive_corruption))
            .transpose()
    }

    pub fn archive(&mut self, epoch: u64, receipts: &[CachedWrite]) -> Result<()> {
        let transaction = self.connection.transaction().map_err(archive_error)?;
        {
            let mut insert = transaction.prepare_cached(
                "INSERT OR IGNORE INTO receipts(epoch, sequence, request, payload) VALUES(?1,?2,?3,?4)"
            ).map_err(archive_error)?;
            let mut existing = transaction
                .prepare_cached("SELECT payload FROM receipts WHERE epoch=?1 AND sequence=?2")
                .map_err(archive_error)?;
            let epoch = number_key(epoch);
            for cached in receipts {
                let sequence = number_key(cached.receipt.batch_sequence());
                let encoded = serde_json::to_vec(cached).map_err(archive_error)?;
                let inserted = insert
                    .execute(params![
                        epoch,
                        sequence,
                        cached.request_id.to_string(),
                        encoded
                    ])
                    .map_err(archive_error)?;
                if inserted == 0 {
                    let previous: Vec<u8> = existing
                        .query_row(params![epoch, sequence], |row| row.get(0))
                        .map_err(archive_corruption)?;
                    let previous: CachedWrite =
                        serde_json::from_slice(&previous).map_err(archive_corruption)?;
                    if previous.request_id != cached.request_id
                        || previous.fingerprint != cached.fingerprint
                        || previous.receipt.offsets() != cached.receipt.offsets()
                    {
                        return Err(archive_corruption("archived receipt identity conflicts"));
                    }
                }
            }
        }
        transaction.commit().map_err(archive_error)
    }

    /// Stores the first manifest-backed acknowledgement without changing its write identity.
    pub fn promote_persisted(
        &mut self,
        epoch: u64,
        receipt: &zippy_types::ingest::WriteReceipt,
    ) -> Result<zippy_types::ingest::WriteReceipt> {
        use zippy_types::ingest::Durability;
        if receipt.durability_reached() != Durability::Persisted
            || receipt
                .persisted_snapshot_version()
                .is_none_or(|version| version == 0)
        {
            return Err(archive_corruption(
                "persisted receipt has no manifest version",
            ));
        }
        let mut cached = self
            .lookup(epoch, receipt.batch_sequence())?
            .ok_or_else(|| {
                archive_corruption("persisted receipt is absent from the durable archive")
            })?;
        let promoted = crate::native::promote_receipt(
            &cached.receipt,
            Durability::Persisted,
            receipt.persisted_snapshot_version(),
        );
        if promoted != *receipt {
            return Err(archive_corruption(
                "persisted receipt identity conflicts with its archive",
            ));
        }
        if cached.receipt.durability_reached() == Durability::Persisted {
            return Ok(cached.receipt);
        }
        cached.receipt = receipt.clone();
        let encoded = serde_json::to_vec(&cached).map_err(archive_error)?;
        let transaction = self.connection.transaction().map_err(archive_error)?;
        transaction
            .execute(
                "UPDATE receipts SET payload=?3 WHERE epoch=?1 AND sequence=?2",
                params![
                    number_key(epoch),
                    number_key(receipt.batch_sequence()),
                    encoded
                ],
            )
            .map_err(archive_error)?;
        transaction.commit().map_err(archive_error)?;
        Ok(receipt.clone())
    }

    pub fn checkpoint(&self) -> Result<Option<WalCheckpoint>> {
        let encoded: Option<Vec<u8>> = self
            .connection
            .query_row("SELECT payload FROM checkpoint WHERE id=1", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(archive_error)?;
        encoded
            .map(|bytes| serde_json::from_slice(&bytes).map_err(archive_corruption))
            .transpose()
    }

    pub fn commit_checkpoint(&mut self, checkpoint: &WalCheckpoint) -> Result<()> {
        let terminal = self.lookup(checkpoint.producer_epoch, checkpoint.last_sequence)?;
        if !terminal
            .is_some_and(|cached| cached.receipt.committed_log_offset() == checkpoint.log_offset)
        {
            return Err(archive_corruption(
                "checkpoint has no archived terminal receipt",
            ));
        }
        let encoded = serde_json::to_vec(checkpoint).map_err(archive_error)?;
        let transaction = self.connection.transaction().map_err(archive_error)?;
        transaction.execute(
            "INSERT INTO checkpoint(id,payload) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
            params![encoded],
        ).map_err(archive_error)?;
        // A new manifest-backed epoch no longer needs exact receipts from fenced writers.
        transaction
            .execute(
                "DELETE FROM receipts WHERE epoch < ?1",
                params![number_key(checkpoint.producer_epoch)],
            )
            .map_err(archive_error)?;
        transaction.commit().map_err(archive_error)
    }
}

fn number_key(value: u64) -> String {
    format!("{value:020}")
}

fn archive_error(error: impl std::fmt::Display) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!("receipt archive operation failed error=[{error}]"),
    )
    .with_retryable(false)
}

fn archive_corruption(error: impl std::fmt::Display) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!("receipt archive is inconsistent error=[{error}]"),
    )
    .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;
    use zippy_types::ingest::{
        Durability, IncomingWriteRequest, MutationFingerprintComputer, TableMutation, WriteReceipt,
    };

    fn cached(sequence: u64, id: u128) -> CachedWrite {
        let request_id = RequestId::from_u128(id).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]));
        let request = crate::ArrowIpcMutationFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id,
                batch_sequence: sequence,
                schema_version: 1,
                mutation: TableMutation::Append(RecordBatch::new_empty(schema)),
            })
            .unwrap();
        CachedWrite {
            request_id,
            fingerprint: request.mutation_fingerprint(),
            receipt: WriteReceipt::new(
                request_id,
                sequence,
                0,
                None,
                OffsetWatermark::Empty,
                OffsetWatermark::Empty,
                Durability::Wal,
                None,
            )
            .unwrap(),
        }
    }

    #[test]
    fn archive_conflicts_are_atomic_and_checkpoint_survives_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let mut archive = ReceiptArchive::open(temp.path()).unwrap();
        archive.archive(1, &[cached(0, 1)]).unwrap();
        assert!(archive.archive(1, &[cached(1, 2), cached(0, 3)]).is_err());
        assert!(
            archive.lookup(1, 1).unwrap().is_none(),
            "failed batch rolls back every row"
        );
        let mut checkpoint = WalCheckpoint {
            table_id: TableId::from_u128(1).unwrap(),
            producer_id: ProducerId::from_u128(1).unwrap(),
            schema_version: 1,
            producer_epoch: 1,
            last_sequence: 0,
            log_offset: OffsetWatermark::Empty,
            through_file: "00000000000000000001-00000000000000000000.wal".into(),
        };
        archive.commit_checkpoint(&checkpoint).unwrap();
        drop(archive);
        let mut archive = ReceiptArchive::open(temp.path()).unwrap();
        assert_eq!(archive.checkpoint().unwrap().unwrap().last_sequence, 0);
        assert_eq!(
            archive
                .request_sequence(1, RequestId::from_u128(1).unwrap())
                .unwrap(),
            Some(0)
        );
        checkpoint.producer_epoch = 2;
        assert!(
            archive.commit_checkpoint(&checkpoint).is_err(),
            "anchor receipt must be durable first"
        );
        archive.archive(2, &[cached(0, 4)]).unwrap();
        archive.commit_checkpoint(&checkpoint).unwrap();
        assert!(
            archive.lookup(1, 0).unwrap().is_none(),
            "fenced epoch is retired only with a new checkpoint"
        );
        assert!(archive.lookup(2, 0).unwrap().is_some());
    }
}
