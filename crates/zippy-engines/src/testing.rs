use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

pub fn hash_record_batches(batches: &[RecordBatch]) -> String {
    let mut payload = Vec::new();

    if let Some(first_batch) = batches.first() {
        let mut writer = StreamWriter::try_new(&mut payload, &first_batch.schema())
            .expect("failed to create arrow ipc writer for deterministic hashing");

        for batch in batches {
            writer
                .write(batch)
                .expect("failed to write batch into deterministic hash payload");
        }

        writer
            .finish()
            .expect("failed to finish deterministic hash payload");
    }

    format!("{:x}", md5::compute(payload))
}
