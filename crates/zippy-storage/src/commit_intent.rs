use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zippy_core::{DataDirectoryError, DurableRemoveOutcome, OwnedDataDirectory};
use zippy_types::catalog::{
    CatalogActorKind, CatalogCodec, CatalogCommandV1, VersionedCatalogCommand,
};
use zippy_types::table::ManagedDataPath;
use zippy_types::{ErrorCode, RequestId, Result, ServerError};

use crate::candidate::verify_pending_candidate;

pub(crate) const COMMIT_INTENT_RECORD_VERSION: u32 = 1;

const COMMIT_INTENT_DIRECTORY: &str = "wal/catalog-commit-intents";
const MAX_PENDING_INTENT_FILES: usize = 65_536;
const MAX_INTENT_RECORD_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCommitIntentV1 {
    format_version: u32,
    sequence: u64,
    command: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitIntentPath {
    pub(crate) sequence: u64,
    pub(crate) request_id: RequestId,
    pub(crate) managed_path: ManagedDataPath,
}

#[derive(Debug, Clone)]
pub(crate) struct DurableCommitIntent {
    pub(crate) path: CommitIntentPath,
    pub(crate) command: VersionedCatalogCommand,
}

#[derive(Debug)]
pub(crate) struct CommitIntentJournal {
    data_directory: Arc<OwnedDataDirectory>,
}

impl CommitIntentJournal {
    pub(crate) fn new(data_directory: Arc<OwnedDataDirectory>) -> Self {
        Self { data_directory }
    }

    pub(crate) fn scan(&self) -> Result<(Vec<CommitIntentPath>, u64)> {
        let directory = self
            .data_directory
            .directory()
            .root()
            .join(COMMIT_INTENT_DIRECTORY);
        match fs::symlink_metadata(&directory) {
            Ok(metadata) => validate_intent_directory(&directory, &metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok((Vec::new(), 1));
            }
            Err(error) => {
                return Err(io_error(
                    "inspect catalog commit intent directory",
                    &directory,
                    error,
                ));
            }
        }
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) => return Err(io_error("scan catalog commit intents", &directory, error)),
        };

        let mut paths = Vec::new();
        for entry in entries {
            let entry = entry
                .map_err(|error| io_error("read catalog commit intent entry", &directory, error))?;
            let file_name = entry.file_name().into_string().map_err(|_| {
                ServerError::new(
                    ErrorCode::DataCorruption,
                    "catalog commit intent file name is not UTF-8",
                )
            })?;
            if is_temporary_intent(&file_name) {
                let managed_path = managed_path_for_file(&file_name)?;
                self.data_directory
                    .remove_durable_file(&managed_path)
                    .map_err(map_data_directory_error_for_intent)?;
                continue;
            }
            if paths.len() >= MAX_PENDING_INTENT_FILES {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "catalog commit intent count exceeds recovery limit maximum=[{}]",
                        MAX_PENDING_INTENT_FILES
                    ),
                )
                .with_retryable(false));
            }
            paths.push(parse_intent_path(&file_name)?);
        }
        paths.sort_unstable_by_key(|path| (path.sequence, path.request_id));

        let mut request_ids = HashSet::with_capacity(paths.len());
        for pair in paths.windows(2) {
            if pair[0].sequence == pair[1].sequence {
                return Err(corruption(format!(
                    "duplicate catalog commit intent sequence sequence=[{}]",
                    pair[0].sequence
                )));
            }
        }
        for path in &paths {
            if !request_ids.insert(path.request_id) {
                return Err(corruption(format!(
                    "duplicate catalog commit intent request id request_id=[{}]",
                    path.request_id
                )));
            }
        }
        let next_sequence = paths.last().map_or(Ok(1), |path| {
            path.sequence.checked_add(1).ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "catalog commit intent sequence exhausted u64",
                )
                .with_retryable(false)
            })
        })?;
        Ok((paths, next_sequence))
    }

    pub(crate) fn stage(
        &self,
        sequence: u64,
        command: VersionedCatalogCommand,
    ) -> Result<DurableCommitIntent> {
        let request_id = command_request_id(&command);
        if let Some(existing_path) = self.find_request(request_id)? {
            let existing = self.load(&existing_path)?;
            if existing.command == command {
                return Ok(existing);
            }
            return Err(ServerError::new(
                ErrorCode::Conflict,
                "catalog request id already has a different durable commit intent",
            )
            .with_request_id(request_id)
            .with_retryable(false));
        }
        let command_payload = CatalogCodec::encode_command(&command).map_err(|error| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                format!("encode catalog commit intent command error=[{error}]"),
            )
            .with_request_id(request_id)
            .with_retryable(false)
        })?;
        let command_value = serde_json::from_slice(&command_payload).map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("decode freshly encoded catalog command JSON error=[{error}]"),
            )
            .with_request_id(request_id)
        })?;
        let record = serde_json::to_vec(&StoredCommitIntentV1 {
            format_version: COMMIT_INTENT_RECORD_VERSION,
            sequence,
            command: command_value,
        })
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("encode catalog commit intent record error=[{error}]"),
            )
            .with_request_id(request_id)
        })?;
        let path = intent_path(sequence, request_id)?;
        let mut transaction = self
            .data_directory
            .begin_durable_file(&path.managed_path)
            .map_err(map_data_directory_error_for_intent)?;
        transaction.write_all(&record).map_err(|error| {
            io_error(
                "write catalog commit intent",
                transaction.temporary_path(),
                error,
            )
            .with_request_id(request_id)
        })?;
        match transaction.publish_noclobber() {
            Ok(_) => self.load(&path),
            Err(DataDirectoryError::AlreadyExists { .. }) => {
                let existing = self.load(&path)?;
                if existing.command != command {
                    return Err(ServerError::new(
                        ErrorCode::Conflict,
                        "catalog commit intent sequence already names a different command",
                    )
                    .with_request_id(request_id)
                    .with_retryable(false));
                }
                Ok(existing)
            }
            Err(error) => {
                Err(map_data_directory_error_for_intent(error).with_request_id(request_id))
            }
        }
    }

    pub(crate) fn load(&self, path: &CommitIntentPath) -> Result<DurableCommitIntent> {
        let absolute_path = self
            .data_directory
            .directory()
            .root()
            .join(path.managed_path.as_str());
        let mut file = open_intent_file(&absolute_path)?;
        let metadata = file
            .metadata()
            .map_err(|error| io_error("inspect catalog commit intent", &absolute_path, error))?;
        if metadata.len() > MAX_INTENT_RECORD_BYTES {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "catalog commit intent exceeds recovery byte limit actual=[{}] maximum=[{}]",
                    metadata.len(),
                    MAX_INTENT_RECORD_BYTES
                ),
            )
            .with_request_id(path.request_id)
            .with_retryable(false));
        }
        let capacity = usize::try_from(metadata.len()).map_err(|_| {
            terminal_record_limit(
                path.request_id,
                "catalog commit intent length exceeds addressable memory",
            )
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        file.read_to_end(&mut bytes)
            .map_err(|error| io_error("read catalog commit intent", &absolute_path, error))?;
        let stored = serde_json::from_slice::<StoredCommitIntentV1>(&bytes).map_err(|error| {
            corruption(format!(
                "decode catalog commit intent failed path=[{}] error=[{}]",
                absolute_path.display(),
                error
            ))
            .with_request_id(path.request_id)
        })?;
        if stored.format_version != COMMIT_INTENT_RECORD_VERSION || stored.sequence != path.sequence
        {
            return Err(corruption(format!(
                "catalog commit intent record identity mismatch path=[{}]",
                absolute_path.display()
            ))
            .with_request_id(path.request_id));
        }
        let command_payload = serde_json::to_vec(&stored.command).map_err(|error| {
            corruption(format!(
                "re-encode catalog commit intent command failed error=[{error}]"
            ))
            .with_request_id(path.request_id)
        })?;
        let command = CatalogCodec::decode_command(&command_payload).map_err(|error| {
            corruption(format!(
                "decode frozen catalog commit intent command failed error=[{error}]"
            ))
            .with_request_id(path.request_id)
        })?;
        validate_replayable_command(path, &command)?;
        Ok(DurableCommitIntent {
            path: path.clone(),
            command,
        })
    }

    pub(crate) fn verify_candidates(&self, intent: &DurableCommitIntent) -> Result<()> {
        let files = command_files(&intent.command);
        for file in files {
            verify_pending_candidate(&self.data_directory, file)
                .map_err(|error| error.with_request_id(intent.path.request_id))?;
        }
        Ok(())
    }

    pub(crate) fn complete(&self, path: &CommitIntentPath) -> Result<DurableRemoveOutcome> {
        self.data_directory
            .remove_durable_file(&path.managed_path)
            .map_err(|error| {
                map_data_directory_error_for_intent(error).with_request_id(path.request_id)
            })
    }

    fn find_request(&self, request_id: RequestId) -> Result<Option<CommitIntentPath>> {
        let directory = self
            .data_directory
            .directory()
            .root()
            .join(COMMIT_INTENT_DIRECTORY);
        let metadata = match fs::symlink_metadata(&directory) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(io_error(
                    "inspect catalog commit intent directory",
                    &directory,
                    error,
                ));
            }
        };
        validate_intent_directory(&directory, &metadata)?;
        for entry in fs::read_dir(&directory)
            .map_err(|error| io_error("scan catalog commit intents", &directory, error))?
        {
            let entry = entry
                .map_err(|error| io_error("read catalog commit intent entry", &directory, error))?;
            let file_name = entry
                .file_name()
                .into_string()
                .map_err(|_| corruption("catalog commit intent file name is not UTF-8"))?;
            if is_temporary_intent(&file_name) {
                continue;
            }
            let path = parse_intent_path(&file_name)?;
            if path.request_id == request_id {
                return Ok(Some(path));
            }
        }
        Ok(None)
    }
}

fn intent_path(sequence: u64, request_id: RequestId) -> Result<CommitIntentPath> {
    let file_name = format!("{sequence:020}-{request_id}.json");
    Ok(CommitIntentPath {
        sequence,
        request_id,
        managed_path: managed_path_for_file(&file_name)?,
    })
}

fn managed_path_for_file(file_name: &str) -> Result<ManagedDataPath> {
    ManagedDataPath::new(format!("{COMMIT_INTENT_DIRECTORY}/{file_name}")).map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("construct catalog commit intent path error=[{error}]"),
        )
    })
}

fn parse_intent_path(file_name: &str) -> Result<CommitIntentPath> {
    let (sequence, request) = file_name.split_once('-').ok_or_else(|| {
        corruption(format!(
            "invalid catalog commit intent file name name=[{file_name}]"
        ))
    })?;
    if sequence.len() != 20 || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(corruption(format!(
            "invalid catalog commit intent sequence name=[{file_name}]"
        )));
    }
    let sequence = sequence.parse::<u64>().map_err(|error| {
        corruption(format!(
            "parse catalog commit intent sequence failed name=[{file_name}] error=[{error}]"
        ))
    })?;
    let request = request.strip_suffix(".json").ok_or_else(|| {
        corruption(format!(
            "catalog commit intent file lacks json suffix name=[{file_name}]"
        ))
    })?;
    let request_id = RequestId::from_str(request).map_err(|error| {
        corruption(format!(
            "parse catalog commit intent request id failed name=[{file_name}] error=[{error}]"
        ))
    })?;
    intent_path(sequence, request_id)
}

fn is_temporary_intent(file_name: &str) -> bool {
    file_name.starts_with('.') && file_name.contains(".json.tmp-")
}

fn validate_replayable_command(
    path: &CommitIntentPath,
    command: &VersionedCatalogCommand,
) -> Result<()> {
    let VersionedCatalogCommand::V1(envelope) = command;
    if envelope.request_id != path.request_id {
        return Err(
            corruption("catalog commit intent request id does not match its file name")
                .with_request_id(path.request_id),
        );
    }
    if envelope.expected_revision.is_some() {
        return Err(corruption(
            "catalog data-file commit intent unexpectedly binds a global revision",
        )
        .with_request_id(path.request_id));
    }
    if envelope.actor.kind != CatalogActorKind::Server {
        return Err(corruption(
            "catalog data-file commit intent was not created by a server actor",
        )
        .with_request_id(path.request_id));
    }
    if !matches!(envelope.command, CatalogCommandV1::CommitDataFiles(_)) {
        return Err(
            corruption("catalog commit intent contains a non-persist command")
                .with_request_id(path.request_id),
        );
    }
    Ok(())
}

fn command_request_id(command: &VersionedCatalogCommand) -> RequestId {
    let VersionedCatalogCommand::V1(envelope) = command;
    envelope.request_id
}

pub(crate) fn command_files(
    command: &VersionedCatalogCommand,
) -> &[zippy_types::table::PendingDataFile] {
    let VersionedCatalogCommand::V1(envelope) = command;
    let CatalogCommandV1::CommitDataFiles(commit) = &envelope.command else {
        unreachable!("validated commit intent contains CommitDataFiles")
    };
    &commit.files
}

fn validate_intent_directory(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() {
        return Err(corruption(format!(
            "catalog commit intent directory is a symbolic link path=[{}]",
            path.display()
        )));
    }
    if !metadata.is_dir() {
        return Err(corruption(format!(
            "catalog commit intent path is not a directory path=[{}]",
            path.display()
        )));
    }
    validate_owner_security(path, metadata, false)
}

fn open_intent_file(path: &Path) -> Result<fs::File> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| io_error("inspect catalog commit intent", path, error))?;
    if metadata.file_type().is_symlink() {
        return Err(corruption(format!(
            "catalog commit intent is a symbolic link path=[{}]",
            path.display()
        )));
    }
    if !metadata.is_file() {
        return Err(corruption(format!(
            "catalog commit intent is not a regular file path=[{}]",
            path.display()
        )));
    }
    validate_owner_security(path, &metadata, true)?;
    open_intent_file_nofollow(path, &metadata)
}

#[cfg(unix)]
fn validate_owner_security(
    path: &Path,
    metadata: &fs::Metadata,
    require_one_link: bool,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let current_uid = rustix::process::geteuid().as_raw();
    let mode = metadata.permissions().mode() & 0o777;
    if metadata.uid() != current_uid
        || (require_one_link && metadata.nlink() != 1)
        || mode & 0o077 != 0
    {
        return Err(corruption(format!(
            "catalog commit intent ownership, links, or permissions are invalid path=[{}]",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_owner_security(
    _path: &Path,
    _metadata: &fs::Metadata,
    _require_one_link: bool,
) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn open_intent_file_nofollow(path: &Path, path_metadata: &fs::Metadata) -> Result<fs::File> {
    use std::os::unix::fs::MetadataExt;

    let descriptor = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| {
        io_error(
            "open catalog commit intent",
            path,
            std::io::Error::from(error),
        )
    })?;
    let file = fs::File::from(descriptor);
    let descriptor_metadata = file
        .metadata()
        .map_err(|error| io_error("inspect open catalog commit intent", path, error))?;
    if descriptor_metadata.dev() != path_metadata.dev()
        || descriptor_metadata.ino() != path_metadata.ino()
    {
        return Err(corruption(format!(
            "catalog commit intent identity changed during secure open path=[{}]",
            path.display()
        )));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_intent_file_nofollow(path: &Path, _path_metadata: &fs::Metadata) -> Result<fs::File> {
    fs::File::open(path).map_err(|error| io_error("open catalog commit intent", path, error))
}

fn map_data_directory_error_for_intent(error: DataDirectoryError) -> ServerError {
    let code = match &error {
        DataDirectoryError::OwnershipHeld { .. } | DataDirectoryError::AlreadyExists { .. } => {
            ErrorCode::Conflict
        }
        DataDirectoryError::Io { .. } => ErrorCode::Unavailable,
        DataDirectoryError::InvalidManagedPath { .. } => ErrorCode::Internal,
        DataDirectoryError::NotDirectory { .. }
        | DataDirectoryError::NotRegularFile { .. }
        | DataDirectoryError::SymbolicLink { .. }
        | DataDirectoryError::PathIdentityChanged { .. }
        | DataDirectoryError::UnversionedNonEmpty { .. }
        | DataDirectoryError::InvalidFormatVersion { .. }
        | DataDirectoryError::UnsupportedFormatVersion { .. } => ErrorCode::DataCorruption,
        #[cfg(unix)]
        DataDirectoryError::WrongOwner { .. }
        | DataDirectoryError::UnexpectedLinkCount { .. }
        | DataDirectoryError::InsecurePermissions { .. } => ErrorCode::DataCorruption,
        #[cfg(not(unix))]
        DataDirectoryError::UnsupportedPlatformSecurity => ErrorCode::Unavailable,
    };
    ServerError::new(
        code,
        format!("durable catalog commit intent operation failed error=[{error}]"),
    )
}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!(
            "{operation} failed path=[{}] error=[{}]",
            path.display(),
            error
        ),
    )
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message).with_retryable(false)
}

fn terminal_record_limit(request_id: RequestId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
        .with_request_id(request_id)
        .with_retryable(false)
}
