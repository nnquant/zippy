/// 共享内存占位模块。
///
/// 当前任务只固定 active segment builder 与提交协议，真正的 shm 映射后续再补。
#[derive(Debug, Default, Clone)]
pub struct ShmRegion;
