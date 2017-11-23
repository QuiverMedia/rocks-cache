
use rocksdb::Error as RocksDbError;
use bincode::ErrorKind as BincodeError;
use std::io::Error as StdError;

#[derive(Debug, Fail, Clone)]
#[fail(display = "RocksDB Error: {}", description)]
pub struct RocksCacheError {
    pub description: String,
}

impl From<RocksDbError> for RocksCacheError {
    fn from(e: RocksDbError) -> Self {
        RocksCacheError { description: format!("RocksDB Error: {}", e) }
    }
}

impl From<Box<BincodeError>> for RocksCacheError {
    fn from(e: Box<BincodeError>) -> Self {
        RocksCacheError { description: format!("Serde Deserialization Bincode Error: {}", e) }
    }
}

impl From<StdError> for RocksCacheError {
    fn from(e: StdError) -> Self {
        RocksCacheError { description: format!("StdIo Error: {}", e) }
    }
}
