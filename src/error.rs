use std::error::Error as StdError;


#[derive(Debug, Clone)]
pub struct Error {
    pub description: String,
}

/* 
impl StdError for Error {
    fn description(&self) -> &str {
        self.description.as_str()
    }
}
*/

impl<'a, E: StdError + 'a> From<E> for Error {
    fn from(e: E) -> Error {
        Error { description: e.description().to_owned() }
    }
}

