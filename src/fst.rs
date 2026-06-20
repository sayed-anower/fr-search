use fst::{IntoStreamer, Set, SetBuilder, Streamer};
use fst::automaton::Str;
use thiserror::Error;
use fst::Automaton;

// Structured, static error types for our Fst framework
#[derive(Error, Debug)]
pub enum FstError {
    #[error("Failed to build or parse the FST graph: {0}")]
    FstInternal(#[from] fst::Error),

    #[error("Failed to parse string from FST bytes: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
}

// A lightweight, high-performance wrapper around `fst::Set`
pub struct Fst {
    // We hold the raw byte vector to keep the internal Set view alive
    _bytes: Vec<u8>,
    set: Set<Vec<u8>>,
}

impl Fst {
    // Creates a new FST instance. Automatically deduplicates and sorts the input.
    pub fn new<I, S>(keywords: I) -> Result<Self, FstError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        // 1. Extract, sort, and deduplicate the input data safely
        let mut data: Vec<String> = keywords
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        
        data.sort_unstable();
        data.dedup();

        // 2. Build the FST into an in-memory byte buffer
        let mut bytes = Vec::new();
        let mut builder = SetBuilder::new(&mut bytes)?;
        
        for word in data {
            builder.insert(word)?;
        }
        builder.finish()?;

        // 3. Initialize the FST set with the built bytes
        let set = Set::new(bytes.clone())?;

        Ok(Fst { _bytes: bytes, set })
    }

    // Performs a lightning-fast autocomplete prefix search.
    // Returns a vector of matching strings, or an empty vector if no matches exist.
    pub fn search(&self, prefix: &str) -> Vec<String> {
        let automaton = Str::new(prefix).starts_with();
        let mut stream = self.set.search(automaton).into_stream();
        let mut results = Vec::new();

        while let Some(key) = stream.next() {
            // Because we only insert valid UTF-8 Strings, this is safe to unwrap or ignore
            if let Ok(key_str) = std::str::from_utf8(key) {
                results.push(key_str.to_string());
            }
        }

        results
    }
}