use std::fs;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::Value;
use tantivy::schema::{Schema, Field, STORED, STRING, TEXT};
use tantivy::{Index, IndexReader, TantivyDocument, ReloadPolicy};
use thiserror::Error;

// --- Error Handling ---
#[derive(Error, Debug)]
pub enum FtsError {
    #[error("Tantivy internal error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("Failed to parse query: {0}")]
    QueryParse(#[from] tantivy::query::QueryParserError),
    #[error("Indexing queue is offline or full")]
    QueueOffline,
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Tantivy open directory error: {0}")]
    OpenDirectory(#[from] tantivy::directory::error::OpenDirectoryError),
    #[error("Index writer error: {0}")]
    Writer(String),
}

// --- Data Structures ---
#[derive(Clone, Debug)]
pub struct FtsConfig {
    pub index_path: String,
}

#[derive(Clone, Debug)]
pub struct Content {
    pub id: String,
    pub content: String,
}

pub enum IndexCommand {
    Add(Content),
    Delete(String),
}

#[derive(Clone)]
struct SchemaFields {
    id: Field,
    content: Field,
}

// --- Main Engine ---
#[derive(Clone)]
pub struct Fts {
    index: Index,
    reader: IndexReader,
    fields: SchemaFields,
    command_sender: mpsc::SyncSender<IndexCommand>, // synchronous sender for blocking thread
}

impl Fts {
    pub fn new(config: FtsConfig) -> Result<Self, FtsError> {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_text_field("id", STRING | STORED);
        let content = schema_builder.add_text_field("content", TEXT);
        let schema = schema_builder.build();
        let fields = SchemaFields { id, content };

        fs::create_dir_all(&config.index_path)?;
        let mmap_directory = MmapDirectory::open(&config.index_path)?;
        let index = Index::open_or_create(mmap_directory, schema)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        // Create a bounded synchronous channel for commands.
        let (sync_tx, sync_rx) = mpsc::sync_channel(20_000); // capacity for high throughput
        
        // Cloned For Background Threads
        let index_clone = index.clone();
        let fields_clone = fields.clone();
        // Spawn a dedicated blocking thread for the index writer.
        thread::Builder::new()
            .name("index-writer".to_string())
            .spawn(move || {
                run_index_writer(index_clone, fields_clone, sync_rx);
            })
            .map_err(|e| FtsError::Writer(e.to_string()))?;

        Ok(Self {
            index,
            reader,
            fields,
            command_sender: sync_tx,
        })
    }

    // Public API: add a document (Content)
    pub async fn add_doc(&self, content: Content) -> Result<(), FtsError> {
        self.command_sender
            .send(IndexCommand::Add(content))
            .map_err(|_| FtsError::QueueOffline)
    }

    // Public API: delete by id
    pub async fn del_doc(&self, doc_id: String) -> Result<(), FtsError> {
        self.command_sender
            .send(IndexCommand::Delete(doc_id))
            .map_err(|_| FtsError::QueueOffline)
    }

    // Public API: edit (delete + add)
    pub async fn edit_doc(&self, doc_id: String, new_content: Content) -> Result<(), FtsError> {
        self.del_doc(doc_id).await?;
        self.add_doc(new_content).await?;
        Ok(())
    }

    // Search: returns list of matching ids, sorted by relevance (score).
    pub fn search(&self, keyword: &str, limit: usize, offset: usize) -> Result<Vec<String>, FtsError> {
        let searcher = self.reader.searcher();

        // Query only over the "content" field.
        let query_parser = QueryParser::for_index(&self.index, vec![self.fields.content]);
        let query = query_parser.parse_query(keyword)?;

        // Use TopDocs with offset and limit, sorted by relevance (default).
        let collector = TopDocs::with_limit(limit).and_offset(offset);
        let top_docs = searcher.search(&query, &collector)?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (_score, doc_address) in top_docs {
            if let Ok(retrieved_doc) = searcher.doc::<TantivyDocument>(doc_address) {
                if let Some(id_value) = retrieved_doc.get_first(self.fields.id) {
                    if let Some(id_str) = id_value.as_str() {
                        results.push(id_str.to_string());
                    }
                }
            }
        }
        Ok(results)
    }
}

// --- Blocking Index Writer (runs in its own thread) ---
fn run_index_writer(index: Index, fields: SchemaFields, receiver: mpsc::Receiver<IndexCommand>) {
    let mut writer = match index.writer(100_000_000) { // 100 MB budget for better throughput
        Ok(w) => w,
        Err(e) => {
            eprintln!("CRITICAL: Failed to create index writer: {}", e);
            return;
        }
    };

    let mut uncommitted = 0usize;
    const COMMIT_THRESHOLD: usize = 1_000;       // commit after 1000 changes
    const COMMIT_INTERVAL: Duration = Duration::from_secs(10);
    let mut last_commit = Instant::now();

    loop {
        // Wait for a command with a timeout to allow periodic commits even when idle.
        match receiver.recv_timeout(Duration::from_millis(500)) {
            Ok(command) => {
                match command {
                    IndexCommand::Add(content) => {
                        let mut doc = TantivyDocument::new();
                        doc.add_text(fields.id, &content.id);
                        doc.add_text(fields.content, &content.content);
                        if let Err(e) = writer.add_document(doc) {
                            eprintln!("Failed to add document {}: {}", content.id, e);
                        } else {
                            uncommitted += 1;
                        }
                    }
                    IndexCommand::Delete(doc_id) => {
                        let term = tantivy::Term::from_field_text(fields.id, &doc_id);
                        writer.delete_term(term);
                        uncommitted += 1;
                    }
                }

                // Commit if threshold reached.
                if uncommitted >= COMMIT_THRESHOLD {
                    if let Err(e) = writer.commit() {
                        eprintln!("Failed to commit: {}", e);
                    } else {
                        uncommitted = 0;
                        last_commit = Instant::now();
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // No command received, check if we need to commit due to time.
                if uncommitted > 0 && last_commit.elapsed() >= COMMIT_INTERVAL {
                    if let Err(e) = writer.commit() {
                        eprintln!("Failed to commit: {}", e);
                    } else {
                        uncommitted = 0;
                        last_commit = Instant::now();
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // Sender dropped, exit the thread.
                break;
            }
        }
    }

    // Final commit on shutdown.
    if uncommitted > 0 {
        let _ = writer.commit();
    }
}