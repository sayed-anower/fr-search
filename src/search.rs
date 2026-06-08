use std::fs;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::schema::Value;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, Field, FAST, STORED, STRING, TEXT};
use tantivy::{Index, IndexReader, TantivyDocument};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

// --- Error Handling ---

#[derive(Error, Debug)]
pub enum FtsError {
    #[error("Tantivy internal error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("Failed to load FastText model: {0}")]
    FastText(String),
    #[error("Failed to parse query: {0}")]
    QueryParse(#[from] tantivy::query::QueryParserError),
    #[error("Indexing queue is offline or full")]
    QueueOffline,
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Tantivy open directory error: {0}")]
    OpenDirectory(#[from] tantivy::directory::error::OpenDirectoryError),
}

// --- Data Structures ---

#[derive(Clone, Debug)]
pub struct FtsConfig {
    pub index_path: String,
}

#[derive(Clone, Debug)]
pub struct Document {
    pub id: String,
    pub title: String,
    pub body: String,
    pub tags: Vec<String>,
    pub timestamp: u64,
}

pub enum IndexCommand {
    Add(Document),
    Delete(String),
}

#[derive(Clone)]
struct SchemaFields {
    id: Field,
    title: Field,
    body: Field,
    tags: Field,
    timestamp: Field,
}

// --- Main Engine ---

#[derive(Clone)]
pub struct Fts {
    index: Index,
    reader: IndexReader,
    fields: SchemaFields,
    command_sender: mpsc::Sender<IndexCommand>,
}

impl Fts {
    pub fn new(config: FtsConfig) -> Result<Self, FtsError> {
        let mut schema_builder = Schema::builder();
        
        let id = schema_builder.add_text_field("id", STRING | STORED);
        let title = schema_builder.add_text_field("title", TEXT);
        let body = schema_builder.add_text_field("body", TEXT);
        let tags = schema_builder.add_text_field("tags", TEXT);
        let timestamp = schema_builder.add_u64_field("timestamp", FAST);
        
        let schema = schema_builder.build();
        let fields = SchemaFields { id, title, body, tags, timestamp };

        fs::create_dir_all(&config.index_path)?;
        let mmap_directory = MmapDirectory::open(&config.index_path)?;
        let index = Index::open_or_create(mmap_directory, schema.clone())?;

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let (command_sender, command_receiver) = mpsc::channel(10_000);
        let index_clone = index.clone();
        let fields_clone = fields.clone();

        // Notice: FastText is completely gone from the worker
        tokio::spawn(async move {
            run_index_worker(index_clone, fields_clone, command_receiver).await;
        });

        Ok(Self {
            index,
            reader,
            fields,
            command_sender,
        })
    }

    pub async fn add_doc(&self, doc: Document) -> Result<(), FtsError> {
        self.command_sender
            .send(IndexCommand::Add(doc))
            .await
            .map_err(|_| FtsError::QueueOffline)
    }

    pub async fn del_doc(&self, doc_id: String) -> Result<(), FtsError> {
        self.command_sender
            .send(IndexCommand::Delete(doc_id))
            .await
            .map_err(|_| FtsError::QueueOffline)
    }

    pub async fn edit_doc(&self, doc_id: String, new_doc: Document) -> Result<(), FtsError> {
        self.del_doc(doc_id).await?;
        self.add_doc(new_doc).await?;
        Ok(())
    }

    pub fn search(&self, keyword: String, limit: usize, offset: usize) -> Result<Vec<String>, FtsError> {
        let searcher = self.reader.searcher();
        
        let query_parser = QueryParser::for_index(
            &self.index,
            vec![self.fields.title, self.fields.body, self.fields.tags],
        );

        let query = query_parser.parse_query(keyword)?;

        let top_docs_collector = TopDocs::with_limit(limit).and_offset(offset).order_by_u64_field("timestamp", tantivy::Order::Desc);

        let top_docs = searcher.search(&query, &top_docs_collector)?;

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

// --- Background Worker ---

async fn run_index_worker(
    index: Index,
    fields: SchemaFields,
    mut receiver: mpsc::Receiver<IndexCommand>,
) {
    let mut writer = match index.writer(50_000_000) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("CRITICAL: Failed to create index writer: {}", e);
            return;
        }
    };

    let mut uncommitted_changes = 0usize;
    let mut flush_interval = interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            Some(command) = receiver.recv() => {
                match command {
                    IndexCommand::Add(doc) => {
                        let mut document = TantivyDocument::new();
                        document.add_text(fields.id, &doc.id);
                        document.add_text(fields.title, &doc.title);
                        document.add_text(fields.body, &doc.body);
                        document.add_u64(fields.timestamp, doc.timestamp);

                        // Directly index the tags provided by the user
                        for tag in &doc.tags {
                            document.add_text(fields.tags, tag);
                        }

                        if let Err(e) = writer.add_document(document) {
                            eprintln!("Failed to add document {}: {}", doc.id, e);
                        } else {
                            uncommitted_changes += 1;
                        }
                    }
                    IndexCommand::Delete(doc_id) => {
                        let term = tantivy::Term::from_field_text(fields.id, &doc_id);
                        writer.delete_term(term);
                        uncommitted_changes += 1;
                    }
                }

                if uncommitted_changes >= 500 {
                    if let Err(e) = writer.commit() {
                        eprintln!("Failed to commit: {}", e);
                    }
                    uncommitted_changes = 0;
                }
            }
            _ = flush_interval.tick() => {
                if uncommitted_changes > 0 {
                    if let Err(e) = writer.commit() {
                        eprintln!("Failed to commit: {}", e);
                    }
                    uncommitted_changes = 0;
                }
            }
        }
    }
}
