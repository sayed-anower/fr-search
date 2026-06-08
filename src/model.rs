use fasttext::FastText;
use thiserror::Error;

// --- Error Handling ---
#[derive(Error, Debug)]
pub enum FastTextError {
    #[error("Failed to load FastText model: {0}")]
    FastText(String),
}

// --- External Tagger ---

pub struct Tagger {
    model: FastText,
}

impl Tagger {
    /// Loads the FastText model into memory. Call this once at startup.
    pub fn new(model_path: &str) -> Result<Self, FastTextError> {
        let mut model = FastText::new();
        model
            .load_model(model_path)
            .map_err(|e| FastTextError::FastText(e.to_string()))?;
        
        Ok(Self { model })
    }

    /// Takes a document body, cleans it, and returns up to 5 tags.
    /// The first tag is prefixed with "lang:", and subsequent ones are raw categories.
    pub fn generate_tags(&self, text: String) -> Vec<String> {
        let clean_text = text.replace('\n', " ").replace('\r', " ");
        if clean_text.trim().is_empty() {
            return Vec::new();
        }

        // 1. Request up to 5 predictions from FastText (k = 5, threshold = 0.0)
        if let Ok(predictions) = self.model.predict(&clean_text, 5, 0.0) {
            let mut formatted_tags = Vec::new();

            for (index, prediction) in predictions.iter().enumerate() {
                // Clean the default FastText "__label__" prefix
                let raw_label = prediction.label.replace("__label__", "");

                if index == 0 {
                    // Force the 1st tag to be the language tag format
                    formatted_tags.push(format!("lang:{}", raw_label));
                } else {
                    // The rest are treated as content/category tags
                    formatted_tags.push(raw_label);
                }
            }
            return formatted_tags;
        }

        Vec::new()
    }
}
