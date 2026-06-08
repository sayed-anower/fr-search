pub mod fst;
pub mod search;
pub mod model;

pub mod prelude {
  pub use crate::fst::{
    Fst
  };
  pub use crate::search::{
    Fts,
    FtsConfig,
    Document,
  };
  pub use crate::model::{
    Tagger
  };
}