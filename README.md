# ⚡ FR-SEARCH

[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**FR-SEARCH** is a high-performance, ultra-optimized search engine and document tagging system built in Rust. By leveraging cutting-edge underlying libraries and Meta's FastText model, it delivers lightning-fast search suggestions and highly accurate automated document tagging with minimal resource overhead.

📖 **[Read the Documentation](https://github.com/sayed-anower/docs/blob/643f3b5582b7f1f593c0725012d8d0e5e8cb518a/docs/fr-search.md)**

---

## 🚀 Features

*   **Minimalist Syntax:** Designed for rapid development with drastically reduced boilerplate and zero performance compromises.
*   **Blazing Fast Performance:** Highly optimized core utilizing industry-standard frameworks like `tantivy`, `fst`, and `FastText` for peak efficiency.
*   **Intelligent Auto-Tagging:** Seamlessly categorize and tag documents using a compact, high-performance ML model—ideal for boosting search accuracy and improving feed management systems.
*   **Instant Search Suggestions:** Built-in support for low-latency typeahead and search recommendations out of the box.

---

## ⚠️ Architectural Considerations

*   **Opinionated Design:** To maximize developer velocity and provide an effortless learning curve, certain low-level configurations are abstracted by default. While this guarantees ease of use, advanced power users requiring granular control over the underlying engines may find it slightly restrictive.
