//! Exact Token increment authority. SQLite watermark/cursor already exist in
//! the native cache; this module is the typed increment and window projection
//! surface. Tokens come only from explicit usage metadata.

#[cfg(test)]
use crate::window::UsageWindow;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceId(String);

impl SourceId {
    pub fn synthetic(agent_id: &str, source_ordinal: u32) -> Self {
        Self(format!("{agent_id}:source-{source_ordinal}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCursor {
    pub source_id: SourceId,
    pub watermark: u64,
    pub coverage_start: String,
    pub coverage_end: String,
    pub parser_revision: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TokenDelta {
    pub prompt_tokens: u64,
    pub cached_input_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

impl TokenDelta {
    pub fn from_explicit(prompt: u64, cached: u64, completion: u64) -> Self {
        Self {
            prompt_tokens: prompt,
            cached_input_tokens: cached.min(prompt),
            completion_tokens: completion,
            total_tokens: prompt.saturating_add(completion),
        }
    }

    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            prompt_tokens: self.prompt_tokens.saturating_add(other.prompt_tokens),
            cached_input_tokens: self
                .cached_input_tokens
                .saturating_add(other.cached_input_tokens),
            completion_tokens: self
                .completion_tokens
                .saturating_add(other.completion_tokens),
            total_tokens: self.total_tokens.saturating_add(other.total_tokens),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementDecision {
    RecomputeChangedRange { from_watermark: u64 },
    ReplaceSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceChange {
    pub source_id: SourceId,
    pub observed_watermark: u64,
    pub parser_revision: String,
    pub day: String,
    pub model: String,
    pub delta: TokenDelta,
    pub cursor_valid: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DayModelRow {
    pub day: String,
    pub model: String,
    pub tokens: TokenDelta,
}

/// Indexed daily/model projection for one requested window. Immutable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsageWindowProjection {
    pub start: String,
    pub end: String,
    pub days: u64,
    pub rows: Vec<DayModelRow>,
    pub cumulative: TokenDelta,
}

impl UsageWindowProjection {
    #[cfg(test)]
    pub fn from_rows(window: &UsageWindow, rows: Vec<DayModelRow>) -> Self {
        let mut filtered = Vec::new();
        let mut cumulative = TokenDelta::default();
        for row in rows {
            if !window.contains(&row.day) {
                continue;
            }
            cumulative = cumulative.saturating_add(row.tokens);
            filtered.push(row);
        }
        Self {
            start: window.start().to_owned(),
            end: window.end().to_owned(),
            days: window.days(),
            rows: filtered,
            cumulative,
        }
    }
}

/// Incremental source registry. Valid cursors recompute only the changed
/// range; an invalid cursor atomically replaces that source. Never infers
/// tokens from text or price.
#[derive(Clone, Debug, Default)]
pub struct UsageIncrementalAuthority {
    cursors: BTreeMap<String, SourceCursor>,
    rows: BTreeMap<(String, String, String), TokenDelta>,
}

impl UsageIncrementalAuthority {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, change: SourceChange) -> IncrementDecision {
        let key = change.source_id.as_str().to_owned();
        let decision = match self.cursors.get(&key) {
            Some(existing)
                if change.cursor_valid
                    && existing.parser_revision == change.parser_revision
                    && change.observed_watermark >= existing.watermark =>
            {
                IncrementDecision::RecomputeChangedRange {
                    from_watermark: existing.watermark,
                }
            }
            _ => {
                self.rows.retain(|(source, _, _), _| source != &key);
                IncrementDecision::ReplaceSource
            }
        };
        self.rows.insert(
            (key.clone(), change.day.clone(), change.model.clone()),
            change.delta,
        );
        let coverage_start = self
            .cursors
            .get(&key)
            .map(|cursor| {
                if cursor.coverage_start <= change.day {
                    cursor.coverage_start.clone()
                } else {
                    change.day.clone()
                }
            })
            .unwrap_or_else(|| change.day.clone());
        self.cursors.insert(
            key,
            SourceCursor {
                source_id: change.source_id,
                watermark: change.observed_watermark,
                coverage_start,
                coverage_end: change.day,
                parser_revision: change.parser_revision,
            },
        );
        decision
    }

    #[cfg(test)]
    pub fn project_window(&self, window: &UsageWindow) -> UsageWindowProjection {
        let rows = self
            .rows
            .iter()
            .map(|((_, day, model), tokens)| DayModelRow {
                day: day.clone(),
                model: model.clone(),
                tokens: *tokens,
            })
            .collect();
        UsageWindowProjection::from_rows(window, rows)
    }

    pub fn cursor(&self, source_id: &SourceId) -> Option<&SourceCursor> {
        self.cursors.get(source_id.as_str())
    }

    /// Explicit metadata only. Missing numeric fields yield zero, never a
    /// character-count or price estimate.
    pub fn explicit_delta(value: &Value) -> Option<TokenDelta> {
        let prompt = explicit_u64(value, &["promptTokens", "prompt_tokens"])?;
        let completion = explicit_u64(value, &["completionTokens", "completion_tokens"])?;
        let cached =
            explicit_u64(value, &["cachedInputTokens", "cached_input_tokens"]).unwrap_or(0);
        Some(TokenDelta::from_explicit(prompt, cached, completion))
    }
}

fn explicit_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(field) = value.get(*key) {
            if let Some(number) = field.as_u64() {
                return Some(number);
            }
            if let Some(number) = field.as_i64().filter(|value| *value >= 0) {
                return Some(number as u64);
            }
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn window() -> UsageWindow {
        UsageWindow::from_params(&json!({
            "now": "2026-07-15T12:00:00Z",
            "historyDays": 7
        }))
    }

    #[test]
    fn valid_cursor_recomputes_only_the_changed_range() {
        let mut authority = UsageIncrementalAuthority::new();
        let source = SourceId::synthetic("codex", 1);
        let first = authority.apply(SourceChange {
            source_id: source.clone(),
            observed_watermark: 10,
            parser_revision: "v8".to_owned(),
            day: "2026-07-14".to_owned(),
            model: "model-a".to_owned(),
            delta: TokenDelta::from_explicit(10, 1, 5),
            cursor_valid: true,
        });
        assert!(matches!(first, IncrementDecision::ReplaceSource));
        let second = authority.apply(SourceChange {
            source_id: source,
            observed_watermark: 12,
            parser_revision: "v8".to_owned(),
            day: "2026-07-15".to_owned(),
            model: "model-a".to_owned(),
            delta: TokenDelta::from_explicit(4, 0, 2),
            cursor_valid: true,
        });
        assert_eq!(
            second,
            IncrementDecision::RecomputeChangedRange { from_watermark: 10 }
        );
        let projection = authority.project_window(&window());
        assert_eq!(projection.days, 7);
        assert_eq!(projection.cumulative.total_tokens, 21);
        assert_eq!(projection.rows.len(), 2);
    }

    #[test]
    fn invalid_cursor_atomically_replaces_that_source() {
        let mut authority = UsageIncrementalAuthority::new();
        let source = SourceId::synthetic("cursor", 1);
        authority.apply(SourceChange {
            source_id: source.clone(),
            observed_watermark: 3,
            parser_revision: "v8".to_owned(),
            day: "2026-07-10".to_owned(),
            model: "model-b".to_owned(),
            delta: TokenDelta::from_explicit(100, 0, 50),
            cursor_valid: true,
        });
        let replaced = authority.apply(SourceChange {
            source_id: source,
            observed_watermark: 1,
            parser_revision: "v8".to_owned(),
            day: "2026-07-15".to_owned(),
            model: "model-b".to_owned(),
            delta: TokenDelta::from_explicit(8, 0, 2),
            cursor_valid: false,
        });
        assert_eq!(replaced, IncrementDecision::ReplaceSource);
        let projection = authority.project_window(&window());
        assert_eq!(projection.cumulative.total_tokens, 10);
        assert_eq!(projection.rows.len(), 1);
    }

    #[test]
    fn text_and_price_are_not_token_sources() {
        assert!(
            UsageIncrementalAuthority::explicit_delta(&json!({
                "text": "hello world",
                "priceUsd": 0.02
            }))
            .is_none()
        );
        assert_eq!(
            UsageIncrementalAuthority::explicit_delta(&json!({
                "promptTokens": 3,
                "completionTokens": 4,
                "cachedInputTokens": 1
            }))
            .expect("explicit"),
            TokenDelta::from_explicit(3, 1, 4)
        );
    }
}
