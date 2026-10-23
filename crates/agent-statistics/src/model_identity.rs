//! Canonical catalog identities and actual historical execution variants.
//! Raw selectors remain lossless; only stable model evidence classifies history.

use crate::contract::{ModelTokenUsageSummary, UNATTRIBUTED_MODEL, UsageVariant};
use crate::registry::{RegistrySnapshot, model_display_name};
use crate::variant::{is_placeholder_id as is_unattributed_selector, model_selection};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
struct ModelProjection {
    display_name: String,
    is_canonical: bool,
    total: ModelTokenUsageSummary,
    variants: BTreeMap<String, ModelTokenUsageSummary>,
    unattributed: ModelTokenUsageSummary,
}

impl ModelProjection {
    fn from_published(value: &Value) -> Self {
        let total = ModelTokenUsageSummary::from_json(value);
        let variants = value
            .get("variants")
            .and_then(Value::as_object)
            .map(|variants| {
                variants
                    .iter()
                    .map(|(label, usage)| (label.clone(), ModelTokenUsageSummary::from_json(usage)))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let mut represented = ModelTokenUsageSummary::default();
        for usage in variants.values() {
            represented.merge(*usage);
        }
        Self {
            display_name: value
                .get("displayName")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            is_canonical: value
                .get("isCanonical")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            total,
            variants,
            unattributed: value
                .get("unattributedVariantUsage")
                .map(ModelTokenUsageSummary::from_json)
                .unwrap_or_else(|| total.saturating_sub(represented)),
        }
    }

    fn merge(&mut self, other: Self) {
        self.display_name = other.display_name;
        self.is_canonical |= other.is_canonical;
        self.total.merge(other.total);
        self.unattributed.merge(other.unattributed);
        for (label, usage) in other.variants {
            self.variants.entry(label).or_default().merge(usage);
        }
    }

    fn to_json(&self) -> Value {
        let mut value = self.total.to_json();
        value["displayName"] = json!(self.display_name);
        value["isCanonical"] = json!(self.is_canonical);
        value["variants"] = json!(
            self.variants
                .iter()
                .map(|(label, usage)| (label.clone(), usage.to_json()))
                .collect::<BTreeMap<_, _>>()
        );
        value["unattributedVariantUsage"] = self.unattributed.to_json();
        value
    }
}

pub type RawModelUsage = BTreeMap<(String, UsageVariant), ModelTokenUsageSummary>;

pub fn project_model_usage(
    raw: &RawModelUsage,
    source_agent: Option<&str>,
    registry: &RegistrySnapshot,
) -> BTreeMap<String, Value> {
    let mut models = BTreeMap::<String, ModelProjection>::new();
    for ((raw_model, observed_variant), usage) in raw {
        let selection = model_selection(raw_model);
        let placeholder = is_unattributed_selector(&selection.id);
        let resolved = registry
            .resolve_historical(
                &selection.id,
                selection.provider_id.as_deref(),
                source_agent,
            )
            // A literal placeholder cannot establish which concrete model a
            // floating route selected, even if a catalog short name matches.
            .filter(|model| !placeholder || model.id == selection.id);
        let (id, display_name) = match resolved {
            Some(model) => (model.id.clone(), model.display_name.clone()),
            None if is_unattributed_selector(&selection.id)
                || (selection.provider_id.is_none()
                    && (registry.is_provider_label(&selection.id)
                        || source_agent
                            .is_some_and(|agent| selection.id.eq_ignore_ascii_case(agent)))) =>
            {
                (UNATTRIBUTED_MODEL.to_owned(), UNATTRIBUTED_MODEL.to_owned())
            }
            None => (
                selection.unresolved_identity(),
                model_display_name(&selection.id),
            ),
        };
        // Only a unique registry match admits model suffixes as execution
        // variants. Unknown `research-high` remains a separate model.
        let (stem, candidate_variant) = terminal_variant(&selection.id);
        let suffix_variant = if resolved.is_some_and(|model| {
            registry
                .resolve_historical(&stem, selection.provider_id.as_deref(), source_agent)
                .is_some_and(|base| base.id == model.id)
        }) {
            candidate_variant
        } else {
            UsageVariant::default()
        };
        let variant = observed_variant
            .with_fallback(&selection.variant)
            .with_fallback(&suffix_variant);
        let model = models.entry(id).or_default();
        model.display_name = display_name;
        model.is_canonical |= resolved.is_some();
        model.total.merge(*usage);
        if let Some(label) = variant.label() {
            model.variants.entry(label).or_default().merge(*usage);
        } else {
            model.unattributed.merge(*usage);
        }
    }
    models
        .into_iter()
        .map(|(id, model)| (id, model.to_json()))
        .collect()
}

fn terminal_variant(raw: &str) -> (String, UsageVariant) {
    let normalized = raw
        .to_ascii_lowercase()
        .replace(['-', '_', '(', ')', '[', ']'], " ");
    let mut words = normalized.split_whitespace().collect::<Vec<_>>();
    let mut variant = UsageVariant::default();
    loop {
        if words.last() == Some(&"fast") {
            words.pop();
            variant.fast = Some(true);
        } else if words.len() >= 2 && words[words.len() - 2..] == ["extra", "high"] {
            words.truncate(words.len() - 2);
            variant.effort = Some("xhigh".to_owned());
        } else if let Some(effort) = words.last().filter(|word| {
            matches!(
                **word,
                "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" | "ultra"
            )
        }) {
            variant.effort = Some((*effort).to_owned());
            words.pop();
        } else {
            break;
        }
    }
    (words.join(" "), variant)
}

pub fn raw_model_usage(source: &Value) -> RawModelUsage {
    let mut raw = RawModelUsage::new();
    if let Some(rows) = source.get("rawModelUsage").and_then(Value::as_array) {
        for row in rows {
            if let Some(model) = row.get("model").and_then(Value::as_str) {
                raw.entry((model.to_owned(), UsageVariant::from_metadata(row)))
                    .or_default()
                    .merge(ModelTokenUsageSummary::from_json(row));
            }
        }
        return raw;
    }
    if let Some(models) = source.get("modelTokenUsage").and_then(Value::as_object) {
        for (model, value) in models {
            let mut represented = ModelTokenUsageSummary::default();
            if let Some(variants) = value.get("variants").and_then(Value::as_object) {
                for (label, counters) in variants {
                    let totals = ModelTokenUsageSummary::from_json(counters);
                    represented.merge(totals);
                    raw.entry((model.clone(), UsageVariant::from_label(label)))
                        .or_default()
                        .merge(totals);
                }
            }
            let residual = ModelTokenUsageSummary::from_json(value).saturating_sub(represented);
            if residual.has_usage() {
                raw.entry((model.clone(), UsageVariant::default()))
                    .or_default()
                    .merge(residual);
            }
        }
    } else if let Some(models) = source.get("modelUsage").and_then(Value::as_object) {
        for (model, total) in models {
            raw.entry((model.clone(), UsageVariant::default()))
                .or_default()
                .merge(ModelTokenUsageSummary {
                    total_tokens: total.as_u64().unwrap_or(0),
                    ..Default::default()
                });
        }
    }
    raw
}

pub fn raw_usage_json(raw: &RawModelUsage) -> Value {
    json!(
        raw.iter()
            .map(|((model, variant), totals)| {
                let mut value = totals.to_json();
                value["model"] = json!(model);
                value["effort"] = json!(variant.effort);
                value["fast"] = json!(variant.fast);
                value
            })
            .collect::<Vec<_>>()
    )
}

pub fn normalize_retained_report(report: &mut Value, registry: &RegistrySnapshot) {
    let preserve_attribution = report
        .get("modelRegistryRevision")
        .and_then(Value::as_str)
        .is_some_and(|revision| !revision.is_empty());
    if let Some(agents) = report.get_mut("agents").and_then(Value::as_array_mut) {
        for agent in agents {
            let source_agent = agent
                .get("agentId")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(history) = agent.get_mut("history") {
                normalize_bucket(
                    history,
                    source_agent.as_deref(),
                    registry,
                    preserve_attribution,
                );
                if let Some(days) = history.get_mut("dailyUsage").and_then(Value::as_array_mut) {
                    for day in days {
                        normalize_bucket(
                            day,
                            source_agent.as_deref(),
                            registry,
                            preserve_attribution,
                        );
                    }
                }
            }
        }
    }
    report["modelRegistryRevision"] = json!(registry.revision());
}

fn normalize_bucket(
    bucket: &mut Value,
    source_agent: Option<&str>,
    registry: &RegistrySnapshot,
    preserve_attribution: bool,
) {
    let raw = raw_model_usage(bucket);
    if raw.is_empty() {
        return;
    }
    let models = if preserve_attribution {
        preserve_published_models(bucket, &raw, source_agent, registry)
            .unwrap_or_else(|| project_model_usage(&raw, source_agent, registry))
    } else {
        project_model_usage(&raw, source_agent, registry)
    };
    bucket["modelUsage"] = json!(
        models
            .iter()
            .map(|(name, usage)| (name.clone(), usage["totalTokens"].clone()))
            .collect::<BTreeMap<_, _>>()
    );
    bucket["modelTokenUsage"] = json!(models);
    if bucket.get("rawModelUsage").is_none() {
        bucket["rawModelUsage"] = raw_usage_json(&raw);
    }
}

fn preserve_published_models(
    bucket: &Value,
    raw: &RawModelUsage,
    source_agent: Option<&str>,
    registry: &RegistrySnapshot,
) -> Option<BTreeMap<String, Value>> {
    let existing = bucket.get("modelTokenUsage")?.as_object()?;
    if existing.is_empty() {
        return None;
    }
    let has_raw = bucket.get("rawModelUsage").is_some();
    let corrected_ids = existing
        .keys()
        .filter_map(|id| registry.canonical_model(id).filter(|model| model.id != *id))
        .map(|model| model.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut corrected_totals = BTreeMap::<&str, ModelTokenUsageSummary>::new();
    for (id, value) in existing {
        if let Some(model) = registry.canonical_model(id)
            && corrected_ids.contains(model.id.as_str())
        {
            corrected_totals
                .entry(model.id.as_str())
                .or_default()
                .merge(ModelTokenUsageSummary::from_json(value));
        }
    }
    let mut corrected_raw = BTreeMap::<&str, RawModelUsage>::new();
    let mut raw_by_identity = BTreeMap::<String, RawModelUsage>::new();
    let mut observed_identities = BTreeSet::new();
    for (key, usage) in raw {
        let selection = model_selection(&key.0);
        if !corrected_ids.is_empty()
            && let Some(model) = registry.resolve_historical(
                &selection.id,
                selection.provider_id.as_deref(),
                source_agent,
            )
            && corrected_ids.contains(model.id.as_str())
        {
            corrected_raw
                .entry(model.id.as_str())
                .or_default()
                .insert(key.clone(), *usage);
        }
        let identity = selection.unresolved_identity();
        observed_identities.insert(identity.clone());
        // The previous serializer omitted the serving provider when a raw ID
        // already contained '/'. That old unknown label is not proof of a
        // canonical assignment merely because the corrected key is qualified.
        if selection.id.contains('/') {
            observed_identities.insert(selection.id);
        }
        raw_by_identity
            .entry(identity)
            .or_default()
            .insert(key.clone(), *usage);
    }
    let mut models = BTreeMap::<String, ModelProjection>::new();
    // Reproject each corrected identity as a whole when its retained raw rows
    // account for the published totals. This preserves explicit request
    // controls even when several former identities used different raw aliases.
    for (id, selected) in corrected_raw {
        let mut total = ModelTokenUsageSummary::default();
        for usage in selected.values() {
            total.merge(*usage);
        }
        if corrected_totals.get(id) == Some(&total) {
            for (id, value) in project_model_usage(&selected, source_agent, registry) {
                models.insert(id, ModelProjection::from_published(&value));
            }
        }
    }
    let reprojected_ids = models.keys().cloned().collect::<BTreeSet<_>>();
    for (id, value) in existing {
        let selection = model_selection(id);
        let current = registry.canonical_model(id);
        if current.is_some_and(|model| reprojected_ids.contains(&model.id)) {
            continue;
        }
        let is_model_id =
            selection.id == *id && (current.is_some() || !is_unattributed_selector(&selection.id));
        // Before the explicit marker existed, a published key different from
        // every recorded selector is evidence that an earlier registry had
        // already assigned a canonical identity. A removed catalog entry must
        // not turn that historical assignment back into a floating selector.
        let canonical = is_model_id
            && value
                .get("isCanonical")
                .and_then(Value::as_bool)
                .unwrap_or_else(|| {
                    current.is_some() || (has_raw && !observed_identities.contains(id))
                });
        if (canonical && current.is_none_or(|model| model.id == *id)) || id == UNATTRIBUTED_MODEL {
            let mut model = ModelProjection::from_published(value);
            model.is_canonical = canonical;
            model.display_name = current
                .map(|model| model.display_name.clone())
                .unwrap_or_else(|| {
                    if model.display_name.is_empty() {
                        model_display_name(id)
                    } else {
                        model_display_name(&model.display_name)
                    }
                });
            models.entry(id.clone()).or_default().merge(model);
            continue;
        }
        let selected = raw_by_identity
            .remove(id)
            .filter(|selected| {
                let mut total = ModelTokenUsageSummary::default();
                for usage in selected.values() {
                    total.merge(*usage);
                }
                total == ModelTokenUsageSummary::from_json(value)
            })
            .unwrap_or_else(|| raw_model_usage(&json!({"modelTokenUsage": {id: value}})));
        for (resolved_id, projection) in project_model_usage(&selected, source_agent, registry) {
            models
                .entry(resolved_id)
                .or_default()
                .merge(ModelProjection::from_published(&projection));
        }
    }
    Some(
        models
            .into_iter()
            .map(|(id, model)| (id, model.to_json()))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn catalog() -> RegistrySnapshot {
        RegistrySnapshot::from_catalog(json!({
            "models": {
                "moonshotai/kimi-k3":{"name":"Kimi K3"},
                "moonshotai/kimi-k2.7-code":{"name":"Kimi K2.7 Code"},
                "xai/grok-4.6":{"name":"Grok 4.6"},
                "xai/grok-4.7":{"name":"Grok 4.7"},
                "deepseek/flash":{"name":"DeepSeek Flash"},
                "deepseek/flash-vision":{"name":"DeepSeek Flash Vision"}
            },
            "providers":{"kimi-for-coding":{"name":"Kimi Code","models":{
                "k3":{"base_model":"moonshotai/kimi-k3"},
                "k3-256k":{"name":"Kimi Code K3 256K", "base_model":"moonshotai/kimi-k3"},
                "kimi-for-coding":{"base_model":"moonshotai/kimi-k2.7-code"}
            }}}
        }))
        .unwrap()
    }
    fn count(total: u64) -> ModelTokenUsageSummary {
        ModelTokenUsageSummary {
            total_tokens: total,
            prompt_tokens: total,
            request_count: 1,
            ..Default::default()
        }
    }
    #[test]
    fn composer_usage_merges_speed_tiers_and_preserves_observed_controls() {
        let raw = [
            ("composer-2.5", UsageVariant::default(), 10),
            ("composer-2.5-fast", UsageVariant::default(), 20),
            ("Composer 2.5 (Fast)", UsageVariant::default(), 30),
            (
                r#"{"id":"composer-2.5-fast","providerID":"cursor","variant":"high","fast":false}"#,
                UsageVariant::default(),
                40,
            ),
            (
                "composer-2.5-fast",
                UsageVariant {
                    effort: Some("xhigh".to_owned()),
                    fast: Some(false),
                },
                50,
            ),
        ]
        .into_iter()
        .map(|(model, variant, total)| ((model.to_owned(), variant), count(total)))
        .collect();
        for agent in ["cursor", "opencode", "codex"] {
            let models = project_model_usage(&raw, Some(agent), &RegistrySnapshot::empty());
            assert_eq!(models.len(), 1);
            let model = &models["cursor/composer-2.5"];
            assert_eq!(model["displayName"], "Composer 2.5");
            assert_eq!(model["isCanonical"], true);
            assert_eq!(model["totalTokens"], 150);
            assert_eq!(model["requestCount"], 5);
            assert_eq!(model["variants"]["Fast"]["totalTokens"], 50);
            assert_eq!(model["variants"]["High"]["totalTokens"], 40);
            assert_eq!(model["variants"]["Extra High"]["totalTokens"], 50);
            assert_eq!(model["unattributedVariantUsage"]["totalTokens"], 10);
        }
    }

    #[test]
    fn retained_composer_speed_identity_is_corrected_without_losing_usage() {
        let standard = ModelTokenUsageSummary {
            total_tokens: 30,
            prompt_tokens: 20,
            cached_input_tokens: 5,
            completion_tokens: 10,
            request_count: 1,
            token_unavailable_requests: 1,
            ..Default::default()
        };
        let fast = ModelTokenUsageSummary {
            total_tokens: 70,
            prompt_tokens: 50,
            cached_input_tokens: 15,
            completion_tokens: 20,
            request_count: 2,
            token_unavailable_requests: 1,
            ..Default::default()
        };
        let raw = RawModelUsage::from([
            (
                ("composer-2.5".to_owned(), UsageVariant::default()),
                standard,
            ),
            (
                ("composer-2.5-fast".to_owned(), UsageVariant::default()),
                fast,
            ),
        ]);
        let mut published = serde_json::Map::new();
        for (id, usage) in [
            ("cursor/composer-2.5", standard),
            ("cursor/composer-2.5-fast", fast),
        ] {
            let mut value = usage.to_json();
            value["isCanonical"] = json!(true);
            value["displayName"] = json!(model_display_name(id));
            published.insert(id.to_owned(), value);
        }
        let bucket = json!({"rawModelUsage":raw_usage_json(&raw),"modelTokenUsage":published});
        let mut report = json!({"modelRegistryRevision":"earlier-catalog","agents":[{
            "agentId":"cursor", "history":{
                "rawModelUsage":bucket["rawModelUsage"],
                "modelTokenUsage":bucket["modelTokenUsage"],
                "dailyUsage":[bucket]
            }
        }]});
        normalize_retained_report(&mut report, &RegistrySnapshot::empty());
        let history = &report["agents"][0]["history"];
        let mut expected = standard;
        expected.merge(fast);
        for bucket in [history, &history["dailyUsage"][0]] {
            assert_eq!(bucket["rawModelUsage"], raw_usage_json(&raw));
            assert_eq!(bucket["modelTokenUsage"].as_object().unwrap().len(), 1);
            let model = &bucket["modelTokenUsage"]["cursor/composer-2.5"];
            assert_eq!(ModelTokenUsageSummary::from_json(model), expected);
            assert_eq!(
                ModelTokenUsageSummary::from_json(&model["variants"]["Fast"]),
                fast
            );
            assert_eq!(
                ModelTokenUsageSummary::from_json(&model["unattributedVariantUsage"]),
                standard
            );
        }
        let normalized = report.clone();
        normalize_retained_report(&mut report, &RegistrySnapshot::empty());
        assert_eq!(report, normalized);

        // An old canonical Fast label must not override a retained request's
        // explicit speed control, including when its raw selector was short.
        let mut report = json!({"modelRegistryRevision":"earlier-catalog","agents":[{
            "agentId":"cursor", "history":bucket
        }]});
        report["agents"][0]["history"]["rawModelUsage"][1]["fast"] = json!(false);
        normalize_retained_report(&mut report, &RegistrySnapshot::empty());
        let model = &report["agents"][0]["history"]["modelTokenUsage"]["cursor/composer-2.5"];
        assert_eq!(ModelTokenUsageSummary::from_json(model), expected);
        assert!(model["variants"].as_object().unwrap().is_empty());
        assert_eq!(
            ModelTokenUsageSummary::from_json(&model["unattributedVariantUsage"]),
            expected
        );
    }

    #[test]
    fn registry_merges_only_admitted_aliases_and_preserves_actual_request_options() {
        let rows = [
            ("k3", UsageVariant::default(), 10),
            (
                "kimi-code-k3-256k",
                UsageVariant {
                    effort: Some("high".into()),
                    fast: None,
                },
                20,
            ),
            (
                r#"{"id":"k3","providerID":"kimi-for-coding","variant":"max"}"#,
                UsageVariant::default(),
                30,
            ),
            (
                "kimi-for-coding/kimi-for-coding",
                UsageVariant::default(),
                40,
            ),
            ("research-high", UsageVariant::default(), 50),
        ]
        .into_iter()
        .map(|(model, variant, total)| ((model.to_owned(), variant), count(total)))
        .collect();
        let models = project_model_usage(&rows, Some("kimi-code"), &catalog());
        assert_eq!(models["moonshotai/kimi-k3"]["totalTokens"], 60);
        assert_eq!(
            models["moonshotai/kimi-k3"]["variants"]["High"]["totalTokens"],
            20
        );
        assert_eq!(
            models["moonshotai/kimi-k3"]["variants"]["Max"]["totalTokens"],
            30
        );
        assert_eq!(
            models["moonshotai/kimi-k3"]["unattributedVariantUsage"]["totalTokens"],
            10
        );
        assert_eq!(models["kimi-for-coding/kimi-for-coding"]["totalTokens"], 40);
        assert_eq!(
            models["kimi-for-coding/kimi-for-coding"]["isCanonical"],
            false
        );
        assert!(
            models["research-high"]["variants"]
                .as_object()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            models
                .values()
                .map(|value| value["totalTokens"].as_u64().unwrap())
                .sum::<u64>(),
            150
        );
    }
    #[test]
    fn source_wrappers_merge_efforts_but_versions_and_vision_remain_distinct() {
        let rows = [
            ("Cursor Grok 4.6 High", 10),
            ("Cursor Grok 4.6 Extra High", 20),
            ("Cursor Grok 4.6 Extra High Fast", 30),
            ("Cursor", 40),
            ("grok-4.7", 50),
            ("deepseek/flash", 60),
            ("deepseek/flash-vision", 70),
        ]
        .into_iter()
        .map(|(model, total)| ((model.to_owned(), UsageVariant::default()), count(total)))
        .collect();
        let models = project_model_usage(&rows, Some("cursor"), &catalog());
        assert_eq!(models["xai/grok-4.6"]["totalTokens"], 60);
        assert_eq!(
            models["xai/grok-4.6"]["variants"]["Extra High Fast"]["totalTokens"],
            30
        );
        assert_eq!(models["Others"]["totalTokens"], 40);
        assert_eq!(models["xai/grok-4.7"]["totalTokens"], 50);
        assert_eq!(models["deepseek/flash"]["totalTokens"], 60);
        assert_eq!(models["deepseek/flash-vision"]["totalTokens"], 70);
    }

    #[test]
    fn placeholders_keep_usage_and_actual_options_without_becoming_models() {
        let mut raw = ["", " Default ", "AUTO", "unknown", "Unspecified", "Others"]
            .into_iter()
            .map(|id| ((id.to_owned(), UsageVariant::default()), count(10)))
            .collect::<RawModelUsage>();
        raw.insert(
            (
                json!({"id":"default","providerID":"synthetic-relay","variant":"high"}).to_string(),
                UsageVariant::default(),
            ),
            count(20),
        );
        raw.insert(
            ("research-high".to_owned(), UsageVariant::default()),
            count(30),
        );
        let models = project_model_usage(&raw, Some("cursor"), &catalog());
        assert_eq!(models.len(), 2);
        assert_eq!(models["Others"]["totalTokens"], 80);
        assert_eq!(models["Others"]["variants"]["High"]["totalTokens"], 20);
        assert_eq!(
            models["Others"]["unattributedVariantUsage"]["totalTokens"],
            60
        );
        assert_eq!(models["research-high"]["totalTokens"], 30);

        let registry = RegistrySnapshot::from_catalog(json!({
            "models":{"lab/default":{"name":"Named Model"}},
            "providers":{"relay":{"models":{"default":{"base_model":"lab/default"}}}}
        }))
        .unwrap();
        let qualified = [
            ("relay/default", 10),
            ("relay/Auto", 20),
            ("default", 30),
            ("lab/default", 40),
            ("research-default", 50),
            ("default-v2", 60),
        ]
        .into_iter()
        .map(|(id, total)| ((id.to_owned(), UsageVariant::default()), count(total)))
        .collect();
        let qualified = project_model_usage(&qualified, Some("opencode"), &registry);
        assert_eq!(qualified.len(), 4);
        assert_eq!(qualified["Others"]["totalTokens"], 60);
        assert_eq!(qualified["lab/default"]["totalTokens"], 40);
        assert_eq!(qualified["research-default"]["totalTokens"], 50);
        assert_eq!(qualified["default-v2"]["totalTokens"], 60);

        let mut report = json!({"modelRegistryRevision":"earlier-catalog","agents":[{"agentId":"cursor","history":{
            "rawModelUsage":raw_usage_json(&RawModelUsage::from([(("relay/default".to_owned(),UsageVariant::default()),count(10))])),
            "modelTokenUsage":{"relay/default":{"totalTokens":10,"promptTokens":10,"requestCount":1,"isCanonical":false,"displayName":"Default"}}
        }}]});
        let original_raw = report["agents"][0]["history"]["rawModelUsage"].clone();
        normalize_retained_report(&mut report, &catalog());
        let history = &report["agents"][0]["history"];
        assert_eq!(history["rawModelUsage"], original_raw);
        assert_eq!(history["modelTokenUsage"]["Others"]["totalTokens"], 10);
        assert!(history["modelTokenUsage"].get("relay/default").is_none());
    }

    #[test]
    fn complete_selectors_resolve_but_damaged_projection_keys_never_pin_an_identity() {
        let registry = RegistrySnapshot::from_catalog(json!({"models":{
            "anthropic/claude-opus-4.6":{"name":"Claude Opus 4.6"},
            "anthropic/claude-opus-4.7":{"name":"Claude Opus 4.7"}
        },"providers":{}}))
        .unwrap();
        let complete = json!({"id":"anthropic/claude-opus-4.6","providerID":"synthetic-relay","variant":"high"}).to_string();
        let damaged = "claude-opus-4.6\" ,\n  \"providerid\" : \"synthetic-relay\",\n  \"variant\" : \"max\"\n}";
        let raw = RawModelUsage::from([
            ((complete.clone(), UsageVariant::default()), count(30)),
            ((damaged.to_owned(), UsageVariant::default()), count(20)),
            (("default".to_owned(), UsageVariant::default()), count(10)),
        ]);
        let fresh = project_model_usage(&raw, Some("opencode"), &registry);
        assert_eq!(fresh.len(), 2);
        assert_eq!(
            fresh["anthropic/claude-opus-4.6"]["displayName"],
            "Claude Opus 4.6"
        );
        assert_eq!(
            fresh["anthropic/claude-opus-4.6"]["variants"]["High"]["totalTokens"],
            30
        );
        assert_eq!(fresh["Others"]["totalTokens"], 30);
        assert!(fresh["Others"]["variants"].as_object().unwrap().is_empty());

        for marker in [Some(false), Some(true), None] {
            let mut published = serde_json::Map::new();
            for ((id, _), usage) in &raw {
                let mut value = usage.to_json();
                value["displayName"] = json!(id);
                if let Some(marker) = marker {
                    value["isCanonical"] = json!(marker);
                }
                published.insert(id.clone(), value);
            }
            // This assignment is an actual earlier canonical identity and
            // must survive even when its catalog entry has since disappeared.
            let mut earlier = count(40).to_json();
            earlier["isCanonical"] = json!(true);
            earlier["displayName"] = json!("Claude Opus 4.5");
            earlier["variants"] = json!({"Max":count(40).to_json()});
            published.insert("anthropic/claude-opus-4.5".to_owned(), earlier);
            let mut retained_raw = raw.clone();
            retained_raw.insert(
                ("earlier-private-slot".to_owned(), UsageVariant::default()),
                count(40),
            );
            let original_raw = raw_usage_json(&retained_raw);
            let mut report = json!({"modelRegistryRevision":"earlier-catalog","agents":[{"agentId":"opencode","history":{
                "rawModelUsage":original_raw,"modelTokenUsage":published
            }}]});
            normalize_retained_report(&mut report, &registry);
            let history = &report["agents"][0]["history"];
            let models = history["modelTokenUsage"].as_object().unwrap();
            assert_eq!(models.len(), 3);
            assert_eq!(models["anthropic/claude-opus-4.6"]["totalTokens"], 30);
            assert_eq!(models["Others"]["totalTokens"], 30);
            assert_eq!(
                models["anthropic/claude-opus-4.5"]["variants"]["Max"]["totalTokens"],
                40
            );
            assert_eq!(
                models
                    .values()
                    .map(|value| value["totalTokens"].as_u64().unwrap())
                    .sum::<u64>(),
                100
            );
            assert_eq!(history["rawModelUsage"], original_raw);
            let normalized = report.clone();
            normalize_retained_report(&mut report, &registry);
            assert_eq!(report, normalized);
        }
    }

    #[test]
    fn retained_raw_identity_can_reproject_after_registry_refresh_without_losing_residual() {
        let mut report = json!({"usageParserRevision":"old-parser", "agents":[{"agentId":"kimi-code","history":{"dailyUsage":[{
            "date":"2026-07-01", "modelTokenUsage":{"k3":{"totalTokens":100,"requestCount":4,"variants":{"High":{"totalTokens":30,"requestCount":1},"Unspecified":{"totalTokens":20,"requestCount":1}}}}
        }]}}]});
        normalize_retained_report(&mut report, &catalog());
        let usage = &report["agents"][0]["history"]["dailyUsage"][0];
        assert_eq!(
            usage["modelTokenUsage"]["moonshotai/kimi-k3"]["unattributedVariantUsage"]["totalTokens"],
            70
        );
        assert_eq!(
            usage["modelTokenUsage"]["moonshotai/kimi-k3"]["variants"]["High"]["totalTokens"],
            30
        );
        assert!(
            usage["modelTokenUsage"]["moonshotai/kimi-k3"]["variants"]
                .get("Unspecified")
                .is_none()
        );
        assert_eq!(report["usageParserRevision"], "old-parser");
        assert_eq!(
            raw_model_usage(usage)
                .values()
                .map(|usage| usage.total_tokens)
                .sum::<u64>(),
            100
        );
    }

    #[test]
    fn opaque_provider_selectors_keep_slash_ids_without_guessing_historical_routes() {
        let registry = RegistrySnapshot::from_catalog(json!({
            "models": {
                "lab/model-1":{"name":"First Model"},
                "lab/model-2":{"name":"Second Model"}
            },
            "providers": {
                "relay-a":{"models":{"lab/flash-latest":{"base_model":"lab/model-1"}}},
                "relay-b":{"models":{"lab/flash-latest":{"base_model":"lab/model-2"}}}
            }
        }))
        .unwrap();
        let raw = [
            (
                json!({"id":"lab/flash-latest","providerID":"relay-a","variant":"high"})
                    .to_string(),
                10,
            ),
            (
                json!({"id":"lab/flash-latest","providerID":"relay-b","variant":"max"}).to_string(),
                20,
            ),
            (
                json!({"id":"private/custom-model-v2","providerID":"relay-a"}).to_string(),
                30,
            ),
            (
                json!({"id":"private/custom-model-v2","providerID":"relay-b"}).to_string(),
                40,
            ),
        ]
        .into_iter()
        .map(|(model, total)| ((model, UsageVariant::default()), count(total)))
        .collect();
        let mut report = json!({"agents":[{"agentId":"opencode","history":{"rawModelUsage":raw_usage_json(&raw)}}]});
        normalize_retained_report(&mut report, &registry);
        let history = &report["agents"][0]["history"];
        let models = history["modelTokenUsage"].as_object().unwrap();
        assert_eq!(
            models["relay-a/lab/flash-latest"]["variants"]["High"]["totalTokens"],
            10
        );
        assert_eq!(
            models["relay-b/lab/flash-latest"]["variants"]["Max"]["totalTokens"],
            20
        );
        assert!(!models.contains_key("lab/model-1"));
        assert!(!models.contains_key("lab/model-2"));
        assert_eq!(
            registry
                .resolve_with_provider("lab/flash-latest", Some("relay-a"), Some("opencode"))
                .unwrap()
                .id,
            "lab/model-1"
        );
        assert_eq!(models["relay-a/private/custom-model-v2"]["totalTokens"], 30);
        assert_eq!(models["relay-b/private/custom-model-v2"]["totalTokens"], 40);
        assert_eq!(
            models["relay-a/private/custom-model-v2"]["displayName"],
            model_display_name("private/custom-model-v2")
        );
        assert_eq!(
            raw_usage_json(&raw_model_usage(history)),
            raw_usage_json(&raw)
        );
        assert_eq!(
            models
                .values()
                .map(|model| model["totalTokens"].as_u64().unwrap())
                .sum::<u64>(),
            100
        );
    }

    #[test]
    fn published_canonical_assignments_survive_catalog_reroutes_and_removal() {
        for keep_original_model in [true, false] {
            for marker in [Some(true), None] {
                let mut catalog = json!({
                    "models": {
                        "lab/model-v2":{"name":"Second Model"},
                        "lab/model-v3":{"name":"Newly Known Model"}
                    },
                    "providers":{"relay":{"models":{"private-slot":{"base_model":"lab/model-v2"}}}}
                });
                if keep_original_model {
                    catalog["models"]["lab/model-v1"] = json!({"name":"First Model Refreshed"});
                }
                let registry = RegistrySnapshot::from_catalog(catalog).unwrap();
                let selector = json!({"id":"private-slot","providerID":"relay"}).to_string();
                let raw = BTreeMap::from([
                    (
                        (
                            selector,
                            UsageVariant {
                                effort: Some("high".into()),
                                fast: Some(true),
                            },
                        ),
                        count(30),
                    ),
                    (("model-v3".to_owned(), UsageVariant::default()), count(20)),
                ]);
                let mut assigned = count(30).to_json();
                assigned["displayName"] = json!("lab/first-model");
                assigned["variants"] = json!({"High Fast":count(30).to_json()});
                assigned["unattributedVariantUsage"] = ModelTokenUsageSummary::default().to_json();
                if let Some(marker) = marker {
                    assigned["isCanonical"] = json!(marker);
                }
                let mut unknown = count(20).to_json();
                unknown["displayName"] = json!("Model V3");
                unknown["isCanonical"] = json!(false);
                let original_raw = raw_usage_json(&raw);
                let mut report = json!({"modelRegistryRevision":"earlier-catalog", "agents":[{"agentId":"opencode","history":{
                    "rawModelUsage":original_raw,
                    "modelTokenUsage":{"lab/model-v1":assigned,"model-v3":unknown}
                }}]});
                normalize_retained_report(&mut report, &registry);
                let history = &report["agents"][0]["history"];
                assert_eq!(history["rawModelUsage"], original_raw);
                assert_eq!(
                    history["modelTokenUsage"]["lab/model-v1"]["totalTokens"],
                    30
                );
                assert_eq!(
                    history["modelTokenUsage"]["lab/model-v1"]["variants"]["High Fast"]["totalTokens"],
                    30
                );
                assert_eq!(
                    history["modelTokenUsage"]["lab/model-v1"]["isCanonical"],
                    true
                );
                assert_eq!(
                    history["modelTokenUsage"]["lab/model-v1"]["displayName"],
                    if keep_original_model {
                        "First Model Refreshed"
                    } else {
                        "First Model"
                    }
                );
                assert_eq!(
                    history["modelTokenUsage"]["lab/model-v3"]["totalTokens"],
                    20
                );
                assert!(history["modelTokenUsage"].get("lab/model-v2").is_none());
                let normalized = report.clone();
                normalize_retained_report(&mut report, &registry);
                assert_eq!(report, normalized);
            }
        }
    }
}
