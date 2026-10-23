//! Command-parameter decoding shared by the statistics entry points.

use serde_json::Value;

pub fn text_param(params: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| params.get(*key).and_then(Value::as_str))
        .map(|value| value.trim().to_string())
}

pub fn number_param(params: &Value, key: &str) -> Option<u64> {
    params.get(key).and_then(|value| {
        value.as_u64().or_else(|| {
            value
                .as_str()
                .and_then(|text| text.trim().parse::<u64>().ok())
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decodes_aliases_and_numbers() {
        let params = json!({
            "agentId": " codex ",
            "limit": "25"
        });

        assert_eq!(text_param(&params, &["agentId"]), Some("codex".to_owned()));
        assert_eq!(number_param(&params, "limit"), Some(25));
    }
}
