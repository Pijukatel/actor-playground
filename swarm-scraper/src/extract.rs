//! Configuration-driven extraction: a map of field name to CSS selector, applied to the document.

use std::collections::BTreeMap;

use anyhow::Context as _;
use crawlee::Document;
use crawlee::http::Element;
use serde_json::{Map, Value, json};

use crate::config::FieldSpec;

fn element_value(element: &Element<'_>, attr: &str) -> Value {
    match attr {
        "text" => Value::String(element.text().trim().to_owned()),
        "html" => Value::String(element.html()),
        "innerHtml" => Value::String(element.inner_html()),
        name => element
            .attr(name)
            .map(|value| Value::String(value.to_owned()))
            .unwrap_or(Value::Null),
    }
}

/// Builds the dataset item for one page. With no fields configured, the item carries the title.
pub fn extract(
    fields: &BTreeMap<String, FieldSpec>,
    doc: &Document,
    url: &str,
    status: u16,
) -> anyhow::Result<Value> {
    let mut item = Map::new();
    item.insert("url".to_owned(), json!(url));
    item.insert("statusCode".to_owned(), json!(status));

    if fields.is_empty() {
        item.insert(
            "title".to_owned(),
            doc.title().map(Value::String).unwrap_or(Value::Null),
        );
        return Ok(Value::Object(item));
    }

    for (name, spec) in fields {
        let (selector, attr, all) = spec.parts();
        let selection = doc
            .select(selector)
            .with_context(|| format!("invalid selector for field `{name}`: {selector}"))?;
        let value = if all {
            Value::Array(
                selection
                    .iter()
                    .map(|element| element_value(&element, attr))
                    .collect(),
            )
        } else {
            selection
                .first()
                .map(|element| element_value(&element, attr))
                .unwrap_or(Value::Null)
        };
        item.insert(name.clone(), value);
    }
    Ok(Value::Object(item))
}
