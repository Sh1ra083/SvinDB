//! Elasticsearch NDJSON writes: preserve action order and report each result.

use crate::{esbulk, http, intake, nested, route, wire, Node, Writer};
use serde_json::{json, Value};

struct Pending {
    at: usize,
    id: String,
    envelope: Value,
    doc: nested::Doc,
    generated: bool,
}

fn item(verb: &str, index: Option<&str>, id: Option<&str>, status: u16, result: &str) -> Value {
    let mut detail = json!({"status": status, "result": result});
    if let Some(index) = index {
        detail["_index"] = json!(index);
    }
    if let Some(id) = id {
        detail["_id"] = json!(id);
    }
    json!({verb: detail})
}

fn failure(verb: &str, index: Option<&str>, id: Option<&str>, status: u16, why: &str) -> Value {
    let mut result = item(verb, index, id, status, "error");
    result[verb].as_object_mut().unwrap().remove("result");
    result[verb]["error"] = json!({
        "type": if status == 400 { "illegal_argument_exception" } else { "unavailable_shards_exception" },
        "reason": why,
    });
    result
}

fn prepare(
    action: &mut esbulk::Item,
    plan: &nested::Plan,
    strict: svindb_core::settings::Strict,
    at: usize,
) -> Result<(nested::Doc, Value), String> {
    let id = match action.id.as_deref() {
        Some("") => return Err("_id must not be empty".into()),
        Some(id) => id.to_string(),
        None => crate::new_document_id()?,
    };
    action.id = Some(id.clone());
    let source = action.body.take().ok_or("за index нет документа")?;
    if !source.is_object() {
        return Err("тело документа должно быть JSON-объектом".into());
    }
    let envelope = json!({"id": id, "_source": source});
    let mut report = intake::Report::default();
    let doc = plan.take(&envelope, at, &mut report)?;
    if !strict.lenient() && !report.clean() {
        return Err(report.refusal());
    }
    Ok((doc, envelope))
}

/// Commit one uninterrupted run of index actions with one WAL group commit.
fn flush(
    pending: &mut Vec<Pending>,
    replies: &mut [Value],
    writer: &Writer,
    node: &Node,
    index: Option<&str>,
    unavailable: &mut bool,
) -> bool {
    if pending.is_empty() {
        return false;
    }
    let batch = std::mem::take(pending);
    let append_only = batch.iter().all(|pending| pending.generated);
    let (positions, docs): (Vec<_>, Vec<_>) = batch
        .into_iter()
        .map(|pending| ((pending.at, pending.id, pending.envelope), pending.doc))
        .unzip();
    // Serialize the already prepared envelopes only for replication; the
    // primary indexes the parsed Docs directly and never reparses this body.
    let raw = node
        .repl
        .as_ref()
        .map(|_| {
            serde_json::to_vec(&json!({
                "documents": positions.iter().map(|(_, _, envelope)| envelope).collect::<Vec<_>>()
            }))
        })
        .transpose();
    let raw = match raw {
        Ok(raw) => raw,
        Err(error) => {
            for (at, id, _) in positions {
                replies[at] = failure("index", index, Some(&id), 503, &error.to_string());
            }
            *unavailable = true;
            return false;
        }
    };
    let outcome = if append_only {
        writer.add(docs).map(|added| Ok((added, 0)))
    } else {
        writer.put_if(docs, &[])
    };
    let (status, why, accepted) = match outcome {
        Ok(Ok((added, _))) => {
            node.metrics.added(added as u64);
            match crate::replicate_effect(node, "put", raw.as_deref(), &[]) {
                Some(ack) if !ack.enough() => {
                    (503, Some("недостаточно подтверждений репликации".to_string()), true)
                }
                _ => (200, None, true),
            }
        }
        Ok(Err(_)) => (503, Some("bulk-upsert получил конфликт без условий".into()), false),
        Err(error) => (503, Some(error), false),
    };
    if status == 503 {
        *unavailable = true;
    }
    for (at, id, _) in positions {
        replies[at] = match why.as_deref() {
            Some(why) => failure("index", index, Some(&id), status, why),
            None => {
                let mut reply = item("index", index, Some(&id), status, "");
                reply["index"].as_object_mut().unwrap().remove("result");
                reply
            }
        };
    }
    accepted
}

/// Called inside the ordinary write admission after routing and authorization.
pub fn serve(
    stream: &mut wire::Wire,
    req: &http::Request,
    route: &route::Route,
    node: &Node,
    keep: bool,
) -> bool {
    let started = std::time::Instant::now();
    // Complete framing validation precedes every write, including deletes.
    let actions = match esbulk::parse(&req.body) {
        Ok(actions) => actions,
        Err(error) => {
            http::respond_with(stream, 400, &json!({"error": error}).to_string(), keep);
            return true;
        }
    };
    let Some(writer) = node.writer.as_deref() else {
        http::respond_with(stream, 503, "{\"error\":\"приём выключен\"}", keep);
        return true;
    };
    let plan = writer.plan();
    let strict = node
        .live_settings
        .read()
        .map(|settings| settings.strict_ingest)
        .unwrap_or(svindb_core::settings::Strict::Batch);
    let index = route.index.as_deref();
    let mut replies = vec![Value::Null; actions.len()];
    let mut pending = Vec::new();
    let mut unavailable = false;
    let mut accepted = false;
    for (at, mut action) in actions.into_iter().enumerate() {
        let refused = action.refused.take().or_else(|| match (index, action.index.as_deref()) {
            (Some(expected), Some(actual)) if expected != actual => {
                Some(format!("action _index {actual:?} отличается от индекса пути {expected:?}"))
            }
            _ => None,
        });
        if let Some(why) = refused {
            replies[at] = failure(
                &action.verb,
                index.or(action.index.as_deref()),
                action.id.as_deref(),
                400,
                &why,
            );
            continue;
        }
        if action.act == esbulk::Act::Index {
            let generated = action.id.is_none();
            match prepare(&mut action, &plan, strict, at) {
                Ok((doc, envelope)) => {
                    pending.push(Pending { at, id: doc.id.clone(), envelope, doc, generated })
                }
                Err(why) => {
                    replies[at] = failure(&action.verb, index, action.id.as_deref(), 400, &why);
                }
            }
            continue;
        }
        accepted |= flush(&mut pending, &mut replies, writer, node, index, &mut unavailable);
        let Some(id) = action.id.filter(|id| !id.is_empty()) else {
            replies[at] = failure("delete", index, None, 400, "delete требует непустой _id");
            continue;
        };
        let ids = [id.clone()];
        replies[at] = match writer.delete(&ids) {
            Ok(deleted) => {
                node.metrics.deleted(deleted as u64);
                match crate::replicate_effect(node, "delete", None, &ids) {
                    Some(ack) if !ack.enough() => {
                        unavailable = true;
                        failure(
                            "delete",
                            index,
                            Some(&id),
                            503,
                            "недостаточно подтверждений репликации",
                        )
                    }
                    _ => item(
                        "delete",
                        index,
                        Some(&id),
                        if deleted > 0 { 200 } else { 404 },
                        if deleted > 0 { "deleted" } else { "not_found" },
                    ),
                }
            }
            Err(why) => {
                unavailable = true;
                failure("delete", index, Some(&id), 503, &why)
            }
        };
    }
    accepted |= flush(&mut pending, &mut replies, writer, node, index, &mut unavailable);
    if accepted {
        node.metrics.ingest_bytes(req.body.len() as u64);
    }
    let errors = replies.iter().any(|reply| {
        reply
            .as_object()
            .is_some_and(|object| object.values().any(|detail| detail.get("error").is_some()))
    });
    let payload =
        json!({"took": started.elapsed().as_millis() as u64, "errors": errors, "items": replies});
    http::respond_with(stream, if unavailable { 503 } else { 200 }, &payload.to_string(), keep);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preparation_uses_action_id_and_keeps_source_id_as_data() {
        let plan = nested::Plan::build(
            &["id".into(), nested::SOURCE_FIELD.into()],
            &[svindb_core::index::Kind::Keyword; 2],
            &[0, 0],
            &[String::new()],
        );
        let mut actions =
            esbulk::parse(b"{\"index\":{\"_id\":\"action\"}}\n{\"id\":\"source\"}\n").unwrap();
        let (doc, envelope) =
            prepare(&mut actions[0], &plan, svindb_core::settings::Strict::Batch, 0).unwrap();
        assert_eq!(doc.id, "action");
        assert_eq!(envelope["_source"]["id"], "source");
        assert!(doc.cells.iter().any(|(field, _, text)| *field == 0 && text == "source"));
    }

    #[test]
    fn missing_action_id_generates_an_id_even_when_source_has_one() {
        let plan = nested::Plan::build(&[], &[], &[], &[String::new()]);
        let mut actions = esbulk::parse(b"{\"index\":{}}\n{\"id\":\"source\"}\n").unwrap();
        let (doc, _) =
            prepare(&mut actions[0], &plan, svindb_core::settings::Strict::Batch, 0).unwrap();
        assert!(!doc.id.is_empty());
        assert_ne!(doc.id, "source");
        assert_eq!(actions[0].id.as_deref(), Some(doc.id.as_str()));
    }
}
