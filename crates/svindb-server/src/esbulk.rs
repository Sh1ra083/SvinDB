//! Тело `_bulk` в виде, каким его шлёт Elasticsearch: строка действия, следом
//! строка документа.
//!
//! ## Зачем
//!
//! Переселенцу проще всего оставить свой загрузчик как есть. Родное тело
//! `{"documents":[…]}` при этом никуда не девается — диалект выбирается по
//! первому непробельному байту, одним правилом на весь узел.
//!
//! ## Чего здесь нет и почему
//!
//! `create` и `update` — отказ по документу, а не подделка. «Есть ли уже
//! такой» узел дёшево не проверяет, а частичное обновление требует прочитать,
//! исправить и записать заново, чего у движка нет вовсе. Ответить на них
//! «сделано» значило бы соврать.

/// Что делать с документом.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Act {
    /// Принять, заменив прежнюю версию по `_id`.
    Index,
    /// Погасить по `_id`.
    Delete,
}

/// Одна строка пачки.
#[derive(Debug)]
pub struct Item {
    pub verb: String,
    pub act: Act,
    pub id: Option<String>,
    /// Имя индекса из action metadata. Терять его нельзя: путь
    /// `/orders/_bulk` с `_index:"users"` иначе молча положил бы документ не
    /// туда, куда попросил клиент.
    pub index: Option<String>,
    /// Тело документа. У гашения его нет.
    pub body: Option<serde_json::Value>,
    /// Отказ по этому элементу: он не выполняется, а остальные — выполняются.
    pub refused: Option<String>,
}

/// Похоже ли тело на NDJSON-пачку.
///
/// Признак — не догадка: родное тело это объект с ключом `documents`, и оно
/// разбирается целиком. Всё прочее непустое читается как NDJSON, а негодное
/// даст отказ с номером строки — то есть молча ни одно тело не пройдёт.
pub fn looks_like_ndjson(body: &[u8]) -> bool {
    let text = String::from_utf8_lossy(body);
    let head = text.trim_start();
    if head.is_empty() {
        return false;
    }
    if !head.starts_with('{') {
        return true;
    }
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(v) => v.get("documents").is_none(),
        // Не разобралось целиком — значит это не один объект, а несколько
        // строк подряд.
        Err(_) => true,
    }
}

/// Разбирает NDJSON-пачку. `Err` — тело негодно целиком.
pub fn parse(body: &[u8]) -> Result<Vec<Item>, String> {
    let text = std::str::from_utf8(body).map_err(|e| format!("тело не UTF-8: {e}"))?;
    let mut out = Vec::new();
    let mut lines = text.lines().enumerate().filter(|(_, l)| !l.trim().is_empty());
    while let Some((n, head)) = lines.next() {
        let v: serde_json::Value = serde_json::from_str(head)
            .map_err(|e| format!("строка {}: действие не разобралось: {e}", n + 1))?;
        let Some(obj) = v.as_object() else {
            return Err(format!("строка {}: действие — не объект", n + 1));
        };
        let Some((verb, meta)) = obj.iter().next() else {
            return Err(format!("строка {}: действие пустое", n + 1));
        };
        if obj.len() != 1 {
            return Err(format!("строка {}: ожидается ровно одно действие", n + 1));
        }
        let mut refused = None;
        if let Some(metadata) = meta.as_object() {
            if let Some(unknown) =
                metadata.keys().find(|key| !matches!(key.as_str(), "_id" | "_index"))
            {
                refused = Some(format!("неподдерживаемый action metadata ключ {unknown:?}"));
            }
        } else {
            refused = Some("action metadata должен быть объектом".into());
        }
        // Идентификатор берётся тем же переводом, что и на приёме: `as_str`
        // на числовом `_id` даёт пустоту, документ становится неадресуемым, и
        // обнаруживается это лишь тогда, когда его не удаётся ни заменить, ни
        // удалить. Ровно на этом уже спотыкались `_bulk` и `_delete`.
        let id = match meta.get("_id") {
            None => None,
            Some(x @ (serde_json::Value::String(_) | serde_json::Value::Number(_))) => {
                let t = crate::intake::take_cell(Some(x), svindb_core::index::Kind::Keyword);
                if t.is_loss() {
                    refused = Some("_id — не строка и не число".into());
                    None
                } else {
                    let id = t.into_text();
                    if id.is_empty() {
                        refused = Some("_id must not be empty".into());
                    }
                    Some(id)
                }
            }
            Some(_) => {
                refused = Some("_id — не строка и не число".into());
                None
            }
        };
        let index = match meta.get("_index") {
            None => None,
            Some(serde_json::Value::String(value)) if !value.is_empty() => Some(value.clone()),
            Some(_) => {
                refused = Some("_index — не непустая строка".into());
                None
            }
        };
        let act = match verb.as_str() {
            "index" | "create" => Act::Index,
            "delete" => Act::Delete,
            "update" => Act::Index,
            other => return Err(format!("строка {}: действие {other:?} неизвестно", n + 1)),
        };
        if act == Act::Delete {
            out.push(Item { verb: verb.clone(), act, id, index, body: None, refused });
            continue;
        }
        let Some((bn, line)) = lines.next() else {
            return Err(format!("строка {}: за действием нет документа", n + 1));
        };
        let doc: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| format!("строка {}: документ не разобрался: {e}", bn + 1))?;
        // `create` и `update` отвергаются **по документу**: пачка при этом
        // выполняется, а клиент видит, что именно не сделано и почему.
        refused = refused.or_else(|| match verb.as_str() {
            "create" => Some(
                "create требует знать, есть ли уже такой документ, — узел этого дёшево не \
                 проверяет; шлите index"
                    .to_string(),
            ),
            "update" => Some(
                "update требует прочитать, исправить и записать заново — этого у движка нет; \
                 шлите index с полным документом"
                    .to_string(),
            ),
            _ => None,
        });
        if !doc.is_object() {
            refused = Some("тело документа должно быть JSON-объектом".into());
        }
        out.push(Item { verb: verb.clone(), act, id, index, body: Some(doc), refused });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Родное тело NDJSON-ом не считается, а NDJSON — считается.
    #[test]
    fn the_dialect_is_decided_by_the_body_not_guessed() {
        assert!(!looks_like_ndjson(br#"{"documents":[{"id":"a"}]}"#));
        assert!(!looks_like_ndjson(b""));
        assert!(looks_like_ndjson(b"{\"index\":{\"_id\":\"a\"}}\n{\"title\":\"\xd0\xb0\"}"));
        // Один объект без `documents` — тоже NDJSON из одной строки действия.
        assert!(looks_like_ndjson(br#"{"delete":{"_id":"a"}}"#));
    }

    /// Пара «действие — документ» читается, а непарная строка отказывает с
    /// номером.
    #[test]
    fn an_action_without_its_document_is_refused_by_line_number() {
        let body = "{\"index\":{\"_id\":\"a\"}}\n{\"t\":\"раз\"}\n{\"index\":{\"_id\":\"b\"}}";
        let e = parse(body.as_bytes()).expect_err("непарная строка принята");
        assert!(e.contains("строка 3"), "не тот номер строки: {e}");

        // Парное тело читается целиком — иначе проверка выше прошла бы и у
        // разбора, отвергающего всякую пачку.
        let body = "{\"index\":{\"_id\":\"a\"}}\n{\"t\":\"раз\"}\n{\"delete\":{\"_id\":\"b\"}}";
        let items = parse(body.as_bytes()).expect("парная пачка отвергнута");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].act, Act::Index);
        assert_eq!(items[0].id.as_deref(), Some("a"));
        assert_eq!(items[1].act, Act::Delete);
        assert!(items[1].body.is_none(), "у гашения не бывает документа");
    }

    /// `create` и `update` отвергаются по документу, а не подделываются.
    #[test]
    fn create_and_update_are_refused_per_document_not_faked() {
        let body = "{\"create\":{\"_id\":\"a\"}}\n{\"t\":\"раз\"}\n\
                    {\"update\":{\"_id\":\"b\"}}\n{\"doc\":{\"t\":\"два\"}}\n\
                    {\"index\":{\"_id\":\"c\"}}\n{\"t\":\"три\"}";
        let items = parse(body.as_bytes()).expect("пачка отвергнута целиком");
        assert_eq!(items.len(), 3);
        assert!(items[0].refused.is_some(), "create принят молча");
        assert!(items[1].refused.is_some(), "update принят молча");
        // А `index` рядом с ними выполняется: отказ по документу, а не по пачке.
        assert!(items[2].refused.is_none(), "index отвергнут заодно с соседями");
    }

    /// Неизвестное действие — отказ всей пачки: догадываться о нём нельзя.
    #[test]
    fn an_unknown_action_refuses_the_whole_batch() {
        let e = parse(b"{\"upsert\":{\"_id\":\"a\"}}\n{\"t\":\"x\"}")
            .expect_err("неизвестное действие принято");
        assert!(e.contains("upsert"), "отказ не назвал действие: {e}");
    }

    /// Числовой `_id` доезжает, а не теряется.
    ///
    /// `as_str` на числе даёт пустоту, документ становится неадресуемым, и
    /// обнаруживается это лишь тогда, когда его не удаётся ни заменить, ни
    /// удалить. На этом уже спотыкались и приём пачкой, и гашение.
    #[test]
    fn a_numeric_id_arrives_instead_of_vanishing() {
        let items = parse(b"{\"index\":{\"_id\":101}}\n{\"t\":\"x\"}").expect("пачка");
        assert_eq!(items[0].id.as_deref(), Some("101"), "числовой _id потерян");

        // Строковый по-прежнему доезжает — иначе проверка выше говорила бы лишь
        // о том, что что-то там читается.
        let items = parse(b"{\"index\":{\"_id\":\"a\"}}\n{\"t\":\"x\"}").expect("пачка");
        assert_eq!(items[0].id.as_deref(), Some("a"));

        // Ошибка метаданных принадлежит элементу, а не соседним действиям.
        let items = parse(b"{\"index\":{\"_id\":{}}}\n{\"t\":\"x\"}").unwrap();
        assert!(items[0].refused.as_deref().unwrap().contains("_id"));
    }

    #[test]
    fn semantic_metadata_errors_are_per_item_and_verbs_are_preserved() {
        for metadata in
            [r#"{"routing":"x"}"#, r#"{"_id":null}"#, r#"{"_id":true}"#, r#"{"_index":4}"#, "[]"]
        {
            let body =
                format!("{{\"index\":{metadata}}}\n{{}}\n{{\"delete\":{{\"_id\":\"ok\"}}}}\n");
            let items = parse(body.as_bytes()).unwrap();
            assert!(items[0].refused.is_some(), "{metadata}");
            assert!(items[1].refused.is_none());
            assert_eq!(items[1].verb, "delete");
        }
        let items = parse(b"{\"create\":{}}\n{}\n{\"update\":{}}\n{}\n").unwrap();
        assert_eq!(items[0].verb, "create");
        assert_eq!(items[1].verb, "update");
    }

    #[test]
    fn multiple_actions_on_one_line_and_late_bad_json_refuse_framing() {
        assert!(parse(b"{\"delete\":{},\"index\":{}}\n{}").is_err());
        assert!(parse(b"{\"delete\":{\"_id\":\"a\"}}\n{bad}").is_err());
        let items = parse(b"{\"index\":{}}\n[]\n").unwrap();
        assert!(items[0].refused.is_some());
    }
}
