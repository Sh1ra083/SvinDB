//! Приём корпуса из NDJSON: по документу на строку.

use std::fs::File;
use std::io::{BufRead, BufReader};

use svindb_core::indexer::Indexer;

/// Читает файл и складывает документы в индекс.
#[allow(clippy::too_many_arguments)]
pub fn index_file(
    path: &str,
    dir: &str,
    fields: &[String],
    boosts: &[f64],
    kinds: &[svindb_core::index::Kind],
    stored: &[bool],
    searched: &[bool],
    per_segment: u32,
    posmin: u32,
    fids: &[u32],
    groups: &[u32],
    group_paths: &[String],
) -> std::io::Result<Indexer> {
    let mut ix = Indexer::typed_stored(
        dir,
        fields.to_vec(),
        boosts.to_vec(),
        kinds.to_vec(),
        stored.to_vec(),
        per_segment,
    );
    ix.set_searched(searched.to_vec());
    ix.set_fids(fids.to_vec());
    if group_paths.len() > 1 {
        ix.set_groups(groups.to_vec(), group_paths.to_vec());
    }
    ix.set_posmin(posmin);
    // Массовая загрузка пишет сегменты фоном: разбор следующих документов идёт,
    // пока предыдущий буфер ложится на диск. Замок состава здесь свой — у этого
    // пути нет ни укрупнения, ни второго писателя, спорить не с кем.
    ix.background(std::sync::Arc::new(std::sync::Mutex::new(())));
    let file = File::open(path)?;
    let plan = crate::nested::Plan::build(fields, kinds, groups, group_paths);
    // Офлайновый путь отчитывается в поток ошибок, а не в ответ: у него нет
    // ответа. Молчать здесь нельзя ровно по той же причине — заливка на
    // миллион строк с опечаткой в имени поля выглядела бы удавшейся.
    let mut report = crate::intake::Report::default();

    for (n, line) in BufReader::with_capacity(1 << 20, file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let doc: serde_json::Value = serde_json::from_str(&line).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("строка {}: {e}", n + 1))
        })?;

        // Тот же план и та же укладка, что у HTTP-приёма. Иначе вложенный путь
        // `author.name` здесь искался бы буквальным ключом, а массивы вообще не
        // получили бы полосы арности — два способа построили бы разные индексы
        // из одного корпуса.
        let parsed = plan.take(&doc, n, &mut report).map_err(|why| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("строка {}: {why}", n + 1))
        })?;
        crate::nested::put_doc(&mut ix, &parsed, false);
    }
    ix.flush_now();
    if !report.clean() {
        eprintln!("приём: {}", report.refusal());
    }
    Ok(ix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn corpus(name: &str, lines: &[&str]) -> String {
        let path = std::env::temp_dir().join(format!("svindb-ndjson-{name}.ndjson"));
        let mut f = std::fs::File::create(&path).unwrap();
        for l in lines {
            writeln!(f, "{l}").unwrap();
        }
        path.to_string_lossy().to_string()
    }

    #[test]
    fn reads_documents_and_fields() {
        let src = corpus(
            "basic",
            &[
                r#"{"id":"a","title":"первый","body":"тело первого"}"#,
                r#"{"id":"b","title":"второй","body":"тело второго"}"#,
                "",
            ],
        );
        let dir = std::env::temp_dir().join("svindb-ndjson-out");
        std::fs::remove_dir_all(&dir).ok();
        let dir = dir.to_string_lossy().to_string();
        let ix = index_file(
            &src,
            &dir,
            &["title".into(), "body".into()],
            &[2.0, 1.0],
            &[svindb_core::index::Kind::Text; 2],
            &[true; 2],
            &[true; 2],
            100,
            0,
            &[0, 1],
            &[0, 0],
            &[String::new()],
        )
        .unwrap();
        assert_eq!(ix.total_docs, 2, "пустая строка не документ");
        std::fs::remove_file(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// CLI-индексация читает вложенность и группы тем же планом, что HTTP.
    #[test]
    fn nested_documents_and_slots_survive_offline_indexing() {
        use svindb_core::index::Kind;
        use svindb_core::segments::{QuerySpec, Segments, SpecWord, Term};

        let src = corpus(
            "nested",
            &[
                r#"{"id":"a","author":{"name":"аня"},"reactions":[{"emoji":"up"},{"emoji":"fire"}]}"#,
                r#"{"id":"b","author":{"name":"борис"},"reactions":[]}"#,
            ],
        );
        let dir = std::env::temp_dir().join("svindb-ndjson-nested-out");
        std::fs::remove_dir_all(&dir).ok();
        let dir = dir.to_string_lossy().to_string();
        index_file(
            &src,
            &dir,
            &["author.name".into(), "reactions.emoji".into()],
            &[1.0, 1.0],
            &[Kind::Keyword, Kind::Keyword],
            &[true, true],
            &[true, true],
            100,
            0,
            &[0, 1],
            &[0, 1],
            &[String::new(), "reactions".into()],
        )
        .expect("офлайн-индекс");

        let segs = Segments::load(&dir).expect("индекс открылся");
        let out = segs.search_with(
            &QuerySpec {
                words: vec![SpecWord {
                    fid: 0,
                    terms: vec![Term::parse(&svindb_core::indexer::keyword_term("аня", true))],
                    given: None,
                }],
                k: 10,
                ..QuerySpec::default()
            },
            1,
        );
        assert_eq!(out.hits.len(), 1, "вложенное ключевое поле осталось пустым");
        assert_eq!(
            segs.list[0].slots[1].range(0).1,
            2,
            "два элемента массива не получили два слота"
        );
        assert_eq!(segs.list[0].slots[1].range(1).1, 0, "пустой массив стал одним слотом");
        std::fs::remove_file(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// То, чего не умел поиск подстроки: экранированные кавычки в значении.
    #[test]
    fn escaped_quotes_do_not_truncate_the_value() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"body":"он сказал \"привет\" и ушёл"}"#).unwrap();
        let t = crate::intake::take_cell(v.get("body"), svindb_core::index::Kind::Text);
        assert_eq!(t.text(), "он сказал \"привет\" и ушёл");
    }

    #[test]
    fn missing_and_non_text_fields() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"n":42,"b":true,"o":{"x":1},"z":null}"#).unwrap();
        use svindb_core::index::Kind;
        let take = |k: &str| crate::intake::take_cell(v.get(k), Kind::Text);
        assert_eq!(take("n").text(), "42");
        assert_eq!(take("b").text(), "true");
        // Объект в текстовом поле даёт пустоту приёму — но, в отличие от
        // прежнего, называется потерей и попадает в отчёт.
        assert_eq!(take("o").text(), "");
        assert!(take("o").is_loss());
        assert_eq!(take("z").text(), "");
        assert!(!take("z").is_loss(), "null — законное отсутствие");
        assert!(v.get("нет").is_none());
    }

    /// Битая строка называет свой номер, а не роняет прогон без объяснения.
    #[test]
    fn broken_line_is_reported_with_its_number() {
        let src = corpus("broken", &[r#"{"id":"a"}"#, "{это не json}"]);
        let dir = std::env::temp_dir().join("svindb-ndjson-broken");
        std::fs::remove_dir_all(&dir).ok();
        let dir = dir.to_string_lossy().to_string();
        let err = match index_file(
            &src,
            &dir,
            &["title".into()],
            &[1.0],
            &[svindb_core::index::Kind::Text],
            &[true],
            &[true],
            100,
            0,
            &[0],
            &[0],
            &[String::new()],
        ) {
            Err(e) => e,
            Ok(_) => panic!("битая строка обязана остановить приём"),
        };
        assert!(err.to_string().contains("строка 2"), "{err}");
        std::fs::remove_file(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }
}
