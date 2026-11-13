//! Потоковый read-only экспорт ID/source для storage-oracle офлайн-копии.
use std::io::{self, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args().nth(1).ok_or("нужен root остановленной экспериментальной копии")?;
    let mut output = io::BufWriter::with_capacity(1 << 20, io::stdout().lock());
    for segment in svindb_core::manifest::segments_checked(&root)? {
        let ix = svindb_core::index::load(&segment)?;
        let field = ix.field_of("__svindb_source").ok_or("нет authoritative source")?;
        let store = ix.fields[field].store.as_ref().ok_or("нет source storage")?;
        let mut walk = store.walk();
        let mut seen = 0usize;
        while let Some((doc, bytes)) = walk.next() {
            if doc as usize != seen {
                return Err("непоследовательный source".into());
            }
            seen += 1;
            if !ix.is_live(doc) {
                continue;
            }
            let id = ix.idmap.id_at(doc).ok_or("нет внешнего ID")?;
            let source: serde_json::Value = serde_json::from_slice(bytes)?;
            serde_json::to_writer(&mut output, &serde_json::json!({"id": id, "source": source}))?;
            output.write_all(b"\n")?;
        }
        if walk.broken() || seen != ix.docs {
            return Err("неполный source".into());
        }
    }
    output.flush()?;
    Ok(())
}
