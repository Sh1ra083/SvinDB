//! Идентификаторы документов: номер сегмента — в строку, которую дал приём.

use crate::mmap::Region;

/// Через сколько записей ставится опора.
pub const ANCHOR: usize = 64;

/// Влезает ли очередная опора идентификаторов в четыре байта.
///
/// ## Почему проверяется, а не подразумевается
///
/// Опора в `ids.idx` — смещение внутри `ids.bin`, и лежит оно в четырёх байтах.
/// Заворот не отказывает и не портит заголовок: опора начинает указывать в
/// середину чужого идентификатора. Дальше выдача возвращает документу **чужой**
/// `_id`, а гашение по идентификатору убивает не тот документ, — и то и другое
/// молча. Ста миллионов документов по сорок байт идентификатора хватает, чтобы
/// `ids.bin` перевалил за потолок.
///
/// Отказ, а не паника: выпуск собирается с `panic = "abort"`.
fn fits_anchor(len: usize, blob: &str, at: usize) -> std::io::Result<()> {
    let ceiling = crate::dict::offset_ceiling();
    if len as u64 <= ceiling {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{blob}: идентификаторы перевалили за четыре гигабайта на {at}-м документе \
             ({len} байт при потолке {ceiling}) — опора рядом с ними тридцатидвухбитная и \
             завернулась бы на чужой идентификатор; уменьшите flush_docs у приёма \
             или target_docs у укрупнения"
        ),
    ))
}

/// Кодировщик идентификаторов: один за раз, поверх любого приёмника.
///
/// ## Почему он один на два пути
///
/// Идентификаторы пишут двое. Приём держит их списком и так — сброс всё равно
/// собран в памяти целиком, — и кодирует одним куском ([`encode`]). Слияние
/// списка не держит: на сегменте в десятки миллионов документов один только
/// `Vec<String>` это гигабайты, а кодировщику дальше **одного** предыдущего
/// идентификатора смотреть не на что. Поэтому у него поток ([`Writer`]).
///
/// Кодирование при этом обязано быть ровно одно. Разойдись эти два хоть на
/// байт, и `ids.bin` слитого сегмента разбирался бы не тем же кодом, что
/// приёмного: `get` съезжал бы в середину чужой записи и отдавал документу
/// чужой `_id` — молча, прямо на выдаче.
struct Coder {
    /// Предыдущий идентификатор: от него считается общее начало.
    prev: Vec<u8>,
    /// Сколько записей уже закодировано — по нему ставятся опоры.
    n: usize,
    anchors: Vec<u8>,
}

impl Coder {
    fn new(hint: usize) -> Coder {
        Coder {
            prev: Vec::with_capacity(64),
            n: 0,
            anchors: Vec::with_capacity(hint.div_ceil(ANCHOR) * 4),
        }
    }

    /// Дописывает идентификатор в `buf`.
    ///
    /// `base` — сколько байт потока уже ушло из `buf` наружу: опора это
    /// смещение **в файле**, а не в буфере, и буфер опустошается, а смещение
    /// нет.
    fn push(
        &mut self,
        id: &[u8],
        base: usize,
        buf: &mut Vec<u8>,
        blob: &str,
    ) -> std::io::Result<()> {
        let at = base + buf.len();
        let common = if self.n.is_multiple_of(ANCHOR) {
            // Отказ **до** записи усечённой опоры: записанная уже неотличима от
            // настоящей.
            fits_anchor(at, blob, self.n + 1)?;
            self.anchors.extend((at as u32).to_le_bytes());
            0
        } else {
            id.iter().zip(self.prev.iter()).take_while(|(a, b)| a == b).count()
        };
        varint(buf, common as u64);
        varint(buf, (id.len() - common) as u64);
        buf.extend_from_slice(&id[common..]);
        self.prev.clear();
        self.prev.extend_from_slice(id);
        self.n += 1;
        Ok(())
    }
}

/// Записывает идентификаторы во фронт-кодированном виде.
///
/// Возвращает `(данные, смещения опор)`. `blob` — куда ляжет вывод; нужен, чтобы
/// отказ по потолку назвал файл, а не оставил оператора гадать.
pub fn encode(ids: &[String], blob: &str) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let mut out: Vec<u8> = Vec::with_capacity(ids.len() * 4);
    let mut coder = Coder::new(ids.len());
    for id in ids.iter() {
        coder.push(id.as_bytes(), 0, &mut out, blob)?;
    }
    Ok((out, coder.anchors))
}

/// Потоковая запись `ids.bin`.
///
/// Слияние прежде собирало **все** идентификаторы выхода отдельными `String`,
/// а потом кодировало их в один блоб — то есть держало корпус в памяти дважды.
/// На шестидесяти четырёх миллионах документов это порядка четырёх гигабайт, и
/// это была одна из трёх причин, по которым потолок сегмента стоял там, где
/// стоял.
///
/// Опоры остаются в памяти: их четыре байта на шестьдесят четыре документа,
/// то есть в сто раз меньше самих идентификаторов, и отдать их надо одним
/// куском в `ids.idx`.
pub struct Writer {
    sink: std::io::BufWriter<std::fs::File>,
    path: String,
    /// Черновик **одной** записи. Пуст на входе в `push` и на выходе из него:
    /// иначе смещение опоры пришлось бы складывать из двух слагаемых, а
    /// сложение это и есть то место, где такие вещи разъезжаются.
    rec: Vec<u8>,
    /// Длина файла. Опоры считаются от неё и ни от чего больше.
    len: usize,
    coder: Coder,
}

/// Размер буфера записи. Собирать больше незачем: он не для смещений, а лишь
/// затем, чтобы не звать ядро на каждый идентификатор.
const FLUSH: usize = 1 << 18;

impl Writer {
    pub fn create(path: &str) -> std::io::Result<Writer> {
        Ok(Writer {
            sink: std::io::BufWriter::with_capacity(FLUSH, std::fs::File::create(path)?),
            path: path.to_string(),
            rec: Vec::with_capacity(256),
            len: 0,
            coder: Coder::new(0),
        })
    }

    /// Дописывает идентификатор. Байты, а не строка: слияние читает их прямо из
    /// `ids.bin` источника, и заводить под каждый `String` ему незачем.
    pub fn push(&mut self, id: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        self.coder.push(id, self.len, &mut self.rec, &self.path)?;
        self.sink.write_all(&self.rec)?;
        self.len += self.rec.len();
        self.rec.clear();
        Ok(())
    }

    /// Дописывает хвост буфера, **доводит файл до диска** и отдаёт опоры.
    ///
    /// Довод тот же, что у `merge::write`: `write_all` возвращается, когда байты
    /// приняты ядром, а не когда легли на носитель, и порядок «заголовок
    /// последним» без этого ничего не обещает.
    pub fn finish(mut self) -> std::io::Result<Vec<u8>> {
        use std::io::Write;
        self.sink.flush()?;
        self.sink.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        Ok(self.coder.anchors)
    }
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn read_varint(src: &[u8], at: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0u32;
    loop {
        let Some(&b) = src.get(*at) else { return v };
        *at += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return v;
        }
        shift += 7;
    }
}

/// Идентификаторы сегмента, отображённые в память.
pub struct Ids {
    blob: Region,
    anchors: Region,
    count: usize,
}

impl Ids {
    /// Открывает файлы сегмента. Отсутствие файлов — не ошибка: индекс без
    /// идентификаторов вполне работоспособен, просто не умеет их отдавать.
    pub fn open(src: &crate::container::SegmentSource, count: usize) -> Ids {
        Ids {
            blob: src.try_open("ids.bin").unwrap_or_else(Region::empty),
            anchors: src.try_open("ids.idx").unwrap_or_else(Region::empty),
            count,
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0 || self.blob.is_empty()
    }

    /// Сходятся ли опоры с числом документов. `Err` — сегмент повреждён.
    ///
    /// Теперь из него собирается **обратное отображение**, а значит от него
    /// зависит удаление. Обрезанные опоры дают `get` за границей, тот отдаёт
    /// `None`, сборка кладёт в отображение пустую строку — и «удали X»
    /// превращается в «такого нет». Молча, с ответом «удалено ноль».
    pub fn check(&self, docs: usize) -> Result<(), String> {
        if docs == 0 || self.blob.is_empty() {
            return Ok(());
        }
        let want = docs.div_ceil(ANCHOR) * 4;
        if self.anchors.len_bytes() < want {
            return Err(format!(
                "ids.idx: {} байт вместо {want} — на {docs} документов",
                self.anchors.len_bytes()
            ));
        }
        Ok(())
    }

    /// Последовательный проход по идентификаторам.
    ///
    /// Отдельно от `get`, потому что тот каждый раз идёт от опоры: перебрать так
    /// все документы значит пройти каждую группу по шестьдесят четыре раза.
    /// Слиянию сегментов нужен проход, и он обязан быть одним.
    pub fn walk(&self) -> IdWalk<'_> {
        IdWalk {
            src: self.blob.slice(0, self.blob.len_bytes()),
            at: 0,
            left: self.count,
            cur: Vec::with_capacity(32),
        }
    }

    /// Идентификатор документа по его номеру внутри сегмента.
    pub fn get(&self, doc: usize) -> Option<String> {
        if doc >= self.count || self.blob.is_empty() {
            return None;
        }
        let anchor = doc / ANCHOR;
        let e = self.anchors.slice(anchor * 4, 4);
        if e.len() < 4 {
            return None;
        }
        let mut at = u32::from_le_bytes([e[0], e[1], e[2], e[3]]) as usize;

        // Хвост опорной группы разбирается подряд: у каждой записи общая часть
        // берётся из предыдущей, своя — из файла.
        let src = self.blob.slice(0, self.blob.len_bytes());
        let mut cur: Vec<u8> = Vec::with_capacity(32);
        for _ in 0..=(doc % ANCHOR) {
            let common = read_varint(src, &mut at) as usize;
            let tail = read_varint(src, &mut at) as usize;
            cur.truncate(common.min(cur.len()));
            let end = (at + tail).min(src.len());
            cur.extend_from_slice(&src[at..end]);
            at = end;
        }
        String::from_utf8(cur).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn roundtrip(ids: &[String]) {
        let dir = std::env::temp_dir().join(format!("svindb-ids-{}", ids.len()));
        std::fs::create_dir_all(&dir).unwrap();
        let (blob, anchors) = encode(ids, "проверка/ids.bin").expect("влезло");
        std::fs::File::create(dir.join("ids.bin")).unwrap().write_all(&blob).unwrap();
        std::fs::File::create(dir.join("ids.idx")).unwrap().write_all(&anchors).unwrap();

        let path = dir.to_string_lossy().to_string();
        let store = Ids::open(&crate::container::loose(&path), ids.len());
        for (i, want) in ids.iter().enumerate() {
            assert_eq!(store.get(i).as_deref(), Some(want.as_str()), "документ {i}");
        }
        assert_eq!(store.get(ids.len()), None, "за концом ничего нет");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn plain_numbers() {
        let ids: Vec<String> = (0..500).map(|i| i.to_string()).collect();
        roundtrip(&ids);
    }

    /// Длинный общий префикс — тот случай, ради которого всё и затевалось.
    #[test]
    fn long_shared_prefix() {
        let ids: Vec<String> = (0..300).map(|i| format!("urn:example:document:{i:08}")).collect();
        roundtrip(&ids);
    }

    /// Общего префикса нет вовсе — формат не должен на этом ломаться.
    #[test]
    fn nothing_in_common() {
        let ids: Vec<String> = (0..200)
            .map(|i| {
                let c = (b'a' + (i % 26) as u8) as char;
                format!("{c}{}", "x".repeat(i % 17))
            })
            .collect();
        roundtrip(&ids);
    }

    #[test]
    fn unicode_and_empty() {
        let mut ids: Vec<String> =
            vec![String::new(), "документ-один".into(), "документ-два".into()];
        ids.extend((0..70).map(|i| format!("док-{i}")));
        roundtrip(&ids);
    }

    /// Ровно граница опоры и одна запись — оба края.
    #[test]
    fn anchor_edges() {
        roundtrip(&(0..ANCHOR).map(|i| format!("a{i}")).collect::<Vec<_>>());
        roundtrip(&(0..ANCHOR + 1).map(|i| format!("a{i}")).collect::<Vec<_>>());
        roundtrip(&["один".to_string()]);
    }

    /// Место действительно экономится, а не только выглядит компактнее.
    #[test]
    fn front_coding_actually_saves() {
        let ids: Vec<String> = (0..10_000).map(|i| format!("doc-2026-08-{i:09}")).collect();
        let plain: usize = ids.iter().map(|s| s.len() + 1).sum();
        let (blob, anchors) = encode(&ids, "проверка/ids.bin").expect("влезло");
        let coded = blob.len() + anchors.len();
        assert!(coded * 2 < plain, "фронт-кодирование {coded} против {plain}");
    }

    /// Потолок опоры идентификаторов: `u32::MAX` ещё влезает, на байт больше — нет.
    ///
    /// ## Что ломалось бы без отказа
    ///
    /// `encode` клал бы `blob.len() as u32` завёрнутым, и опора в `ids.idx`
    /// указывала бы в середину чужого идентификатора того же файла. Дальше
    /// выдача приписывает документу **чужой** `_id`, а гашение по
    /// идентификатору убивает не тот документ. Отказа при этом нет ни на одном
    /// шаге: файл цел, длины сходятся, `check` прежде смотрел только размеры.
    ///
    /// Файл в четыре гигабайта в тесте не собрать, поэтому проверяется правило
    /// и его края — те, на которых сдвиг границы на единицу всё испортил бы.
    #[test]
    fn the_id_anchor_ceiling_is_inclusive_of_u32_max() {
        assert!(fits_anchor(0, "seg0007/ids.bin", 1).is_ok(), "пустые идентификаторы влезают");
        assert!(
            fits_anchor(crate::dict::OFFSET_CEILING as usize, "seg0007/ids.bin", 64).is_ok(),
            "потолок обязан быть самим u32::MAX"
        );

        let e = fits_anchor(crate::dict::OFFSET_CEILING as usize + 1, "seg0007/ids.bin", 65)
            .expect_err("на байт за потолком обязан быть отказ");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        assert!(msg.contains("seg0007/ids.bin"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("4294967296"), "отказ не назвал достигнутое: {msg}");
        assert!(msg.contains("4294967295"), "отказ не назвал потолок: {msg}");
        assert!(msg.contains("65-м документе"), "отказ не назвал место: {msg}");
        assert!(msg.contains("flush_docs"), "отказ не подсказал выход приёму: {msg}");
        assert!(msg.contains("target_docs"), "отказ не подсказал выход укрупнению: {msg}");
    }

    /// Охрана стоит **на пути записи**, а не рядом с ним.
    ///
    /// Проверка выше меряет предикат точными числами — и проходит одинаково при
    /// вызванной охране и при снятой. Здесь потолок опущен до сотни байт, вход
    /// обычный и маленький, и `encode` обязан отказать раньше, чем усечённая
    /// опора ляжет в `anchors`. Сними охрану — идентификаторы соберутся, и
    /// проверка упадёт.
    #[test]
    fn the_anchor_guard_stands_on_the_write_path() {
        // Три опоры: первая приходится на пустой ещё поток и влезает при любом
        // потолке, вторая и третья — уже нет.
        let ids: Vec<String> = (0..3 * ANCHOR).map(|i| format!("док-{i:09}")).collect();

        // При настоящем потолке тот же вход обязан собираться.
        let (blob, _) = encode(&ids, "seg0007/ids.bin").expect("обычные идентификаторы собираются");
        assert!(blob.len() > 100, "вход обязан перерасти опущенный потолок: {}", blob.len());

        let _low = crate::dict::lower_offset_ceiling(100);
        // Разбор `match`, а не `expect_err`: тот при удаче вывалил бы в отчёт
        // весь поток целиком, и причину пришлось бы искать глазами.
        let e = match encode(&ids, "seg0007/ids.bin") {
            Ok(_) => panic!("охрана не на пути записи: опора за потолком легла в поток"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        assert!(msg.contains("seg0007/ids.bin"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("при потолке 100"), "отказ померил не тем потолком: {msg}");
        assert!(msg.contains("flush_docs"), "отказ не подсказал выход: {msg}");
    }

    /// Поток и накопление дают **один и тот же** `ids.bin`, байт в байт.
    ///
    /// ## Почему это несущая проверка
    ///
    /// После правки `ids.bin` слитого сегмента пишет [`Writer`], а приёмного —
    /// по-прежнему [`encode`]. Читатель у них один: `Ids::get` идёт от опоры и
    /// разбирает хвост группы подряд. Разойдись эти двое хоть на байт — и
    /// документу слитого сегмента доставался бы **чужой** `_id`, а гашение по
    /// идентификатору убивало бы не тот документ. Молча.
    ///
    /// Вход нарочно **длиннее буфера записи**: опора это смещение в файле, а
    /// буфер за время сборки опустошается много раз. Пока `ids.bin` меньше
    /// буфера, «смещение в файле» и «смещение в буфере» — одно и то же число, и
    /// подмена одного другим проходит незамеченной.
    #[test]
    fn the_streaming_writer_matches_the_accumulating_encoder() {
        // Длинные общие начала, срывы общего начала и пустые строки — задеты
        // все ветви фронт-кодирования.
        //
        // Общего начала у соседей почти нет — цифры перевёрнуты: иначе
        // фронт-кодирование ужимает поток так, что до сброса дело не доходит, и
        // проверка перестаёт проверять то, ради чего заведена.
        let ids: Vec<String> = (0..200_000u32)
            .map(|i| match i % 1000 {
                0 => String::new(),
                7 => format!("{i}"),
                _ => {
                    let back: String = format!("{i:08}").chars().rev().collect();
                    format!("urn:svindb:документ:{back}:v1")
                }
            })
            .collect();
        let (want_blob, want_anchors) = encode(&ids, "проверка/ids.bin").expect("влезло");
        assert!(
            want_blob.len() > FLUSH * 2,
            "вход обязан перерасти буфер записи, иначе смещение в файле и смещение \
             в буфере — одно и то же число: {}",
            want_blob.len()
        );

        let dir = std::env::temp_dir().join("svindb-ids-stream");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ids.bin").to_string_lossy().to_string();
        let mut w = Writer::create(&path).expect("файл заведён");
        for id in ids.iter() {
            w.push(id.as_bytes()).expect("идентификатор лёг");
        }
        let got_anchors = w.finish().expect("поток закрыт");
        let got_blob = std::fs::read(&path).expect("идентификаторы прочитаны");

        assert_eq!(got_blob.len(), want_blob.len(), "длина разошлась");
        assert_eq!(got_blob, want_blob, "содержимое разошлось");
        assert_eq!(got_anchors, want_anchors, "опоры разошлись");

        // И читается он тем же разбором — на обоих краях каждой опорной группы,
        // потому что съехавшая опора видна именно там.
        std::fs::write(dir.join("ids.idx"), &got_anchors).unwrap();
        let store = Ids::open(&crate::container::loose(&dir.to_string_lossy()), ids.len());
        for doc in [0usize, 1, 63, 64, 65, 999, 1000, ids.len() / 2, ids.len() - 1] {
            assert_eq!(store.get(doc).as_deref(), Some(ids[doc].as_str()), "документ {doc}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Охрана потолка стоит на пути **потоковой** записи, а не только у `encode`.
    ///
    /// У потока она своя: он считает смещение от начала файла, а не от длины
    /// буфера, и вычеркнуть её можно отдельно. Тогда завёрнутая опора легла бы в
    /// `ids.idx` слитого сегмента — и выдача начала бы приписывать документам
    /// чужие идентификаторы.
    #[test]
    fn the_streaming_anchor_guard_stands_on_the_write_path() {
        let dir = std::env::temp_dir().join("svindb-ids-stream-guard");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ids.bin").to_string_lossy().to_string();
        let ids: Vec<String> = (0..3 * ANCHOR).map(|i| format!("док-{i:09}")).collect();

        // Сперва при настоящем потолке: иначе проверку закрыл бы любой другой
        // отказ записи.
        let mut ok = Writer::create(&path).expect("файл заведён");
        for id in ids.iter() {
            ok.push(id.as_bytes()).expect("идентификатор лёг");
        }
        ok.finish().expect("обычные идентификаторы пишутся потоком");
        assert!(
            std::fs::metadata(&path).expect("файл").len() > 100,
            "вход обязан перерасти опущенный потолок"
        );

        let _low = crate::dict::lower_offset_ceiling(100);
        let mut w = Writer::create(&path).expect("файл заведён");
        let e = match ids.iter().try_for_each(|id| w.push(id.as_bytes())) {
            Ok(()) => panic!("охрана не на пути записи: опора за потолком легла в поток"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        assert!(msg.contains("ids.bin"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("при потолке 100"), "отказ померил не тем потолком: {msg}");
        assert!(msg.contains("flush_docs"), "отказ не подсказал выход: {msg}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Обычный набор идентификаторов отказа не даёт.
    #[test]
    fn an_ordinary_id_column_is_never_refused() {
        let ids: Vec<String> = (0..5_000).map(|i| format!("док-{i:09}")).collect();
        let (blob, anchors) =
            encode(&ids, "seg0007/ids.bin").expect("обычные идентификаторы обязаны собраться");
        assert_eq!(anchors.len(), ids.len().div_ceil(ANCHOR) * 4, "опор не столько");
        assert!(!blob.is_empty());
    }
}

/// Проход по идентификаторам от начала.
pub struct IdWalk<'a> {
    src: &'a [u8],
    at: usize,
    left: usize,
    cur: Vec<u8>,
}

impl<'a> IdWalk<'a> {
    /// Следующий идентификатор. `None` — документы кончились.
    // Не `Iterator`: обход умеет прыгать (`seek`) и отдаёт заимствование,
    // связанное с самим обходчиком. Ни то ни другое в `Iterator` не
    // выражается, а притворяться им — обещать `map`/`collect`, которые
    // на прыжках дали бы неверный ответ.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<&[u8]> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let common = read_varint(self.src, &mut self.at) as usize;
        let tail = read_varint(self.src, &mut self.at) as usize;
        self.cur.truncate(common.min(self.cur.len()));
        let end = (self.at + tail).min(self.src.len());
        self.cur.extend_from_slice(&self.src[self.at..end]);
        self.at = end;
        Some(&self.cur)
    }
}

// ---------------------------------------------------------------------------
// Обратное отображение: идентификатор → номер документа
// ---------------------------------------------------------------------------

/// Идентификаторы, отсортированные и с номерами документов.
///
/// Нужно ровно для удаления и обновления: чтобы погасить документ, надо знать,
/// где он лежит, а `ids.bin` отвечает на обратный вопрос.
///
/// Устройство то же, что у словаря терминов, и это не совпадение — задача та же:
/// отсортированные строки, двоичный поиск, отображение в память. Поэтому здесь
/// переиспользуется его кодирование, а не пишется второе такое же.
pub const PACKED: &str = "idmap.dnum";

pub struct IdMap {
    dict: crate::dict::Dict,
    /// Номер документа для каждой записи, в порядке словаря.
    docs: Region,
    /// Ширина упакованного номера в битах.
    width: u32,
    packed: bool,
    /// Откуда собрать отображение, когда файлов нет.
    ///
    /// Источник целиком, а не имя каталога, и это несущая разница: у
    /// упакованного сегмента `ids.bin` лежит куском общего отображения, и
    /// открыть его по имени каталога значило бы завести **вторую** область на
    /// тот же `seg.dat` — то есть отдать ровно то, ради чего контейнер и заведён.
    src: std::sync::Arc<crate::container::SegmentSource>,
    count: usize,
    /// Собранное по требованию: хеш идентификатора и номер документа, по
    /// возрастанию хеша. Пусто, пока не спросили.
    ///
    /// Двенадцать байт на документ в памяти против семи на диске — и платятся
    /// они только тем сегментом, в который пришло удаление, и только один раз
    /// за жизнь процесса.
    built: std::sync::OnceLock<Vec<(u64, u32)>>,
    /// Открытое `ids.bin` для разрешения совпадений хешей.
    ///
    /// ## Почему это надо было кешировать
    ///
    /// Разрешение читает настоящий идентификатор из `ids.bin`, и прежде каждое
    /// такое чтение открывало хранилище заново: `Ids::open` — это два `open`,
    /// два `mmap`, а на выходе из области два `munmap`. Платилось это на
    /// **каждый идентификатор, прошедший фильтр Блума, на каждом сегменте**.
    ///
    /// На пачке в тысячу удалений по сотне сегментов это тысячи цепочек
    /// отображения и снятия, и большая их часть — на ложных срабатываниях
    /// фильтра, то есть впустую. Файлы при этом неизменяемы, и открывать их
    /// повторно не было причины.
    store: std::sync::OnceLock<Ids>,
    /// Отсечение «здесь такого точно нет».
    bloom: Option<crate::bloom::Bloom>,
}

/// Что отдаёт сборка отображения: словарь, привязки, номера документов и
/// фильтр Блума — четыре куска, которые вызывающий кладёт в сегмент по именам.
/// Имя нужно затем, что четыре одинаковых `Vec<u8>` в подписи не различимы.
type Built = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);

impl IdMap {
    /// Собирает отображение из идентификаторов в порядке номеров.
    ///
    /// Отдаёт словарь, привязки, номера документов и фильтр Блума.
    ///
    /// `dir` — каталог сегмента; идёт только в текст отказа по потолку опор.
    pub fn encode(ids: &[String], dir: &str) -> std::io::Result<Built> {
        let mut pairs: Vec<(&str, u32)> =
            ids.iter().enumerate().map(|(d, s)| (s.as_str(), d as u32)).collect();
        pairs.sort_unstable();
        let keys: Vec<(u32, String)> =
            pairs.iter().map(|(s, _)| (0u32, (*s).to_string())).collect();
        let (blob, anchors) = crate::dict::encode(&keys, &format!("{dir}/idmap.blob"))?;
        // Номера документов **упакованы по ширине**, а не по четыре байта.
        //
        // Ширина лежит первым байтом, как у всех упакованных полос движка, и
        // читается тем же `bits::value_at`.
        let nums: Vec<u32> = pairs.iter().map(|(_, d)| *d).collect();
        let mut docs: Vec<u8> = Vec::with_capacity(nums.len() / 2 + 16);
        let mut bp = 0u64;
        crate::bits::pack_values(&mut docs, &nums, &mut bp);
        // Запас в восемь байт: чтение берёт окно в `u64` вокруг значения, и
        // последнее значение полосы заходит за её границу.
        docs.resize(docs.len() + 8, 0);
        let bloom = crate::bloom::build(ids, crate::bloom::DEFAULT_BITS);
        Ok((blob, anchors, docs, bloom))
    }

    pub fn open(
        src: &std::sync::Arc<crate::container::SegmentSource>,
        count: usize,
        layout: u32,
    ) -> IdMap {
        let (docs, width, packed) = match src.try_open(PACKED) {
            Some(m) if !m.is_empty() => {
                let w = m.u8_at(0) as u32;
                (m, w, true)
            }
            _ => (src.try_open("idmap.docs").unwrap_or_else(Region::empty), 0, false),
        };
        IdMap {
            dict: crate::dict::Dict::open_named(src, "idmap", count, layout),
            docs,
            width,
            packed,
            bloom: crate::bloom::Bloom::open(src, "idmap.bloom"),
            src: std::sync::Arc::clone(src),
            count,
            built: std::sync::OnceLock::new(),
            store: std::sync::OnceLock::new(),
        }
    }

    /// Номер документа по месту в словаре.
    #[inline]
    fn doc_at(&self, at: usize) -> u32 {
        if !self.packed {
            return self.docs.u32_at(at * 4);
        }
        let n = self.docs.len_bytes();
        crate::bits::value_at(self.docs.slice(1, n - 1), self.width, at)
    }

    /// Нечего искать вовсе: у сегмента нет идентификаторов.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Сходится ли отображение само с собой. `Err` — сегмент повреждён.
    ///
    /// Проверять надо именно здесь и именно на открытии, потому что промах тут
    /// стоит дороже неверной выдачи. `find_hashed` читает номер документа через
    /// `u32_at`, а тот за границей отдаёт **ноль**: обрезанный `idmap.docs`
    /// превращает «найди документ X» в «документ 0». Гашение по нему рапортует
    /// «удалено 1», искомый документ остаётся в выдаче, а вместо него пропадает
    /// невиновный — первый документ сегмента. Это потеря на записи, и молчаливая.
    ///
    /// Отсутствие отображения целиком — не порча: у сегмента его может не быть
    /// вовсе, и тогда гашение честно вернёт «не нашлось». Порча — это когда оно
    /// есть, но короче, чем обещает словарь.
    pub fn check(&self, docs: usize) -> Result<(), String> {
        // Отображения на диске нет — проверять нечего: оно собирается из
        // `ids.bin`, а его целостность проверяется отдельно.
        if self.is_empty() || !self.on_disk() {
            return Ok(());
        }
        let (want, name) = if self.packed {
            (crate::bits::window_len(self.dict.len(), self.width) + 1, PACKED)
        } else {
            (self.dict.len() * 4, "idmap.docs")
        };
        if self.docs.len_bytes() < want {
            return Err(format!(
                "{name}: {} байт вместо {want} — на {} идентификаторов словаря",
                self.docs.len_bytes(),
                self.dict.len()
            ));
        }
        if self.dict.len() != docs {
            return Err(format!(
                "idmap: {} идентификаторов при {docs} документах сегмента",
                self.dict.len()
            ));
        }
        Ok(())
    }

    /// Есть ли на диске готовое отображение. Ложь — собираем по требованию.
    #[inline]
    fn on_disk(&self) -> bool {
        !self.dict.is_empty() && !self.docs.is_empty()
    }

    /// Перебирает пары «хеш, номер документа» этого сегмента.
    ///
    /// Нужен каталогу: он складывает эти пары со всех сегментов в один
    /// отсортированный файл. Своего источника у него нет и быть не должно —
    /// иначе он перестал бы быть производным.
    pub fn each(&self, on: &mut dyn FnMut(u64, u32)) {
        for &(h, doc) in self.rebuilt().iter() {
            on(h, doc);
        }
    }

    /// Настоящий идентификатор документа — для сверки.
    ///
    /// Нужен каталогу: тот называет место, а верить ему на слово нельзя. Здесь
    /// же читается то, что лежит по названному месту на самом деле.
    pub fn id_at(&self, doc: u32) -> Option<String> {
        self.ids().get(doc as usize)
    }

    /// Открытое `ids.bin` — один раз на сегмент, а не на каждое обращение.
    ///
    /// Файлы сегмента неизменяемы, поэтому открывать их повторно незачем. Разбор
    /// цены — у поля [`IdMap::store`].
    fn ids(&self) -> &Ids {
        self.store.get_or_init(|| Ids::open(&self.src, self.count))
    }

    /// Отображение, собранное из `ids.bin`.
    ///
    /// ## Почему его нет на диске
    ///
    /// Собрать его заново дешевле, чем возить: `ids.bin` читается подряд, а
    /// хеш и сортировка миллиона пар идут доли секунды. Держится в памяти до
    /// конца жизни процесса, поэтому платит только первое удаление.
    ///
    /// Хранится **хеш**, а не строка: двенадцать байт на документ вместо
    /// сорока. Совпадение хешей разрешается чтением настоящего идентификатора
    /// из `ids.bin` — иначе гашение по чужому хешу убило бы невиновного.
    fn rebuilt(&self) -> &[(u64, u32)] {
        self.built.get_or_init(|| {
            let store = self.ids();
            let mut v: Vec<(u64, u32)> = Vec::with_capacity(self.count);
            let mut w = store.walk();
            let mut doc = 0u32;
            while let Some(id) = w.next() {
                v.push((crate::bloom::hash64(id), doc));
                doc += 1;
            }
            v.sort_unstable();
            v
        })
    }

    /// Все номера с этим хешем — отрезок в собранном отображении.
    fn span(&self, h: u64) -> &[(u64, u32)] {
        let v = self.rebuilt();
        let lo = v.partition_point(|x| x.0 < h);
        let hi = v.partition_point(|x| x.0 <= h);
        &v[lo..hi]
    }

    /// Номер документа по идентификатору.
    pub fn find(&self, id: &str) -> Option<u32> {
        self.find_hashed(id, crate::bloom::hash(id))
    }

    /// То же по готовому хешу — когда один идентификатор ищется по всем
    /// сегментам подряд, хеш считается один раз на все.
    pub fn find_hashed(&self, id: &str, h: u64) -> Option<u32> {
        if let Some(b) = self.bloom.as_ref() {
            if !b.maybe_has_hash(h) {
                return None;
            }
        }
        if !self.on_disk() {
            let store = self.ids();
            return self
                .span(h)
                .iter()
                .filter(|(_, d)| store.get(*d as usize).as_deref() == Some(id))
                .map(|(_, d)| *d)
                .min();
        }
        let at = self.dict.lookup(0, id)?;
        Some(self.doc_at(at as usize))
    }

    /// **Все** номера документов с этим идентификатором — в конец `out`.
    ///
    /// Гашение по одному найденному этого не переживает: `find_hashed` отдаёт
    /// первый по порядку номер, то есть самую старую копию, а она уже мертва —
    /// гашение возвращает «ничего не сделано» и оставляет живую копию жить.
    pub fn find_all(&self, id: &str, h: u64, out: &mut Vec<u32>) {
        if let Some(b) = self.bloom.as_ref() {
            if !b.maybe_has_hash(h) {
                return;
            }
        }
        if !self.on_disk() {
            let store = self.ids();
            let mut hits: Vec<u32> = self
                .span(h)
                .iter()
                .filter(|(_, d)| store.get(*d as usize).as_deref() == Some(id))
                .map(|(_, d)| *d)
                .collect();
            // По возрастанию номера: гашение идёт вперёд по документам, и
            // порядок, зависящий от расположения в хеш-пространстве, был бы
            // невоспроизводим.
            hits.sort_unstable();
            out.extend(hits);
            return;
        }
        let Some((at, n)) = self.dict.lookup_range(0, id) else { return };
        for k in 0..n {
            out.push(self.doc_at((at + k) as usize));
        }
    }

    /// Сколько занимает фильтр. Ноль — фильтра нет.
    pub fn bloom_bytes(&self) -> usize {
        self.bloom.as_ref().map_or(0, |b| b.bytes())
    }

    /// Затягивает отображение в кеш страниц.
    pub fn warm(&self) -> usize {
        self.dict.warm() + self.docs.warm() + self.bloom.as_ref().map_or(0, |b| b.warm())
    }
}

#[cfg(test)]
mod idmap_tests {
    use super::*;
    use std::io::Write;

    fn build(name: &str, ids: &[String]) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-idmap-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let (blob, anchors, docs, bloom) = IdMap::encode(ids, "проверка").expect("влезло");
        let w = |n: &str, b: &[u8]| {
            std::fs::File::create(dir.join(n)).unwrap().write_all(b).unwrap();
        };
        w("idmap.blob", &blob);
        w("idmap.idx", &anchors);
        w(PACKED, &docs);
        w("idmap.bloom", &bloom);
        dir.to_string_lossy().to_string()
    }

    /// Поиск работает, когда отображения на диске нет вовсе.
    ///
    /// Это нынешняя норма: новые сегменты его не пишут, и оно собирается из
    /// `ids.bin`. Проверяется и то, что повторы находятся все, и то, что
    /// чужого не находится — совпадение хеша разрешается чтением настоящего
    /// идентификатора, иначе гашение убило бы невиновного.
    #[test]
    fn it_finds_without_any_map_on_disk() {
        let dir = std::env::temp_dir().join("svindb-idmap-lazy");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        // Идентификаторы в порядке приёма, с повтором: замена оставляет обе
        // копии в одном сегменте.
        let mut ids: Vec<String> = (0..4000).map(|i| format!("канал:{i:06}")).collect();
        ids[1500] = "канал:000007".to_string();
        let (blob, anchors) = encode(&ids, "проверка/ids.bin").expect("влезло");
        let w = |n: &str, b: &[u8]| {
            std::fs::File::create(dir.join(n)).unwrap().write_all(b).unwrap();
        };
        w("ids.bin", &blob);
        w("ids.idx", &anchors);
        // Только фильтр — как пишет сегмент сегодня.
        w("idmap.bloom", &crate::bloom::build(&ids, crate::bloom::DEFAULT_BITS));
        let d = dir.to_string_lossy().to_string();
        assert!(!dir.join(PACKED).exists() && !dir.join("idmap.blob").exists());

        let map = IdMap::open(
            &std::sync::Arc::new(crate::container::loose(&d)),
            ids.len(),
            crate::index::LAYOUT,
        );
        assert!(!map.is_empty(), "сегмент с идентификаторами объявлен пустым");
        assert!(map.check(ids.len()).is_ok());
        // Каждый одиночный находится и отдаёт свой номер.
        for doc in [0usize, 1, 63, 64, 3999] {
            assert_eq!(map.find(&ids[doc]), Some(doc as u32), "{}", ids[doc]);
        }
        // Обе копии повтора — и в порядке возрастания номера.
        let mut out = Vec::new();
        map.find_all("канал:000007", crate::bloom::hash("канал:000007"), &mut out);
        assert_eq!(out, vec![7, 1500], "повтор найден не весь");
        // Чужого нет.
        assert_eq!(map.find("канал:999999"), None);
        assert_eq!(map.find("совсем другое"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Сегмент отображается один раз на все поиски, а не один раз на поиск.
    ///
    /// Разрешение совпадения хеша читает настоящий идентификатор из `ids.bin`, и
    /// путь без отображения на диске — не редкость, а **норма**: сегменты пишут
    /// только фильтр. Открывать неизменяемый файл заново на каждый вызов значит
    /// платить два `mmap` и два `munmap` за каждый идентификатор, прошедший
    /// фильтр, на каждом сегменте; пачка удалений разносит это на всё их
    /// произведение.
    ///
    /// Считается **накопленное** число областей, а не живое. Живое поломку не
    /// ловит вовсе: лишние области снимались тут же, по выходу из вызова, и
    /// `live_maps()` до и после цикла показывал одно и то же число. Запас взят с
    /// перевесом, потому что счётчик общий на процесс и соседние тесты
    /// отображают своё: поломка добавила бы сорок тысяч областей, соседям до
    /// такого далеко.
    #[test]
    fn searching_maps_the_segment_once() {
        let dir = std::env::temp_dir().join("svindb-idmap-remap");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let ids: Vec<String> = (0..4000).map(|i| format!("канал:{i:06}")).collect();
        let (blob, anchors) = encode(&ids, "проверка/ids.bin").expect("влезло");
        let w = |n: &str, b: &[u8]| {
            std::fs::File::create(dir.join(n)).unwrap().write_all(b).unwrap();
        };
        w("ids.bin", &blob);
        w("ids.idx", &anchors);
        w("idmap.bloom", &crate::bloom::build(&ids, crate::bloom::DEFAULT_BITS));
        let d = dir.to_string_lossy().to_string();

        let map = IdMap::open(
            &std::sync::Arc::new(crate::container::loose(&d)),
            ids.len(),
            crate::index::LAYOUT,
        );
        assert!(!map.on_disk(), "отображения на диске быть не должно — иначе тест мимо");
        // Первый поиск заводит кеш: его области законны и остаются жить.
        assert_eq!(map.find(&ids[0]), Some(0));

        let before = crate::mmap::maps_made();
        for (doc, id) in ids.iter().enumerate().cycle().take(20_000) {
            assert_eq!(map.find(id), Some(doc as u32), "{id}");
        }
        let after = crate::mmap::maps_made();
        assert!(
            after < before + 10_000,
            "двадцать тысяч поисков завели {} областей — сегмент отображается заново",
            after - before
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_packed_band_is_smaller_than_four_bytes_each() {
        for n in [1000usize, 100_000, 500_000] {
            let ids: Vec<String> = (0..n).map(|i| format!("док-{i:07}")).collect();
            let (_, _, docs, _) = IdMap::encode(&ids, "проверка").expect("влезло");
            let plain = n * 4;
            let want = crate::bits::window_len(n, 32 - ((n - 1) as u32).leading_zeros()) + 1;
            assert_eq!(docs.len(), want, "{n}: длина полосы");
            assert!(
                docs.len() * 100 < plain * 80,
                "{n}: упакованная {} против прежней {plain} — меньше пятой части не сняли",
                docs.len()
            );
        }
    }

    #[test]
    fn the_old_four_byte_band_still_reads() {
        let ids: Vec<String> = (0..3000).map(|i| format!("док-{i:07}")).collect();
        let dir = std::env::temp_dir().join("svindb-idmap-old");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let (blob, anchors, _, bloom) = IdMap::encode(&ids, "проверка").expect("влезло");
        let mut pairs: Vec<(&str, u32)> =
            ids.iter().enumerate().map(|(d, s)| (s.as_str(), d as u32)).collect();
        pairs.sort_unstable();
        let plain: Vec<u8> = pairs.iter().flat_map(|(_, d)| d.to_le_bytes()).collect();
        let w = |n: &str, b: &[u8]| {
            std::fs::File::create(dir.join(n)).unwrap().write_all(b).unwrap();
        };
        w("idmap.blob", &blob);
        w("idmap.idx", &anchors);
        w("idmap.docs", &plain);
        w("idmap.bloom", &bloom);
        let d = dir.to_string_lossy().to_string();
        let map = IdMap::open(
            &std::sync::Arc::new(crate::container::loose(&d)),
            ids.len(),
            crate::index::LAYOUT,
        );
        assert!(map.check(ids.len()).is_ok(), "прежняя полоса не прошла проверку");
        for (doc, id) in ids.iter().enumerate() {
            assert_eq!(map.find(id), Some(doc as u32), "{id}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn finds_every_document() {
        let ids: Vec<String> = (0..5000).map(|i| format!("док-{i:07}")).collect();
        let dir = build("plain", &ids);
        let map = IdMap::open(
            &std::sync::Arc::new(crate::container::loose(&dir)),
            ids.len(),
            crate::index::LAYOUT,
        );
        for (doc, id) in ids.iter().enumerate() {
            assert_eq!(map.find(id), Some(doc as u32), "{id}");
        }
        assert_eq!(map.find("такого нет"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Все копии идентификатора, а не первая по порядку.
    ///
    /// Повторы ставятся в трёх местах намеренно: внутри группы опоры, ровно на
    /// её границе и с разбегом через несколько групп. Опоры стоят через 64
    /// слова, и поиск группы отступает на одну назад — проверяется, что этого
    /// отступа хватает.
    #[test]
    fn finds_all_copies_of_one_id() {
        let mut ids: Vec<String> = (0..300).map(|i| format!("док-{i:05}")).collect();
        // Три копии подряд в середине группы, две — через границу опоры.
        ids[10] = "повтор-а".into();
        ids[11] = "повтор-а".into();
        ids[12] = "повтор-а".into();
        ids[63] = "повтор-б".into();
        ids[64] = "повтор-б".into();
        ids[200] = "повтор-в".into();
        let dir = build("dups", &ids);
        let map = IdMap::open(
            &std::sync::Arc::new(crate::container::loose(&dir)),
            ids.len(),
            crate::index::LAYOUT,
        );

        let all = |id: &str| {
            let mut out = Vec::new();
            map.find_all(id, crate::bloom::hash(id), &mut out);
            out.sort_unstable();
            out
        };
        // Номера идут по порядку сортировки идентификаторов, а он не совпадает
        // с порядком документов — потому и сравниваем отсортированное.
        assert_eq!(all("повтор-а"), vec![10, 11, 12]);
        assert_eq!(all("повтор-б"), vec![63, 64]);
        assert_eq!(all("повтор-в"), vec![200]);
        assert!(all("такого нет").is_empty());
        // Одиночные слова остаются одиночными.
        assert_eq!(all("док-00000"), vec![0]);
        assert_eq!(all("док-00299"), vec![299]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn works_without_a_filter_file() {
        let ids: Vec<String> = (0..3000).map(|i| format!("док-{i:06}")).collect();
        let dir = build("nofilter", &ids);
        std::fs::remove_file(format!("{dir}/idmap.bloom")).unwrap();
        let map = IdMap::open(
            &std::sync::Arc::new(crate::container::loose(&dir)),
            ids.len(),
            crate::index::LAYOUT,
        );
        assert_eq!(map.bloom_bytes(), 0, "фильтра нет, и это не ошибка");
        for (doc, id) in ids.iter().enumerate() {
            assert_eq!(map.find(id), Some(doc as u32), "{id}");
        }
        assert_eq!(map.find("такого нет"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Фильтр не должен отсечь ни одного существующего документа.
    ///
    /// Отдельно от проверки самого фильтра: здесь важно, что он подключён к
    /// поиску по идентификатору правильной стороной. Перепутать «нет» и «да»
    /// местами — ровно та ошибка, которая проходит все тесты на скорость и
    /// молча теряет удаления.
    #[test]
    fn the_filter_never_hides_an_existing_document() {
        let ids: Vec<String> = (0..20_000).map(|i| format!("{}-{i}", i * 7919)).collect();
        let dir = build("filtered", &ids);
        let map = IdMap::open(
            &std::sync::Arc::new(crate::container::loose(&dir)),
            ids.len(),
            crate::index::LAYOUT,
        );
        assert!(map.bloom_bytes() > 0, "фильтр обязан быть");
        for (doc, id) in ids.iter().enumerate() {
            assert_eq!(map.find(id), Some(doc as u32), "фильтр спрятал {id}");
        }
        // И наоборот: почти всё несуществующее должно отсекаться до словаря.
        let misses = (0..20_000).filter(|i| map.find(&format!("нет-{i}")).is_none()).count();
        assert_eq!(misses, 20_000);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Порядок приёма не совпадает с алфавитным — на этом и стоит вся затея.
    #[test]
    fn survives_arbitrary_order() {
        let ids: Vec<String> = vec![
            "zzz".into(),
            "aaa".into(),
            "mmm".into(),
            "".into(),
            "документ".into(),
            "a".into(),
        ];
        let dir = build("order", &ids);
        let map = IdMap::open(
            &std::sync::Arc::new(crate::container::loose(&dir)),
            ids.len(),
            crate::index::LAYOUT,
        );
        for (doc, id) in ids.iter().enumerate() {
            assert_eq!(map.find(id), Some(doc as u32), "{id:?}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
