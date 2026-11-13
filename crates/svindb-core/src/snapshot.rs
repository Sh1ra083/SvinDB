//! Снимок индекса: согласованный слепок в архив, из которого его можно поднять.

use crate::codec::Effort;
use crate::json::{self, Json};
use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::Path;

/// Имя оглавления. Признак того, что снимок дописан.
pub const FILE: &str = "snapshot.json";
pub const FORMAT: u32 = 1;
/// Общие файлы логического контракта индекса.
///
/// Одних сегментов и schema недостаточно: без settings после restore меняются
/// strictness/лимиты, а без compiled Elasticsearch mapping исчезают
/// normalizer, ignore_above, constant_keyword, scaled_float и dynamic rules.
const SHARED_FILES: &[&str] = &["schema.json", "settings.json", "mapping.json"];
/// Кусок, которым файл подаётся на сжатие.
///
/// Мелкий кусок выигрывает дважды: у каждого участка своя таблица Хаффмана, и
/// значений выходит больше, отчего обученный словарь берётся крупнее. Проигрыш
/// — длинные совпадения через всё окно, но на файлах индекса их мало: они уже
/// сжаты своими способами.
const PIECE: usize = 16 << 10;

/// Размеры, которые перебираются целиком на файлах не больше [`WHOLE`].
const PIECES: [usize; 4] = [16 << 10, 64 << 10, 256 << 10, 4 << 20];

/// До какого размера файл жмётся всеми способами, а не одним.
///
/// Восемь мегабайт — та граница, за которой из файлов индекса остаются лишь
/// `postings.bin`, где 16 КиБ и так лучший, и заведомо несжимаемые
/// `message.sbin` с `idmap.bloom`.
///
/// Она же — граница между двумя путями укладки члена: до неё член читается
/// целиком, за ней идёт потоком (см. [`put_streamed`]). Порог годится и на это,
/// потому что смысл у него один — «файл, который не жалко подержать в памяти».
const WHOLE: usize = 8 << 20;
const MAX_INFO_BYTES: u64 = 64 << 20;

/// Сколько байт члена читается за один вызов ядра.
///
/// Кратно [`PIECE`]: буфер нарезается ровно на куски, и неполный кусок
/// случается только в конце файла — там же, где он случился бы у среза.
const READ_BUF: usize = 1 << 20;
const _: () = assert!(READ_BUF.is_multiple_of(PIECE) && READ_BUF >= PIECE);

/// Сколько байт одного куска идёт в выборку под словарь.
const SAMPLE_PIECE: usize = 256;

/// Сколько кусков идёт на обучение таблиц.
const TRAIN_PARTS: usize = 512;

/// Действующий покрой укладки: порог чтения целиком и число обучающих кусков.
///
/// В выпуске это [`WHOLE`] и [`TRAIN_PARTS`] и ничего больше — вызов
/// подставляется целиком и от пары постоянных не отличается ни байтом.
///
/// ## Зачем шов
///
/// Потоковый путь начинается за восемью мегабайтами, а обучающие куски занимают
/// ещё восемь. Проверить на таком входе можно ровно одно — что путь вообще
/// работает; за всё остальное (совпадение с укладкой через память, независимость
/// занятого от размера члена) пришлось бы платить десятками мегабайт сжатия на
/// каждую проверку, а отладочная сборка жмёт около мегабайта в секунду.
/// Опущенный покрой даёт то же самое на входе в четверть мегабайта.
///
/// Ячейка потоко-местная, а не общая: проверки идут параллельно, и опущенный
/// одной порог отправил бы потоком то, что соседняя ждёт прочтённым целиком.
/// Тот же приём и по той же причине стоит у [`crate::store::carry_floor`].
#[cfg(not(test))]
#[inline(always)]
fn cut() -> (usize, usize) {
    (WHOLE, TRAIN_PARTS)
}

#[cfg(test)]
thread_local! {
    static CUT: std::cell::Cell<(usize, usize)> =
        const { std::cell::Cell::new((WHOLE, TRAIN_PARTS)) };
}

#[cfg(test)]
fn cut() -> (usize, usize) {
    CUT.with(|c| c.get())
}

/// Опущенный покрой держится, пока жив ответ [`lower_cut`].
///
/// Возврат прежнего обязателен: libtest переиспользует потоки между проверками,
/// и оставленный на полу порог отправил бы потоком члены следующей.
#[cfg(test)]
struct LoweredCut((usize, usize));

#[cfg(test)]
impl Drop for LoweredCut {
    fn drop(&mut self) {
        CUT.with(|c| c.set(self.0));
    }
}

#[cfg(test)]
#[must_use = "покрой вернётся на место тут же, если ответ уронить"]
fn lower_cut(whole: usize, parts: usize) -> LoweredCut {
    LoweredCut(CUT.with(|c| c.replace((whole, parts))))
}

/// Член архива: один файл индекса.
pub struct Member {
    /// Путь внутри индекса: `mrg0000/norms.bin` или `schema.json`.
    pub path: String,
    pub len: u64,
    pub stored: u64,
    pub crc: u32,
    /// Ложь — файл лёг как есть, сжатие не окупилось.
    pub packed: bool,
}

/// Оглавление снимка.
pub struct Info {
    pub name: String,
    pub format: u32,
    /// Секунды эпохи. Только для человека: порядок задаёт не время, а имя.
    pub taken: u64,
    /// Номер раскладки индекса — по нему восстановление откажет чужому движку.
    pub layout: u32,
    pub docs: u64,
    pub segments: Vec<String>,
    pub members: Vec<Member>,
}

impl Info {
    pub fn total_len(&self) -> u64 {
        self.members.iter().fold(0u64, |sum, m| sum.saturating_add(m.len))
    }

    pub fn total_stored(&self) -> u64 {
        self.members.iter().fold(0u64, |sum, m| sum.saturating_add(m.stored))
    }

    pub fn to_json(&self) -> Json {
        let members: Vec<Json> = self
            .members
            .iter()
            .map(|m| {
                json::obj(vec![
                    ("path", json::s(&m.path)),
                    ("len", Json::Int(m.len as i64)),
                    ("stored", Json::Int(m.stored as i64)),
                    ("crc", Json::Int(m.crc as i64)),
                    ("packed", Json::Bool(m.packed)),
                ])
            })
            .collect();
        json::obj(vec![
            ("format", Json::Int(self.format as i64)),
            ("name", json::s(&self.name)),
            ("taken", Json::Int(self.taken as i64)),
            ("layout", Json::Int(self.layout as i64)),
            ("docs", Json::Int(self.docs as i64)),
            ("segments", Json::Arr(self.segments.iter().map(|s| json::s(s)).collect())),
            ("members", Json::Arr(members)),
        ])
    }

    fn from_json(v: &Json) -> Result<Info, String> {
        let need = |k: &str| v.get(k).ok_or_else(|| format!("нет ключа {k:?}"));
        let unsigned = |value: &Json, what: &str| -> Result<u64, String> {
            let n = value.as_i64().ok_or_else(|| format!("{what} не целое"))?;
            u64::try_from(n).map_err(|_| format!("{what} отрицательное"))
        };
        let small = |value: &Json, what: &str| -> Result<u32, String> {
            u32::try_from(unsigned(value, what)?).map_err(|_| format!("{what} больше {}", u32::MAX))
        };
        let format = small(need("format")?, "format")?;
        if format != FORMAT {
            return Err(format!(
                "раскладка архива {format}, а читается {FORMAT} — снимок снят другой версией"
            ));
        }
        let name = need("name")?.as_str().unwrap_or("").to_string();
        valid_name(&name).map_err(|e| format!("name: {e}"))?;
        let mut segments = Vec::new();
        let mut segment_set = HashSet::new();
        for value in need("segments")?.as_arr().ok_or("segments не массив")? {
            let name = value.as_str().ok_or("имя сегмента не строка")?.to_string();
            if !crate::manifest::valid_segment_name(&name) {
                return Err(format!("негодное имя сегмента {name:?}"));
            }
            if !segment_set.insert(name.clone()) {
                return Err(format!("сегмент {name:?} назван дважды"));
            }
            segments.push(name);
        }
        let raw_members = need("members")?.as_arr().ok_or("members не массив")?;
        if raw_members.len() > 1_000_000 {
            return Err("в snapshot больше миллиона членов".into());
        }
        let mut members = Vec::with_capacity(raw_members.len());
        let mut member_paths = HashSet::new();
        let mut manifests = 0usize;
        let mut total_len = 0u64;
        let mut total_stored = 0u64;
        for m in raw_members {
            let g = |k: &str| m.get(k).ok_or_else(|| format!("у члена нет {k:?}"));
            let path = g("path")?.as_str().ok_or("path не строка")?.to_string();
            valid_member_path(&path, &segment_set)?;
            if !member_paths.insert(path.clone()) {
                return Err(format!("член {path:?} назван дважды"));
            }
            if path == crate::manifest::FILE {
                manifests += 1;
            }
            let len = unsigned(g("len")?, "len")?;
            let stored = unsigned(g("stored")?, "stored")?;
            let packed = g("packed")?.as_bool().ok_or("packed не логическое")?;
            if !packed && stored != len {
                return Err(format!("несжатый член {path:?}: stored {stored} не равен len {len}"));
            }
            total_len = total_len
                .checked_add(len)
                .ok_or("суммарная распакованная длина snapshot переполнена")?;
            total_stored = total_stored
                .checked_add(stored)
                .ok_or("суммарная хранимая длина snapshot переполнена")?;
            members.push(Member { path, len, stored, crc: small(g("crc")?, "crc")?, packed });
        }
        if manifests != 1 {
            return Err(format!(
                "в snapshot должен быть ровно один {}, найдено {manifests}",
                crate::manifest::FILE
            ));
        }
        Ok(Info {
            name,
            format,
            taken: unsigned(need("taken")?, "taken")?,
            layout: small(need("layout")?, "layout")?,
            docs: unsigned(need("docs")?, "docs")?,
            segments,
            members,
        })
    }
}

fn valid_member_file(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('.')
        && name.chars().all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

fn valid_member_path(path: &str, segments: &HashSet<String>) -> Result<(), String> {
    let parts = crate::fs::relative_components(path).map_err(|e| format!("член {path:?}: {e}"))?;
    match parts.as_slice() {
        [file]
            if file == crate::manifest::FILE
                || SHARED_FILES.iter().any(|allowed| file == allowed) =>
        {
            Ok(())
        }
        [segment, file]
            if segments.contains(segment)
                && crate::manifest::valid_segment_name(segment)
                && valid_member_file(file) =>
        {
            Ok(())
        }
        _ => Err(format!("член {path:?} не входит в разрешённую раскладку snapshot")),
    }
}

/// Годится ли имя снимка.
///
/// Имя становится именем каталога, поэтому правила те же, что у имени поля:
/// буквы, цифры, `_`, `-`, точка. Косая черта и `..` запрещены — иначе
/// `POST /_snapshot/../../etc` писал бы куда попало, а проверка приёмника по
/// спискам обходилась бы одним запросом.
pub fn valid_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 128 {
        return Err("имя снимка пустое или длиннее ста двадцати восьми байт".into());
    }
    if name.starts_with('.') {
        return Err("имя снимка начинается с точки".into());
    }
    for c in name.chars() {
        if !(c.is_alphanumeric() || c == '_' || c == '-' || c == '.') {
            return Err(format!(
                "имя снимка {name:?}: символ {c:?} не годится — имя становится именем каталога"
            ));
        }
    }
    Ok(())
}

/// Читает оглавление снимка. `None` — снимок не дописан.
pub fn read(dir: &str) -> Option<Info> {
    read_checked(dir).ok()
}

/// То же, но с причиной отказа.
///
/// Нужно восстановлению, и вот почему. `read` отвечает `None` на всё сразу —
/// файла нет, файл не в UTF-8, член назван дважды, путь члена выходит из корня.
/// Восстановление на это отвечало «нет snapshot.json — снимок не дописан либо
/// это не снимок», и человек шёл искать пропажу файла, который лежит на месте и
/// прекрасно читается.
///
/// Списку снимков (`list`) причина, наоборот, не нужна: каталог без годного
/// оглавления он пропускает молча, и это верно — там перебираются чужие
/// каталоги, а не открывается названный.
pub fn read_checked(dir: &str) -> Result<Info, String> {
    let path = Path::new(dir).join(FILE);
    let bytes = crate::fs::read_regular_limited(&path, MAX_INFO_BYTES, false)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let text =
        std::str::from_utf8(&bytes).map_err(|e| format!("{}: не в UTF-8: {e}", path.display()))?;
    let parsed = json::parse(text).map_err(|e| format!("{}: {e}", path.display()))?;
    Info::from_json(&parsed).map_err(|e| format!("{}: {e}", path.display()))
}

/// Снимки в приёмнике, по возрастанию имени.
///
/// Каталог без оглавления пропускается молча: это либо снимок, который пишется
/// прямо сейчас, либо остаток прерванного. Ни то ни другое не ошибка приёмника.
pub fn list(dest: &str) -> Vec<Info> {
    let Ok(it) = std::fs::read_dir(dest) else { return Vec::new() };
    let mut out: Vec<Info> = it
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter_map(|e| read(&e.path().to_string_lossy()))
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Список сегментов для архива — из захваченного состава, а не с диска.
///
/// Раскладка та же, что у [`crate::manifest::write`]: имя каталога на строку,
/// порядок несущий. Читать файл повторно нельзя, см. шапку модуля.
fn manifest_bytes(segments: &[String]) -> Vec<u8> {
    let mut out = String::new();
    for s in segments {
        out.push_str(&crate::manifest::name_of(s));
        out.push('\n');
    }
    out.into_bytes()
}

/// Файлы индекса, которые входят в снимок.
///
/// Списка сегментов здесь нет намеренно: он не копируется, а составляется
/// [`manifest_bytes`] из того же состава, по которому построен этот план.
///
/// Порядок несущий: карта живых идёт **последней**, см. шапку модуля.
fn plan(index: &str, segments: &[String]) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for file in SHARED_FILES {
        if std::path::Path::new(index).join(file).exists() {
            out.push((*file).to_string());
        }
    }
    let mut live: Vec<String> = Vec::new();
    // `manifest::segments` отдаёт **полные пути**, а внутри архива нужны имена:
    // снимок обязан подняться в любой каталог, а не только в тот, откуда снят.
    for full in segments {
        let seg = crate::manifest::name_of(full);
        // Непрочитанный каталог — отказ, а не пропуск. Держатель взят, значит
        // пропасть сегменту неоткуда, и если он всё-таки пропал, то молчаливый
        // `continue` дал бы худшее из возможного: архив без сегмента, который
        // назван в его собственном списке, — то есть снимок, выглядящий целым и
        // не поднимающийся.
        let it = std::fs::read_dir(full).map_err(|e| {
            format!("{seg}: каталог сегмента не перечислился: {e} — снимок был бы неполон")
        })?;
        let mut names = Vec::new();
        for entry in it {
            let entry = entry.map_err(|e| format!("{seg}: член не перечислился: {e}"))?;
            if !entry
                .file_type()
                .map_err(|e| format!("{seg}: тип члена не прочёлся: {e}"))?
                .is_file()
            {
                return Err(format!(
                    "{seg}/{}: snapshot принимает только обычные файлы",
                    entry.file_name().to_string_lossy()
                ));
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if !valid_member_file(&name) {
                return Err(format!("{seg}/{name}: негодное имя файла snapshot"));
            }
            names.push(name);
        }
        names.sort();
        for n in names {
            let rel = format!("{seg}/{n}");
            if n == crate::live::FILE {
                live.push(rel);
            } else {
                out.push(rel);
            }
        }
    }
    out.extend(live);
    Ok(out)
}

/// Кладёт один файл в архив. Отдаёт член оглавления.
/// Жмёт члены **по нескольку сразу**.
///
/// Каждому члену при этом отдаётся **один** поток. Иначе потоки перемножаются:
/// восемь членов по восемь потоков — шестьдесят четыре на восьми ядрах, и они
/// начинают мешать друг другу больше, чем помогают.
///
/// Отказ первого же члена — отказ всего: снимок без файла выглядел бы целым и
/// не был им.
fn squeeze_all(
    index: &str,
    out_dir: &str,
    plan: &[String],
    effort: Effort,
) -> Result<Vec<Member>, String> {
    let threads =
        std::thread::available_parallelism().map_or(1, |n| n.get()).min(plan.len().max(1));
    let next = std::sync::atomic::AtomicUsize::new(0);
    let done: Vec<std::sync::Mutex<Option<Result<Member, String>>>> =
        (0..plan.len()).map(|_| std::sync::Mutex::new(None)).collect();
    std::thread::scope(|sc| {
        for _ in 0..threads {
            sc.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(rel) = plan.get(i) else { return };
                let got = put(index, out_dir, rel, effort).map_err(|e| format!("{rel}: {e}"));
                *done[i].lock().expect("замок члена") = Some(got);
            });
        }
    });
    let mut members = Vec::with_capacity(plan.len());
    for slot in done {
        // Порядок сохраняется: карта живых обязана лечь последней, а по номерам
        // членов потом сверяется целость.
        members.push(slot.into_inner().expect("замок члена").expect("член не обработан")?);
    }
    Ok(members)
}

/// Жмёт файл; мелкий — всеми размерами куска, оставляя лучший.
///
/// Перебор для мелких не расточительство, а единственный способ узнать ответ:
/// он зависит от того, насколько файл однороден, а этого ни по имени, ни по
/// выборке не видно.
///
/// Отказ пропускается наружу, а не гасится в `None`: `store::build_with`
/// отказывает, когда засечка куска не влезает в четыре байта, и `None` на этом
/// месте означал бы «сжимать нечего» — член лёг бы в архив несжатым и молча.
fn squeeze(src: &[u8], effort: Effort, rel: &str) -> std::io::Result<Option<crate::store::Built>> {
    let pack = |n: usize| {
        crate::store::build_with(
            |emit| {
                for part in src.chunks(n) {
                    emit(part);
                }
            },
            effort,
            1,
            rel,
        )
    };
    let cost = |b: &crate::store::Built| b.data.len() + b.marks.len() + b.dict.len();
    if src.len() > cut().0 || matches!(effort, Effort::Fast) {
        return pack(PIECE);
    }
    let mut best: Option<crate::store::Built> = None;
    for n in PIECES {
        if let Some(b) = pack(n)? {
            if best.as_ref().is_none_or(|x| cost(&b) < cost(x)) {
                best = Some(b);
            }
        }
    }
    Ok(best)
}

fn bent(msg: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

/// Кладёт файл индекса в архив.
///
/// Мелкий читается целиком, крупный идёт потоком — разбор границы у [`WHOLE`] и
/// у [`put_streamed`].
fn put(index: &str, out_dir: &str, rel: &str, effort: Effort) -> std::io::Result<Member> {
    let src = Path::new(index).join(rel);
    // Длина спрашивается **до** чтения: от неё и зависит, читать ли вообще.
    // Открытие то же самое, каким читал бы `read_regular`, — жёсткая ссылка на
    // чужой inode отвергается здесь, а не после того, как файл прочтён.
    let len = crate::fs::open_private_regular(&src)?.metadata()?.len();
    if len <= cut().0 as u64 {
        return put_bytes(out_dir, rel, &crate::fs::read_regular(&src, true)?, effort);
    }
    put_streamed(out_dir, rel, &src, len, effort)
}

/// Проходит файл кусками постоянного размера. Отдаёт, сколько байт прочтено.
///
/// Куски ровно по [`PIECE`] байт, последний — сколько осталось: ровно та же
/// нарезка, какую дал бы `src.chunks(PIECE)` на прочитанном целиком файле. На
/// этом стоит совпадение двух путей укладки члена, см. [`put_streamed`].
fn scan(path: &Path, mut each: impl FnMut(&[u8]) -> std::io::Result<()>) -> std::io::Result<u64> {
    let mut file = crate::fs::open_private_regular(path)?;
    let mut buf = vec![0u8; READ_BUF];
    let mut read = 0u64;
    loop {
        // Буфер набирается до конца, а не до первого короткого чтения: иначе
        // кусок вышел бы короче [`PIECE`] посреди файла, и нарезка разошлась бы
        // с той, которую даёт срез.
        let mut filled = 0usize;
        while filled < buf.len() {
            let n = file.read(&mut buf[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        if filled == 0 {
            return Ok(read);
        }
        for part in buf[..filled].chunks(PIECE) {
            each(part)?;
        }
        read = read
            .checked_add(filled as u64)
            .ok_or_else(|| bent(format!("{}: длина члена переполнена", path.display())))?;
        if filled < buf.len() {
            return Ok(read);
        }
    }
}

/// Выборка под словарь, набираемая по ходу чтения.
///
/// Повторяет первый проход [`crate::store::build_with`] дословно — прореживание,
/// потолок выборки, число обучающих кусков. Не из любви к повторению: потоковый
/// путь обязан класть **тот же** член, что и путь через память, иначе архив
/// зависел бы от того, каким путём его собрали. Сторожем стоит
/// `tests::the_streamed_member_matches_the_one_built_in_memory`.
struct Training {
    /// Объём колонки со служебными длинами — по нему берётся размер словаря.
    total: usize,
    pieces: Vec<Vec<u8>>,
    parts: Vec<Vec<u8>>,
    buf: Vec<u8>,
    /// Сколько кусков пропускать между взятыми и сколько уже пропущено.
    stride: usize,
    since: usize,
}

impl Training {
    fn new() -> Training {
        Training {
            total: 0,
            pieces: Vec::new(),
            parts: Vec::new(),
            buf: Vec::new(),
            stride: 1,
            since: 0,
        }
    }

    fn feed(&mut self, v: &[u8]) {
        self.total = self.total.saturating_add(v.len() + 4);
        if self.since == 0 {
            let take = v.len().min(SAMPLE_PIECE);
            if take > 0 {
                self.pieces.push(v[..take].to_vec());
            }
            // Набралось столько, сколько словарю всё равно не понадобится, —
            // каждый второй выбрасывается, шаг удваивается. Так выборка
            // остаётся растянутой на весь член при любой его длине, а длину
            // знать не нужно.
            if self.pieces.len() >= crate::store::DICT_MAX / SAMPLE_PIECE {
                let mut keep = Vec::with_capacity(self.pieces.len() / 2 + 1);
                for (i, p) in self.pieces.drain(..).enumerate() {
                    if i % 2 == 0 {
                        keep.push(p);
                    }
                }
                self.pieces = keep;
                self.stride *= 2;
            }
        }
        self.since = (self.since + 1) % self.stride;
        // Обучающие куски — те же данные, но в форме куска: таблицы обучаются
        // на том, что ими потом кодируется.
        if self.parts.len() < cut().1 {
            self.buf.extend((v.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(v);
            if self.buf.len() >= crate::store::BLOCK {
                self.parts.push(std::mem::take(&mut self.buf));
            }
        }
    }

    /// Обучает словарь и отпускает выборку.
    ///
    /// Берётся по значению, чтобы выборка и обучающие куски — а это единственное
    /// крупное, что держит проход, — освободились сразу, а не дожили до конца
    /// сжатия.
    fn dict(mut self, effort: Effort) -> Option<crate::codec::pack::EncoderDict> {
        if self.total == 0 {
            return None;
        }
        if !self.buf.is_empty() && self.parts.is_empty() {
            self.parts.push(std::mem::take(&mut self.buf));
        }
        let want = crate::store::dict_size(self.total).min(self.total);
        let mut sample: Vec<u8> = Vec::with_capacity(want);
        for p in self.pieces.iter() {
            if sample.len() >= want {
                break;
            }
            let take = (want - sample.len()).min(p.len());
            sample.extend_from_slice(&p[..take]);
        }
        let refs: Vec<&[u8]> = self.parts.iter().map(|p| p.as_slice()).collect();
        crate::codec::pack::EncoderDict::train_with(sample, &refs, effort)
    }
}

/// Кладёт крупный член потоком, не держа его в памяти целиком ни разу.
///
/// ## Почему потоково
///
/// Член архива перестал быть мелким файлом. С появлением контейнера весь
/// сегмент — это один `seg.dat`, до четырёх гигабайт; прежний путь читал член в
/// `Vec` и собирал сжатое тоже в `Vec`, а члены жмутся **по нескольку сразу**
/// ([`squeeze_all`]). Снимок стоил бы «член плюс сжатое, умножить на число
/// потоков» и на первом же крупном сегменте съел бы память машины — притом что
/// работа тут чисто проходная: прочёл кусок, посчитал сумму, сжал, отдал файлу.
///
/// Здесь по члену проходят кусками постоянного размера. Сумма считается по ходу
/// ([`scan`]), тела кусков уходят в `.sbin` через [`crate::store::Transfer`] и на
/// куче не задерживаются. Занятое перестаёт зависеть от размера члена: остаются
/// словарь, выборка под него и засечки — десятки байт на кусок.
///
/// ## Чем платим
///
/// Проходов по файлу три вместо одного: выборка под словарь, сжатие и — если
/// сжатие не окупилось — укладка как есть. Решение «окупилось ли» принимается
/// тем же правилом, что и раньше, а узнать его заранее нечем: у члена, который
/// целиком лежит в памяти, лишний проход бесплатен, у потокового он стоит
/// чтения. Это и есть цена того, что снимок перестал зависеть от размера
/// сегмента.
fn put_streamed(
    out_dir: &str,
    rel: &str,
    src: &Path,
    len: u64,
    effort: Effort,
) -> std::io::Result<Member> {
    let dst = format!("{out_dir}/{rel}");
    // `Transfer` складывает путь как `{каталог}/{поле}.sbin`, поэтому член
    // разбирается на каталог и имя — иначе вышло бы `…/seg.dat/.sbin`. Ровно
    // так же его разбирает обратно `restore_member`.
    let whole = Path::new(&dst);
    let (dir, stem) =
        match (whole.parent().and_then(|x| x.to_str()), whole.file_name().and_then(|x| x.to_str()))
        {
            (Some(dir), Some(stem)) if !dir.is_empty() => (dir, stem),
            _ => return Err(bent(format!("{dst}: у члена нет каталога или имени"))),
        };
    crate::fs::create_dir_all_real(Path::new(dir))?;

    // Проход первый: сумма всего члена и выборка под словарь.
    let mut sum = crate::wal::Crc32::new();
    let mut train = Training::new();
    let read = scan(src, |part| {
        sum.update(part);
        train.feed(part);
        Ok(())
    })?;
    let crc = sum.finish();
    if read != len {
        return Err(bent(format!(
            "{}: прочтено {read} байт вместо {len} — член поехал под руками",
            src.display()
        )));
    }

    // Проход второй: сжатие.
    //
    // Необучившийся словарь — не отказ, а «учиться нечему»: тот же ответ, что у
    // `squeeze`, где это `Ok(None)`. Член тогда ляжет как есть.
    let stored = match train.dict(effort) {
        None => None,
        Some(dict) => match write_packed(dir, stem, src, len, crc, &dict, effort) {
            // Сжатие принимается только если окупилось — правило то же, что у
            // [`put_bytes`], и считается по тем же трём файлам.
            Ok(n) if n < len => Some(n),
            Ok(_) => {
                drop_packed(dir, stem)?;
                None
            }
            Err(e) => {
                // Полусжатый член в архиве опаснее его отсутствия: опись о нём
                // не узнает, а место он займёт.
                drop_packed(dir, stem)?;
                return Err(e);
            }
        },
    };
    match stored {
        Some(stored) => Ok(Member { path: rel.to_string(), len, stored, crc, packed: true }),
        None => {
            write_raw(out_dir, rel, src, len, crc)?;
            Ok(Member { path: rel.to_string(), len, stored: len, crc, packed: false })
        }
    }
}

/// Сжимает член в `.sbin`/`.smrk`/`.sdic` и отдаёт, сколько они заняли.
fn write_packed(
    dir: &str,
    stem: &str,
    src: &Path,
    len: u64,
    crc: u32,
    dict: &crate::codec::pack::EncoderDict,
    effort: Effort,
) -> std::io::Result<u64> {
    let mut out = crate::store::Transfer::create(dir, stem, effort)?;
    out.open_dict(&dict.to_bytes())?;
    let mut sum = crate::wal::Crc32::new();
    let read = scan(src, |part| {
        sum.update(part);
        out.push(part)
    })?;
    let made = out.finish()?;
    // Проходов два, и между ними файл никто не держит. Разойдись они — в архив
    // лёг бы член, сумма которого посчитана по одному содержимому, а байты по
    // другому, и узналось бы это только при восстановлении.
    if read != len || sum.finish() != crc {
        return Err(bent(format!(
            "{}: член изменился между проходами ({read} байт против {len}) — \
             снимку нужен неизменный файл",
            src.display()
        )));
    }
    let want = len.div_ceil(PIECE as u64);
    if u64::from(made.docs) != want {
        return Err(bent(format!(
            "{dir}/{stem}.sbin: уложено {} кусков вместо {want} — член разошёлся с нарезкой",
            made.docs
        )));
    }
    // Суммы распакованных кусков члену архива не нужны: он накрыт **одной**
    // своей суммой целиком, и её проверяет `restore_member`. Файл убирается, а
    // не остаётся лежать: он вошёл бы в архив, но не в опись, и `stored`
    // перестал бы называть занятое место.
    remove_if_there(&format!("{dir}/{stem}.scrc"))?;
    // `Transfer` доводит до носителя сами файлы, но не запись о них в каталоге —
    // ему это и не нужно, там признаком готовности служит `head.tsv`. Здесь
    // признак другой: оглавление снимка, и оно называет член существующим.
    // Обещать это можно только после того, как имя переживёт остановку машины —
    // ровно то, что `atomic_write` делает для каждого мелкого члена.
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| std::io::Error::new(e.kind(), format!("{dir}: {e}")))?;
    let mut stored = 0u64;
    for ext in ["sbin", "smrk", "sdic"] {
        let path = format!("{dir}/{stem}.{ext}");
        let got = std::fs::metadata(&path)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?
            .len();
        stored = stored
            .checked_add(got)
            .ok_or_else(|| bent(format!("{path}: хранимая длина члена переполнена")))?;
    }
    Ok(stored)
}

/// Кладёт член как есть, потоком.
///
/// Сумма и длина сверяются **до** публикации имени: `write_new_beneath_with`
/// переименовывает временный файл только после того, как писатель вернул `Ok`,
/// поэтому отказ здесь не оставляет в архиве члена, которому опись не
/// соответствует.
fn write_raw(out_dir: &str, rel: &str, src: &Path, len: u64, crc: u32) -> std::io::Result<()> {
    crate::fs::write_new_beneath_with(Path::new(out_dir), &format!("{rel}.raw"), |out| {
        let mut sum = crate::wal::Crc32::new();
        let read = scan(src, |part| {
            sum.update(part);
            out.write_all(part)
        })?;
        if read != len || sum.finish() != crc {
            return Err(bent(format!(
                "{}: член изменился между проходами ({read} байт против {len})",
                src.display()
            )));
        }
        Ok(())
    })
}

fn remove_if_there(path: &str) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(std::io::Error::new(e.kind(), format!("{path}: {e}"))),
    }
}

/// Сносит сжатый вид члена целиком.
fn drop_packed(dir: &str, stem: &str) -> std::io::Result<()> {
    for ext in ["sbin", "smrk", "sdic", "scrc", "sdix"] {
        remove_if_there(&format!("{dir}/{stem}.{ext}"))?;
    }
    Ok(())
}

/// То же для содержимого, которого на диске нет в нужном виде.
///
/// Нужно ровно одному члену — списку сегментов: он составляется из захваченного
/// состава, а не читается, см. шапку модуля.
fn put_bytes(out_dir: &str, rel: &str, src: &[u8], effort: Effort) -> std::io::Result<Member> {
    let crc = crate::wal::crc32(src);
    let dst = format!("{out_dir}/{rel}");
    if let Some(parent) = std::path::Path::new(&dst).parent() {
        crate::fs::create_dir_all_real(parent)?;
    }

    // Пустой файл сжимать нечем и незачем: `store::build` на пустом входе
    // отдаёт `None`, и без этой ветки он выглядел бы отказом.
    let built = if src.is_empty() { None } else { squeeze(src, effort, rel)? };

    // Сжатие принимается только если оно окупилось. Считается **весь** член,
    // вместе со словарём и засечками: словарь на мелком файле бывает крупнее
    // самого файла, и без этой суммы архив рос бы от сжатия.
    let packed = match &built {
        Some(b) => b.data.len() + b.marks.len() + b.dict.len() < src.len(),
        None => false,
    };
    let stored = if packed {
        let b = built.as_ref().expect("сжатое есть, раз оно окупилось");
        crate::fs::atomic_write(Path::new(&format!("{dst}.sbin")), &b.data, 0o600)?;
        crate::fs::atomic_write(Path::new(&format!("{dst}.smrk")), &b.marks, 0o600)?;
        crate::fs::atomic_write(Path::new(&format!("{dst}.sdic")), &b.dict, 0o600)?;
        (b.data.len() + b.marks.len() + b.dict.len()) as u64
    } else {
        crate::fs::atomic_write(Path::new(&format!("{dst}.raw")), src, 0o600)?;
        src.len() as u64
    };
    Ok(Member { path: rel.to_string(), len: src.len() as u64, stored, crc, packed })
}

/// Потоково восстанавливает один член, не резервируя `len` байт в памяти.
///
/// Объявленная длина является только верхней границей writer, а не размером
/// аллокации. CRC и точная длина проверяются до атомарной публикации имени.
fn restore_member(arc: &str, dest: &Path, m: &Member) -> Result<(), String> {
    let base = format!("{arc}/{}", m.path);
    let result = crate::fs::write_new_beneath_with(dest, &m.path, |out| {
        let mut written = 0u64;
        let mut crc = crate::wal::Crc32::new();
        let mut emit = |part: &[u8]| -> std::io::Result<()> {
            let part_len = u64::try_from(part.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "длина части не помещается в u64",
                )
            })?;
            written = written.checked_add(part_len).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "длина члена переполнена")
            })?;
            if written > m.len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("распаковано больше объявленных {} байт", m.len),
                ));
            }
            out.write_all(part)?;
            crc.update(part);
            Ok(())
        };

        if m.packed {
            let mut stored = 0u64;
            for ext in ["sbin", "smrk", "sdic"] {
                let rel = format!("{}.{ext}", m.path);
                crate::fs::no_symlink_parents(Path::new(arc), &rel)?;
                let path = Path::new(arc).join(&rel);
                let file = crate::fs::open_private_regular(&path)?;
                stored = stored.checked_add(file.metadata()?.len()).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "хранимая длина члена переполнена",
                    )
                })?;
            }
            if stored != m.stored {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("сжатый член хранит {stored} байт вместо {}", m.stored),
                ));
            }
            // `Store::open` складывает путь как `{каталог}/{поле}.sbin`, поэтому
            // член разбирается обратно на каталог и имя — иначе получилось бы
            // `…/body.sbin/.sbin`.
            let p = Path::new(&base);
            let (dir, field) = (
                p.parent().map(|x| x.to_string_lossy().to_string()).unwrap_or_default(),
                p.file_name().map(|x| x.to_string_lossy().to_string()).unwrap_or_default(),
            );
            // Источник **недоверенный**: жёсткая ссылка на чужой inode здесь
            // отвергается, иначе распаковка архива вытащила бы наружу файл, до
            // которого архиву дела нет. Прежде это делала отдельная
            // `Store::open_private`; теперь недоверие живёт в источнике, и
            // разъехаться двум наборам путей больше негде.
            let store = crate::store::Store::open(&crate::container::loose_private(&dir), &field)
                .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "сжатый член не открылся")
            })?;
            let mut walk = store.walk();
            while let Some((_, part)) = walk.next() {
                emit(part)?;
            }
            if walk.broken() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "сжатый член повреждён",
                ));
            }
        } else {
            let rel = format!("{}.raw", m.path);
            crate::fs::no_symlink_parents(Path::new(arc), &rel)?;
            let path = Path::new(&format!("{base}.raw")).to_path_buf();
            let mut input = crate::fs::open_private_regular(&path)?;
            let actual = input.metadata()?.len();
            if actual != m.len || actual != m.stored {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("несжатый член хранит {actual} байт вместо {}", m.len),
                ));
            }
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = input.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                emit(&buf[..n])?;
            }
        }
        if written != m.len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("получено {written} байт вместо {}", m.len),
            ));
        }
        let got_crc = crc.finish();
        if got_crc != m.crc {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("сумма {got_crc:08x} вместо {:08x}", m.crc),
            ));
        }
        Ok(())
    });
    result.map_err(|e| format!("{}: {e}", m.path))
}

/// Снимает слепок индекса в `<dest>/<name>`.
pub fn create(index: &str, dest: &str, name: &str, effort: Effort) -> Result<Info, String> {
    valid_name(name)?;
    let out_dir = format!("{dest}/{name}");
    if std::path::Path::new(&out_dir).exists() {
        return Err(format!("снимок {name:?} в этом приёмнике уже есть"));
    }
    // Держатель берётся **до** чтения списка: с этой секунды из каталога ничего
    // не удаляется, значит всё, что окажется в списке, доживёт до конца снятия.
    // Обратный порядок оставил бы окно ровно там, где оно опаснее всего —
    // укрупнение сносит исходные каталоги через две секунды после подмены
    // списка, а копирование гигабайта идёт дольше.
    let _pin = crate::manifest::Pin::hold(index, &format!("снимок {name}"))
        .map_err(|e| format!("{index}: держатель не взялся: {e}"))?;
    // Состав берётся **один раз**, и дальше живёт только это значение: файл
    // списка к концу снятия называет уже другой состав, см. шапку модуля.
    //
    // Берётся строгий вариант: индекс, который сам не открывается, не должен
    // давать снимок, который открывается. Иначе пропажа сегмента пережила бы
    // восстановление, обернувшись сдвигом номеров документов у всех уцелевших.
    let segments = crate::manifest::segments_checked(index).map_err(|e| format!("{index}: {e}"))?;
    if segments.is_empty() {
        return Err(format!("{index}: нечего снимать — ни одного сегмента"));
    }
    // Номер раскладки и число документов берутся из первого сегмента: у всех
    // сегментов индекса они по построению одни.
    let head = crate::index::read_head(&segments[0])
        .map_err(|e| format!("{}: заголовок не прочёлся: {e}", segments[0]))?;
    let mut docs = 0u64;
    for s in segments.iter() {
        if let Ok(h) = crate::index::read_head(s) {
            docs += h.docs as u64;
        }
    }

    // План составляется **до** создания каталога снимка: отказ на неперечислимом
    // сегменте не должен оставлять в приёмнике пустой огрызок, который потом
    // займёт имя и не даст повторить попытку.
    let plan = plan(index, &segments)?;

    crate::fs::create_dir_all_real(Path::new(dest)).map_err(|e| format!("{dest}: {e}"))?;
    std::fs::create_dir(&out_dir).map_err(|e| format!("{out_dir}: {e}"))?;
    // Список идёт первым членом — тем же местом, которое он занимал, пока
    // копировался с диска. Восстановление всё равно кладёт его последним, но
    // порядок членов сверяется по номерам, и менять его без нужды незачем.
    let mut members =
        vec![put_bytes(&out_dir, crate::manifest::FILE, &manifest_bytes(&segments), effort)
            .map_err(|e| format!("{}: {e}", crate::manifest::FILE))?];
    members.extend(squeeze_all(index, &out_dir, &plan, effort)?);

    let info = Info {
        name: name.to_string(),
        format: FORMAT,
        taken: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        layout: head.layout,
        docs,
        segments: segments.iter().map(|s| crate::manifest::name_of(s)).collect(),
        members,
    };
    write_info(&out_dir, &info).map_err(|e| format!("{out_dir}/{FILE}: {e}"))?;
    Ok(info)
}

/// Пишет оглавление последним и атомарно — как `manifest::write`.
fn write_info(dir: &str, info: &Info) -> std::io::Result<()> {
    let mut body = info.to_json().to_pretty().into_bytes();
    body.push(b'\n');
    crate::fs::atomic_write(std::path::Path::new(dir).join(FILE).as_path(), &body, 0o600)
}

/// Поднимает индекс из снимка в пустой каталог.
///
/// Каталог обязан быть пустым или отсутствовать. Восстановление поверх живого
/// индекса не поддержано намеренно: смешать два индекса — значит получить
/// третий, который не согласуется сам с собой, а отличить его от здорового
/// нечем.
pub fn restore(arc: &str, dest: &str) -> Result<Info, String> {
    let info = read_checked(arc)
        .map_err(|e| format!("{e} — снимок не дописан, испорчен либо это не снимок"))?;
    if info.layout > crate::index::LAYOUT || info.layout < crate::index::LAYOUT_MIN {
        return Err(format!(
            "снимок снят раскладкой {}, а читаются {}–{}",
            info.layout,
            crate::index::LAYOUT_MIN,
            crate::index::LAYOUT
        ));
    }
    match std::fs::symlink_metadata(dest) {
        Ok(m) if !m.file_type().is_dir() => return Err(format!("{dest}: не настоящий каталог")),
        Ok(_) => {
            let mut it = std::fs::read_dir(dest).map_err(|e| format!("{dest}: {e}"))?;
            if it.next().is_some() {
                return Err(format!("{dest}: каталог не пуст"));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            crate::fs::create_dir_all_real(Path::new(dest)).map_err(|e| format!("{dest}: {e}"))?;
        }
        Err(e) => return Err(format!("{dest}: {e}")),
    }
    let dest_root = Path::new(dest);

    // Сперва всё, кроме списка сегментов: он — признак готовности индекса, как
    // заголовок у сегмента. Положи его первым, и прерванное восстановление
    // выглядело бы работающим индексом с недостающими файлами.
    let mut manifest: Option<&Member> = None;
    for m in info.members.iter() {
        if m.path == crate::manifest::FILE {
            manifest = Some(m);
            continue;
        }
        restore_member(arc, dest_root, m)?;
    }
    if let Some(m) = manifest {
        restore_member(arc, dest_root, m)?;
    }
    std::fs::File::open(dest).and_then(|f| f.sync_all()).map_err(|e| format!("{dest}: {e}"))?;
    Ok(info)
}

#[cfg(test)]
mod tests {
    /// Восстановление называет причину отказа, а не «нет snapshot.json».
    ///
    /// Оглавление отвергается по десятку поводов — член назван дважды, путь
    /// выходит из корня, длина не сходится, — и все они складывались в одно
    /// сообщение про отсутствующий файл. Файл при этом лежал на месте и
    /// прекрасно читался, а человек шёл искать пропажу.
    ///
    /// Заодно проверяется сам запрет выхода из корня: архив приходит откуда
    /// угодно, и путь члена в нём — недоверенный вход.
    #[test]
    fn restore_names_the_reason_and_refuses_to_leave_the_root() {
        let base = std::env::temp_dir().join(format!("svindb-restore-why-{}", std::process::id()));
        let base = base.to_string_lossy().to_string();
        std::fs::remove_dir_all(&base).ok();
        let src = format!("{base}/src");
        std::fs::create_dir_all(&src).expect("каталог");
        let mut ix = crate::indexer::Indexer::new(
            &src,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            1000,
        );
        for i in 0..50u32 {
            ix.add(&format!("d{i}"), &[&format!("док {i}"), "общее"]);
        }
        ix.flush();
        ix.drain();
        let dest = format!("{base}/dest");
        std::fs::create_dir_all(&dest).expect("каталог");
        create(&src, &dest, "snap", crate::codec::Effort::Fast).expect("снимок снят");
        let arc = format!("{dest}/snap");

        // Здоровый — поднимается.
        assert!(restore(&arc, &format!("{base}/ok")).is_ok(), "здоровый снимок обязан подниматься");

        // Путь члена, выходящий из корня, отвергается **и называется**.
        //
        // Подменяется путь **контейнера**: с тех пор как сегмент лежит одним
        // `seg.dat`, отдельных `norms.bin` в описи нет вовсе, и подмена по
        // прежнему имени тихо не находила бы, что менять, — то есть проверка
        // шла бы вхолостую на здоровом архиве.
        let info = std::fs::read_to_string(format!("{arc}/{FILE}")).expect("оглавление");
        let member = format!("seg0000/{}", crate::container::FILE);
        assert!(
            read(&arc).expect("оглавление").members.iter().any(|m| m.path == member),
            "в описи снимка нет {member} — подменять нечего"
        );
        let evil =
            info.replace(&format!("\"path\": \"{member}\""), "\"path\": \"../../ускользнувший\"");
        assert_ne!(evil, info, "подмена обязана была случиться");
        std::fs::write(format!("{arc}/{FILE}"), &evil).expect("записано");
        let e = match restore(&arc, &format!("{base}/evil")) {
            Err(e) => e,
            Ok(_) => panic!("выход из корня обязан был отказать"),
        };
        assert!(
            e.contains("выходит из корня"),
            "отказ обязан назвать причину, а не пропажу файла: {e}"
        );
        assert!(
            !std::path::Path::new(&format!("{base}/ускользнувший")).exists()
                && !std::path::Path::new("/tmp/ускользнувший").exists(),
            "за корень ничего не записалось"
        );

        // Настоящее отсутствие оглавления по-прежнему называется отсутствием.
        std::fs::remove_file(format!("{arc}/{FILE}")).expect("удалено");
        let e = match restore(&arc, &format!("{base}/gone")) {
            Err(e) => e,
            Ok(_) => panic!("без оглавления обязан быть отказ"),
        };
        assert!(e.contains(FILE), "{e}");

        std::fs::remove_dir_all(&base).ok();
    }

    use super::*;

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-snap-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        d
    }

    fn corpus(dir: &str, n: usize) {
        let mut ix = crate::indexer::Indexer::new(
            dir,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            (n / 2).max(1) as u32,
        );
        for i in 0..n {
            ix.add(
                &format!("d{i:06}"),
                &[&format!("загол{}", i % 97), &format!("общее слово{} тело{}", i % 7, i % 53)],
            );
        }
        ix.flush_now();
    }

    /// Restore обязан вернуть не только сегменты, но и семантику их записи.
    /// Без этих файлов данные читаются, однако следующий документ проходит с
    /// другими dynamic/normalizer/scaled_float правилами — это логическая
    /// порча, замаскированная физически здоровым индексом.
    #[test]
    fn shared_settings_and_compiled_mapping_survive_restore() {
        let (src, arc, dst) = (tmp("shared-src"), tmp("shared-arc"), tmp("shared-dst"));
        corpus(&src, 20);
        let schema = br#"{"format":1,"generation":1,"fields":[],"groups":[]}"#;
        let settings = br#"{"strict_ingest":"batch","flush_mb":64}"#;
        let mapping =
            br#"{"mappings":{"dynamic":"strict","properties":{"title":{"type":"keyword"}}}}"#;
        std::fs::write(format!("{src}/schema.json"), schema).unwrap();
        std::fs::write(format!("{src}/settings.json"), settings).unwrap();
        std::fs::write(format!("{src}/mapping.json"), mapping).unwrap();

        let info = create(&src, &arc, "shared", Effort::Fast).expect("снимок");
        for file in SHARED_FILES {
            assert!(
                info.members.iter().any(|member| member.path == *file),
                "общий файл {file} не вошёл в snapshot"
            );
        }
        restore(&format!("{arc}/shared"), &dst).expect("restore");
        assert_eq!(std::fs::read(format!("{dst}/schema.json")).unwrap(), schema);
        assert_eq!(std::fs::read(format!("{dst}/settings.json")).unwrap(), settings);
        assert_eq!(std::fs::read(format!("{dst}/mapping.json")).unwrap(), mapping);
        let opened = crate::segments::Segments::load(&dst).expect("поднятый индекс открывается");
        assert_eq!(opened.docs(), 20);

        for dir in [src, arc, dst] {
            std::fs::remove_dir_all(dir).ok();
        }
    }

    /// Снимок под непрерывным укрупнением — тот случай, ради которого держатели
    /// и заводились.
    ///
    /// Укрупнение сносит каталоги исходных сегментов вскоре после подмены
    /// списка (`grace_ms`), а снятие перечисляет файлы и читает их по одному.
    /// Без держателя между перечислением и чтением каталог исчезает, и снятие
    /// падает на «нет такого файла» — либо, что хуже, успевает записать
    /// оглавление на неполный состав.
    ///
    /// ## Что он на самом деле поймал
    ///
    /// Не ту гонку, ради которой писался. Держатель со **снятием** справлялся:
    /// ни один сегмент из захваченного состава не пропал. Падало
    /// **восстановление** — и по причине, к удалению отношения не имеющей:
    /// `create` копировал `segments.tsv` с диска отдельным членом, уже после
    /// того как состав был захвачен, и укрупнение успевало подменить файл в
    /// этом промежутке. В архив попадал список, называющий `mrg0003` — сегмент,
    /// созданный **после** захвата, которого в архиве поэтому нет.
    ///
    /// Ловилось примерно раз в семь полных прогонов набора. Разбор и починка —
    /// в шапке модуля; здесь тест оставлен как есть, потому что нагрузка ему
    /// нужна настоящая, а свойство, которое чинилось, проверяется отдельно и
    /// без часов: [`the_archived_list_is_composed_not_copied`].
    #[test]
    fn a_snapshot_survives_continuous_consolidation() {
        let (src, arc) = (tmp("race-src"), tmp("race-arc"));
        let mut ix = crate::indexer::Indexer::new(
            &src,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            400,
        );
        for i in 0..6000 {
            ix.add(
                &format!("d{i:06}"),
                &[&format!("загол{}", i % 97), &format!("общее слово{} тело{}", i % 7, i % 53)],
            );
        }
        ix.flush_now();
        drop(ix);
        assert!(
            crate::manifest::segments(&src).len() >= 8,
            "мало сегментов — укрупнению нечего будет сносить"
        );

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (s2, d2) = (stop.clone(), src.clone());
        let churn = std::thread::spawn(move || {
            let cfg = crate::maintain::Config {
                target_docs: 1_000_000,
                at_once: 2,
                grace_ms: 0,
                ..Default::default()
            };
            while !s2.load(std::sync::atomic::Ordering::Relaxed) {
                match crate::maintain::once(&d2, cfg) {
                    Ok(Some(_)) => {}
                    // Слить больше нечего — дальше молотить незачем.
                    _ => break,
                }
            }
        });

        for n in 0..12 {
            let info = create(&src, &arc, &format!("сн{n}"), Effort::Fast)
                .unwrap_or_else(|e| panic!("снимок {n} не снялся: {e}"));
            let dst = format!("{}/поднят{n}", tmp("race-dst"));
            restore(&format!("{arc}/сн{n}"), &dst)
                .unwrap_or_else(|e| panic!("снимок {n} не поднялся: {e}"));
            let listed: Vec<String> =
                crate::manifest::read(&dst).iter().map(|p| crate::manifest::name_of(p)).collect();
            assert_eq!(
                listed, info.segments,
                "снимок {n}: список в архиве разошёлся с оглавлением — \
                 состав захвачен один раз, а записан другой"
            );
            let g = crate::segments::Segments::load(&dst).expect("поднятый не открылся");
            let docs: u64 = g.list.iter().map(|s| s.docs as u64).sum();
            assert_eq!(docs, info.docs, "снимок {n}: документов не столько, сколько записано");
            assert_eq!(g.list.len(), info.segments.len(), "снимок {n}: сегментов не столько");
            std::fs::remove_dir_all(&dst).ok();
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        churn.join().ok();
        for d in [src, arc, tmp("race-dst")] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Список сегментов в архиве **составляется**, а не копируется с диска.
    ///
    /// То самое свойство, нарушение которого ловилось раз в семь прогонов
    /// [`a_snapshot_survives_continuous_consolidation`]. Проверяется без часов и
    /// без второго потока — по доктрине соседнего модуля: воспроизводить окно
    /// между чтением и записью значит получить тест, который то ловит, то нет,
    /// поэтому проверяется само свойство.
    ///
    /// Приём такой: файл списка на диске кладётся **не в канонической
    /// раскладке** — с пустыми строками и отступами, которые `manifest::read`
    /// терпит. Состав от этого не меняется, а байты меняются. Копирование
    /// принесло бы эти байты в архив; составление — нет.
    #[test]
    fn the_archived_list_is_composed_not_copied() {
        let (src, arc, dst) = (tmp("list-src"), tmp("list-arc"), tmp("list-dst"));
        corpus(&src, 600);
        let names: Vec<String> =
            crate::manifest::segments(&src).iter().map(|p| crate::manifest::name_of(p)).collect();
        assert!(names.len() >= 2, "нужно хотя бы два сегмента, вышло {}", names.len());

        let mut raw = String::new();
        for n in &names {
            raw.push_str(&format!("  {n}  \n\n"));
        }
        std::fs::write(format!("{src}/{}", crate::manifest::FILE), &raw).unwrap();
        // Состав от этого не поехал — иначе проверялось бы не то.
        let after: Vec<String> =
            crate::manifest::segments(&src).iter().map(|p| crate::manifest::name_of(p)).collect();
        assert_eq!(after, names, "подготовка испортила состав");

        let info = create(&src, &arc, "список", Effort::Fast).expect("снимок");
        let one = format!("{arc}/список");
        let m = info
            .members
            .iter()
            .find(|m| m.path == crate::manifest::FILE)
            .expect("список сегментов обязан быть членом архива");
        let member_dst = tmp("list-member");
        std::fs::create_dir_all(&member_dst).unwrap();
        restore_member(&one, Path::new(&member_dst), m).expect("член читается");
        let got = std::fs::read(format!("{member_dst}/{}", crate::manifest::FILE)).unwrap();
        assert_eq!(
            String::from_utf8(got.clone()).unwrap(),
            format!("{}\n", names.join("\n")),
            "в архив легли байты с диска, а не составленный список"
        );
        assert_eq!(got, manifest_bytes(&info.segments), "список разошёлся с оглавлением");

        // И поднятый индекс от этого открывается.
        restore(&one, &dst).expect("восстановление");
        let g = crate::segments::Segments::load(&dst).expect("поднятый не открылся");
        assert_eq!(g.list.len(), info.segments.len());
        for d in [src, arc, dst, member_dst] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Снимок неполного индекса — отказ, а не архив, который откроется.
    ///
    /// Индекс, который сам не читается, не должен давать снимок, который
    /// читается: пропажа пережила бы восстановление, обернувшись сдвигом
    /// номеров документов у всех уцелевших — а это хуже самой пропажи, потому
    /// что выданные курсоры страниц начнут указывать на другие документы.
    #[test]
    fn a_snapshot_of_an_incomplete_index_is_refused() {
        let (src, arc) = (tmp("gap-src"), tmp("gap-arc"));
        corpus(&src, 400);
        let mut names: Vec<String> =
            crate::manifest::segments(&src).iter().map(|p| crate::manifest::name_of(p)).collect();
        names.push("seg9999".to_string());
        crate::manifest::write(&src, &names).unwrap();

        let e = create(&src, &arc, "дырявый", Effort::Fast)
            .err()
            .expect("снимок неполного индекса обязан отказать");
        assert!(e.contains("seg9999"), "отказ не назвал пропавший сегмент: {e}");
        assert!(
            !std::path::Path::new(&format!("{arc}/дырявый")).exists(),
            "после отказа остался каталог снимка"
        );
        for d in [src, arc] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Круг «снял — поднял — сверил»: выдача обязана совпасть дословно.
    ///
    /// Не «индекс открылся», а именно выдача: открыться может и слепок, где
    /// половина постингов легла не туда, и отличить его от здорового нечем,
    /// кроме как спросив.
    #[test]
    fn a_restored_index_answers_exactly_the_same() {
        let (src, arc, dst) = (tmp("src"), tmp("arc"), tmp("dst"));
        // Порог опущен, чтобы контейнер сегмента лёг в архив **потоком**: в
        // выпуске потоковый путь начинается за восемью мегабайтами, а корпус
        // такого размера считался бы минуту. Круг «снял — поднял — сверил»
        // единственный, кто проходит этот путь насквозь: от `create` до выдачи
        // поднятого индекса.
        let _cut = lower_cut(64 << 10, TRAIN_PARTS);
        corpus(&src, 4000);
        let info = create(&src, &arc, "первый", Effort::Thorough).expect("снимок");
        assert!(info.total_stored() > 0 && !info.members.is_empty());
        assert!(
            info.members.iter().any(|m| m.len > cut().0 as u64),
            "ни один член не крупнее порога — потоковый путь не тронулся, \
             и круг проверяет не то, что задуман"
        );
        restore(&format!("{arc}/первый"), &dst).expect("восстановление");

        let a = crate::segments::Segments::load(&src).expect("исходный");
        let b = crate::segments::Segments::load(&dst).expect("поднятый");
        assert_eq!(a.list.len(), b.list.len(), "число сегментов");
        for (fid, word) in [(1usize, "слово1"), (1, "тело7"), (0, "загол3"), (1, "общее")]
        {
            let mut spec = crate::segments::QuerySpec { k: 25, ..Default::default() };
            spec.words = vec![crate::segments::SpecWord {
                fid,
                given: None,
                terms: vec![crate::segments::Term { text: word.into(), prefix: false }],
            }];
            let x: Vec<(u64, u64)> =
                a.search(&spec).hits.iter().map(|h| (h.doc, h.score.to_bits())).collect();
            let y: Vec<(u64, u64)> =
                b.search(&spec).hits.iter().map(|h| (h.doc, h.score.to_bits())).collect();
            assert_eq!(x, y, "слово {word}: выдача разошлась");
            assert!(!x.is_empty(), "слово {word} ничего не нашло — проверка вхолостую");
        }
        for d in [src, arc, dst] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Порча ловится по имени, а не читается как годное.
    #[test]
    fn a_damaged_member_is_named_not_read() {
        let (src, arc, dst) = (tmp("bad-src"), tmp("bad-arc"), tmp("bad-dst"));
        corpus(&src, 800);
        create(&src, &arc, "снимок", Effort::Fast).expect("снимок");
        let one = format!("{arc}/снимок");

        // Портим один байт в первом попавшемся члене.
        let info = read(&one).expect("оглавление");
        let m = info.members.iter().find(|m| m.len > 64).expect("есть непустой член");
        let path = format!("{one}/{}{}", m.path, if m.packed { ".sbin" } else { ".raw" });
        let mut bytes = std::fs::read(&path).unwrap();
        let at = bytes.len() / 2;
        bytes[at] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let err = restore(&one, &dst).err().expect("порча принята молча");
        assert!(err.contains(&m.path), "отказ не назвал файл: {err}");
        for d in [src, arc, dst] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Недописанный снимок не виден в списке и не восстанавливается.
    #[test]
    fn an_unfinished_snapshot_is_invisible() {
        let (src, arc, dst) = (tmp("part-src"), tmp("part-arc"), tmp("part-dst"));
        corpus(&src, 400);
        create(&src, &arc, "целый", Effort::Fast).expect("снимок");
        // Второй снимок обрываем на полпути: файлы есть, оглавления нет.
        std::fs::create_dir_all(format!("{arc}/обрыв")).unwrap();
        std::fs::write(format!("{arc}/обрыв/мусор.raw"), b"nothing").unwrap();

        let seen: Vec<String> = list(&arc).into_iter().map(|i| i.name).collect();
        assert_eq!(seen, vec!["целый".to_string()], "недописанный попал в список");
        assert!(restore(&format!("{arc}/обрыв"), &dst).is_err());
        for d in [src, arc, dst] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Имя снимка не выпускает за приёмник.
    #[test]
    fn a_name_cannot_escape_the_destination() {
        for bad in ["..", "../чужое", "a/b", ".скрытый", "", "с пробелом"] {
            assert!(valid_name(bad).is_err(), "имя {bad:?} принято");
        }
        for good in ["снимок", "2026-08-07", "ночной_1", "a.b"] {
            assert!(valid_name(good).is_ok(), "имя {good:?} отвергнуто");
        }
    }

    #[test]
    fn snapshot_metadata_rejects_traversal_and_negative_lengths() {
        let base = |member: &str| {
            crate::json::parse(&format!(
                r#"{{"format":1,"name":"safe","taken":1,"layout":{},"docs":1,"segments":["seg0000"],"members":[{{"path":"segments.tsv","len":1,"stored":1,"crc":0,"packed":false}},{member}]}}"#,
                crate::index::LAYOUT
            ))
            .unwrap()
        };
        let escaped =
            base(r#"{"path":"seg0000/../../outside","len":1,"stored":1,"crc":0,"packed":false}"#);
        assert!(Info::from_json(&escaped).is_err());

        let negative = crate::json::parse(&format!(
            r#"{{"format":1,"name":"safe","taken":1,"layout":{},"docs":1,"segments":["seg0000"],"members":[{{"path":"segments.tsv","len":-1,"stored":1,"crc":0,"packed":false}}]}}"#,
            crate::index::LAYOUT
        ))
        .unwrap();
        assert!(Info::from_json(&negative).is_err());
    }

    #[test]
    fn huge_declared_packed_length_is_streamed_and_refused_without_reservation() {
        let (arc, dst) = (tmp("huge-packed-arc"), tmp("huge-packed-dst"));
        std::fs::create_dir_all(&arc).unwrap();
        std::fs::create_dir_all(&dst).unwrap();
        let source = vec![b'x'; 256 * 1024];
        let mut member = put_bytes(&arc, "schema.json", &source, Effort::Fast).unwrap();
        assert!(member.packed, "однородный тестовый член должен сжаться");
        member.len = i64::MAX as u64;
        assert!(restore_member(&arc, Path::new(&dst), &member).is_err());
        assert!(!Path::new(&dst).join("schema.json").exists(), "член с ложной длиной опубликован");
        std::fs::remove_dir_all(arc).ok();
        std::fs::remove_dir_all(dst).ok();
    }

    #[test]
    fn snapshot_plan_refuses_links_and_special_entries() {
        use std::os::unix::fs::symlink;
        let root = tmp("plan-link");
        let segment = format!("{root}/seg0000");
        std::fs::create_dir_all(&segment).unwrap();
        std::fs::write(format!("{segment}/head.tsv"), b"head").unwrap();
        let outside = format!("{root}/outside");
        std::fs::write(&outside, b"secret").unwrap();
        symlink(&outside, format!("{segment}/leak.bin")).unwrap();
        assert!(plan(&root, std::slice::from_ref(&segment)).is_err());
        std::fs::remove_dir_all(&root).ok();
    }

    /// Сжимаемое содержимое заданной длины.
    fn tidy(n: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n + 64);
        let mut i = 0u32;
        while out.len() < n {
            out.extend_from_slice(
                format!("строка {i} поле{} общее слово{}\n", i % 97, i % 7).as_bytes(),
            );
            i += 1;
        }
        out.truncate(n);
        out
    }

    /// Заведомо несжимаемое содержимое заданной длины.
    fn noise(n: usize) -> Vec<u8> {
        let mut s = 0x9E37_79B9_7F4A_7C15u64;
        (0..n)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                s as u8
            })
            .collect()
    }

    /// Крупный член, уложенный потоком, совпадает с уложенным через память.
    ///
    /// Совпадение здесь не украшение, а договор: [`put_streamed`] повторяет
    /// нарезку и выборку `store::build_with` дословно, и стоит хоть одному из
    /// двух путей поехать — архив начнёт зависеть от того, каким путём его
    /// собрали. Проверяются оба исхода правила «сжатие принимается, если
    /// окупилось»: сжимаемый член и заведомо несжимаемый.
    #[test]
    fn the_streamed_member_matches_the_one_built_in_memory() {
        let (index, mem, flow, dst) =
            (tmp("flow-src"), tmp("flow-mem"), tmp("flow-out"), tmp("flow-dst"));
        std::fs::create_dir_all(format!("{index}/seg0000")).expect("каталог");
        std::fs::create_dir_all(&dst).expect("каталог");
        // Порог опущен, число обучающих кусков — нет: их у члена такого размера
        // всё равно меньше потолка, и оба пути берут одни и те же.
        let _cut = lower_cut(64 << 10, TRAIN_PARTS);
        // Вчетверо крупнее порога: ниже него член читается целиком, и потоковый
        // путь не тронулся бы вовсе.
        let big = cut().0 * 4;
        for (name, src, want_packed) in
            [("seg.dat", tidy(big), true), ("шум.bin", noise(big), false)]
        {
            let rel = format!("seg0000/{name}");
            std::fs::write(format!("{index}/{rel}"), &src).expect("член");
            let a = put_bytes(&mem, &rel, &src, Effort::Fast).expect("через память");
            // Потоковая укладка зовётся напрямую: она здесь и проверяется, а то,
            // что `put` отдаёт ей крупные члены, стоит на своей проверке —
            // `a_streamed_member_costs_the_same_whatever_its_size`.
            let b = put_streamed(
                &flow,
                &rel,
                &Path::new(&index).join(&rel),
                src.len() as u64,
                Effort::Fast,
            )
            .expect("потоком");
            assert_eq!(a.packed, want_packed, "{name}: решение о сжатии не то, что ожидалось");
            assert_eq!(
                (a.len, a.stored, a.crc, a.packed),
                (b.len, b.stored, b.crc, b.packed),
                "{name}: опись потокового члена разошлась с описью того же члена в памяти"
            );
            assert_eq!(b.len, src.len() as u64, "{name}: длина в описи не длина члена");
            for ext in if a.packed { &["sbin", "smrk", "sdic"][..] } else { &["raw"][..] } {
                assert_eq!(
                    std::fs::read(format!("{mem}/{rel}.{ext}")).expect("через память"),
                    std::fs::read(format!("{flow}/{rel}.{ext}")).expect("потоком"),
                    "{name}: .{ext} потокового члена разошёлся с тем же членом в памяти"
                );
            }
            // Лишнего в архиве не осталось: суммы кусков сняты, а у несжатого
            // члена не осталось и следа от неокупившейся попытки.
            for ext in ["scrc", "sdix"] {
                assert!(
                    !Path::new(&format!("{flow}/{rel}.{ext}")).exists(),
                    "{name}: в архиве остался .{ext}, которого нет в описи"
                );
            }
            if !a.packed {
                for ext in ["sbin", "smrk", "sdic"] {
                    assert!(
                        !Path::new(&format!("{flow}/{rel}.{ext}")).exists(),
                        "{name}: неокупившееся сжатие осталось лежать в архиве"
                    );
                }
            }
            // И член поднимается обратно ровно тем, чем был.
            restore_member(&flow, Path::new(&dst), &b).expect("член поднялся");
            assert_eq!(
                std::fs::read(format!("{dst}/{rel}")).expect("поднятый"),
                src,
                "{name}: поднятый член не совпал с исходным"
            );
        }
        for d in [index, mem, flow, dst] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Член, поехавший между проходами, в архив не попадает.
    ///
    /// Потоковая укладка проходит по файлу дважды, а то и трижды, и между
    /// проходами его никто не держит. Разойдись проходы — в архив лёг бы член,
    /// сумма которого посчитана по одному содержимому, а байты по другому;
    /// узналось бы это при восстановлении и уже ничем не помогло.
    ///
    /// Подделывается здесь не файл, а то, с чем сверяется проход: длина и сумма
    /// первого прохода. Расхождение обязано быть отказом, а не молчанием.
    #[test]
    fn a_member_that_shifted_between_passes_is_refused() {
        let (index, out) = (tmp("shift-src"), tmp("shift-out"));
        std::fs::create_dir_all(&index).expect("каталог");
        std::fs::create_dir_all(&out).expect("каталог");
        let body = tidy(128 << 10);
        let src = Path::new(&index).join("член");
        std::fs::write(&src, &body).expect("член");
        let (len, crc) = (body.len() as u64, crate::wal::crc32(&body));

        // Как есть: имя публикуется только после сверки, поэтому неверная длина
        // или сумма не оставляют в архиве ничего.
        for (what, len, crc) in [("длина", len - 1, crc), ("сумма", len, crc ^ 1)] {
            assert!(
                write_raw(&out, "член", &src, len, crc).is_err(),
                "{what} разошлась, а член принят"
            );
            assert!(
                !Path::new(&format!("{out}/член.raw")).exists(),
                "{what} разошлась, а член лёг в архив"
            );
        }
        write_raw(&out, "член", &src, len, crc).expect("целый член отвергнут");

        // Сжатым — то же самое.
        let mut train = Training::new();
        scan(&src, |part| {
            train.feed(part);
            Ok(())
        })
        .expect("проход");
        let dict = train.dict(Effort::Fast).expect("словарь обучился");
        for (what, len, crc) in [("длина", len - 1, crc), ("сумма", len, crc ^ 1)] {
            assert!(
                write_packed(&out, "член", &src, len, crc, &dict, Effort::Fast).is_err(),
                "{what} разошлась, а сжатый член принят"
            );
            drop_packed(&out, "член").expect("уборка");
        }
        write_packed(&out, "член", &src, len, crc, &dict, Effort::Fast)
            .expect("целый член отвергнут");
        for d in [index, out] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Снимок не растёт в памяти вместе с членом.
    ///
    /// Ради чего [`put_streamed`] и заведён. Член архива теперь весь сегмент, до
    /// четырёх гигабайт, и члены жмутся по нескольку сразу: чтение члена в `Vec`
    /// стоило бы «размер сегмента, умножить на число потоков».
    ///
    /// Проверяется **прирост**, а не занятое: постоянного здесь мегабайты —
    /// буфер чтения, рабочие таблицы упаковщика, словарь с его цепочками,
    /// выборка и обучающие куски, — и на опущенном покрое оно само по себе
    /// крупнее члена. Но потолок у каждого свой, и от размера члена не зависит
    /// ни один: вот это и меряется.
    ///
    /// Считается [`crate::alloc::peak`]: у укладки члена нулевой след — она всё
    /// освобождает и отдаёт наружу файлы, — стоит она ровно тем, что держала в
    /// середине.
    #[test]
    fn a_streamed_member_costs_the_same_whatever_its_size() {
        let (index, out) = (tmp("peak-src"), tmp("peak-out"));
        std::fs::create_dir_all(format!("{index}/seg0000")).expect("каталог");
        // Обучающие куски здесь тоже опущены: их потолок в выпуске — постоянная,
        // а на членах мельче восьми мегабайт он не достигается, и они росли бы
        // вместе с членом, пряча то, что меряется.
        let small = 1 << 20;
        let large = 4 << 20;
        for (name, n) in [("малый", small), ("крупный", large)] {
            std::fs::write(format!("{index}/seg0000/{name}"), tidy(n)).expect("член");
        }
        let cost = |name: &'static str| -> usize {
            // Свежая нить даёт каждому замеру чистый потоко-местный счётчик.
            // Libtest переиспользует рабочие нити, а выделенное одной нитью
            // иногда освобождает другая; счётчик это допускает и потому может
            // начинать следующий тест с отрицательного `live`. В одиночном
            // прогоне разницы нет, под всем workspace это давало ложный рост
            // примерно на мегабайт.
            let (index, out) = (index.clone(), out.clone());
            std::thread::spawn(move || {
                let _cut = lower_cut(64 << 10, 4);
                crate::alloc::peak(|| {
                    let m = put(&index, &out, &format!("seg0000/{name}"), Effort::Fast)
                        .expect("член уложен");
                    assert!(m.packed, "{name}: сжимаемый член лёг несжатым — мерялось бы не то");
                })
            })
            .join()
            .expect("нить замера завершилась")
        };
        let a = cost("малый");
        let b = cost("крупный");
        // Один READ_BUF допускается как шум перекрытия системного чтения и
        // буфера упаковщика: под полным параллельным прогоном их пики могут
        // попасть в одну точку только у одного из двух замеров. Это всё ещё
        // далеко от ложного пути «держать член целиком»: при росте 1 → 4 МБ
        // тот дал бы не меньше трёх лишних мегабайт.
        let noise = READ_BUF + (512 << 10);
        assert!(
            b <= a + noise,
            "член вырос вчетверо ({small} → {large} байт), а занятое с ним: {a} → {b} — \
             значит член держится в памяти, а не течёт"
        );
        for d in [index, out] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Сжатие включается там, где окупается, и не включается там, где нет.
    ///
    /// Проверяется не «сжалось», а **решение**: случайные байты обязаны лечь
    /// как есть, иначе архив рос бы от сжатия.
    #[test]
    fn compression_is_taken_only_when_it_pays() {
        let dir = tmp("pays");
        std::fs::create_dir_all(&dir).unwrap();
        let out = tmp("pays-out");
        std::fs::create_dir_all(&out).unwrap();

        // Повторяющийся — обязан сжаться.
        std::fs::write(format!("{dir}/повтор"), "одно и то же ".repeat(20000)).unwrap();
        let m = put(&dir, &out, "повтор", Effort::Thorough).unwrap();
        assert!(m.packed, "повторяющийся не сжался");
        assert!(m.stored * 10 < m.len, "сжался всего в {:.1} раза", m.len as f64 / m.stored as f64);

        // Случайный — обязан лечь как есть.
        let mut s = 0x1234_5678u64;
        let noise: Vec<u8> = (0..200_000)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                s as u8
            })
            .collect();
        std::fs::write(format!("{dir}/шум"), &noise).unwrap();
        let m = put(&dir, &out, "шум", Effort::Thorough).unwrap();
        assert!(!m.packed, "случайные байты объявлены сжатыми");
        assert_eq!(m.stored, m.len);

        // Пустой — не отказ.
        std::fs::write(format!("{dir}/пусто"), b"").unwrap();
        let m = put(&dir, &out, "пусто", Effort::Thorough).unwrap();
        assert_eq!((m.len, m.stored, m.packed), (0, 0, false));
        for d in [dir, out] {
            std::fs::remove_dir_all(&d).ok();
        }
    }
}

// ---------------------------------------------------------------------------
// Инкрементальные снимки: репозиторий объектов.
//
// ## На чём это стоит
//
// Каталог сегмента **неизменяем** после того, как записан `head.tsv`: движок
// никогда не правит его файлы, только заводит новые каталоги и снимает старые.
// Значит второй снимок того же индекса переписывает те же байты — и это
// единственный расход, который здесь убирается.
//
// Тождество сегмента уже есть: [`crate::index::head_ident`] по `head.tsv`.
// Сегмент с тем же именем и тем же тождеством — это тот же сегмент, и снятый
// однажды он больше не снимается.
//
// ## Устройство
//
// ```text
// приёмник/
//   objects/<имя>-<тождество>/    сжатые члены сегмента + их опись
//   <снимок>/snapshot.json        оглавление, ссылающееся на объекты
// ```
//
// Объект пишется во временное имя `.partial-<uuid>` и переименовывается по
// готовности. Отсюда возобновляемость: недописанный объект не имеет
// настоящего имени, его никто не найдёт, а повтор снятия просто напишет его
// заново. Полуготовых объектов под настоящим именем не бывает **никогда** —
// переименование атомарно.
// ---------------------------------------------------------------------------

/// Имя каталога объектов в приёмнике.
pub const OBJECTS: &str = "objects";

/// Опись одного объекта — лежит внутри него.
const OBJECT_FILE: &str = "object.json";

/// Ключ объекта: имя сегмента и его тождество.
///
/// Тождество берётся у `head.tsv` — файла, который пишется последним и после
/// которого сегмент неизменяем. Совпал ключ — совпал и сегмент побайтово.
pub fn object_key(index: &str, seg: &str) -> String {
    let (ino, mtime) = crate::index::head_ident(&format!("{index}/{seg}"));
    // Свёртка в одно число: имя каталога не должно зависеть от того, как
    // печатается отрицательное время на разных платформах.
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for x in [ino, mtime as u64] {
        h ^= x;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    format!("{seg}-{h:016x}")
}

/// Уже ли этот объект лежит в приёмнике целиком.
fn object_ready(dest: &str, key: &str) -> Option<Vec<Member>> {
    let path = format!("{dest}/{OBJECTS}/{key}/{OBJECT_FILE}");
    let bytes = crate::fs::read_regular_limited(Path::new(&path), 64 << 20, false).ok()?;
    let text = String::from_utf8(bytes).ok()?;
    let json = json::parse(&text).ok()?;
    let Json::Arr(arr) = json.get("members")? else { return None };
    let mut out = Vec::with_capacity(arr.len());
    for m in arr.iter() {
        out.push(Member {
            path: m.get("path")?.as_str()?.to_string(),
            len: u64::try_from(m.get("len")?.as_i64()?).ok()?,
            stored: u64::try_from(m.get("stored")?.as_i64()?).ok()?,
            crc: u32::try_from(m.get("crc")?.as_i64()?).ok()?,
            packed: m.get("packed")?.as_bool()?,
        });
    }
    Some(out)
}

fn object_json(members: &[Member]) -> Vec<u8> {
    let list = Json::Arr(
        members
            .iter()
            .map(|m| {
                json::obj(vec![
                    ("path", json::s(&m.path)),
                    ("len", Json::Int(m.len as i64)),
                    ("stored", Json::Int(m.stored as i64)),
                    ("crc", Json::Int(m.crc as i64)),
                    ("packed", Json::Bool(m.packed)),
                ])
            })
            .collect(),
    );
    let mut body = json::obj(vec![("members", list)]).to_pretty().into_bytes();
    body.push(b'\n');
    body
}

/// Снимает индекс, переиспользуя уже снятые сегменты.
///
/// ## Что здесь дороже и что дешевле
///
/// Дешевле: сегмент, снятый прошлый раз, не снимается снова — ни чтения, ни
/// сжатия, ни записи. На дописываемом корпусе это почти весь снимок: старые
/// сегменты не меняются, новыми оказываются только те, что легли с прошлого
/// раза.
///
/// Дороже: приёмник хранит объекты, на которые больше никто не ссылается, пока
/// их не уберёт [`prune`]. Это осознанный размен — место против времени, и
/// убирается оно по явной просьбе, а не догадкой.
pub fn create_incremental(
    index: &str,
    dest: &str,
    name: &str,
    effort: Effort,
) -> Result<Info, String> {
    valid_name(name)?;
    let out_dir = format!("{dest}/{name}");
    if std::path::Path::new(&out_dir).exists() {
        return Err(format!("снимок {name:?} в этом приёмнике уже есть"));
    }
    let _pin = crate::manifest::Pin::hold(index, &format!("снимок {name}"))
        .map_err(|e| format!("{index}: держатель не взялся: {e}"))?;
    let segments = crate::manifest::segments_checked(index).map_err(|e| format!("{index}: {e}"))?;
    if segments.is_empty() {
        return Err(format!("{index}: нечего снимать — ни одного сегмента"));
    }
    let head = crate::index::read_head(&segments[0])
        .map_err(|e| format!("{}: заголовок не прочёлся: {e}", segments[0]))?;
    let mut docs = 0u64;
    for s in segments.iter() {
        if let Ok(h) = crate::index::read_head(s) {
            docs += h.docs as u64;
        }
    }

    crate::fs::create_dir_all_real(Path::new(&format!("{dest}/{OBJECTS}")))
        .map_err(|e| format!("{dest}/{OBJECTS}: {e}"))?;

    let names: Vec<String> = segments.iter().map(|s| crate::manifest::name_of(s)).collect();
    let mut members: Vec<Member> = Vec::new();
    let mut keys: Vec<String> = Vec::with_capacity(names.len());
    for seg in names.iter() {
        let key = object_key(index, seg);
        keys.push(key.clone());
        if let Some(have) = object_ready(dest, &key) {
            // Этот сегмент уже снят — и снят он был неизменяемым, значит
            // переснимать нечего. Это и есть весь выигрыш инкрементальности.
            members.extend(have);
            continue;
        }
        // Своё имя на попытку: две одновременные попытки не спорят за один
        // временный каталог, а недописанная не занимает настоящего имени.
        let tmp = format!("{dest}/{OBJECTS}/.partial-{}-{}", std::process::id(), key);
        std::fs::remove_dir_all(&tmp).ok();
        crate::fs::create_dir_all_real(Path::new(&tmp)).map_err(|e| format!("{tmp}: {e}"))?;
        let one = plan(index, std::slice::from_ref(&format!("{index}/{seg}")))?;
        // Общие файлы индекса в объект сегмента не идут: они не его.
        let mine: Vec<String> =
            one.into_iter().filter(|p| p.starts_with(&format!("{seg}/"))).collect();
        let got = squeeze_all(index, &tmp, &mine, effort)?;
        crate::fs::atomic_write(
            Path::new(&tmp).join(OBJECT_FILE).as_path(),
            &object_json(&got),
            0o600,
        )
        .map_err(|e| format!("{tmp}/{OBJECT_FILE}: {e}"))?;
        let final_dir = format!("{dest}/{OBJECTS}/{key}");
        std::fs::rename(&tmp, &final_dir).map_err(|e| format!("{final_dir}: {e}"))?;
        members.extend(got);
    }

    // Общие файлы индекса — схема, настройки, compiled mapping и список —
    // живут в самом снимке, а не в объектах: они меняются с каждым снимком, и
    // делить их незачем.
    std::fs::create_dir_all(&out_dir).map_err(|e| format!("{out_dir}: {e}"))?;
    let mut shared =
        vec![put_bytes(&out_dir, crate::manifest::FILE, &manifest_bytes(&segments), effort)
            .map_err(|e| format!("{}: {e}", crate::manifest::FILE))?];
    for file in SHARED_FILES {
        if std::path::Path::new(index).join(file).exists() {
            shared.push(put(index, &out_dir, file, effort).map_err(|e| format!("{file}: {e}"))?);
        }
    }
    shared.extend(members);

    let info = Info {
        name: name.to_string(),
        format: FORMAT,
        taken: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        layout: head.layout,
        docs,
        segments: names,
        members: shared,
    };
    write_info(&out_dir, &info).map_err(|e| format!("{out_dir}/{FILE}: {e}"))?;
    // Ключи объектов пишутся рядом отдельным файлом: оглавление снимка — общий
    // формат с полными снимками, и добавлять в него поле значило бы, что старый
    // читатель молча пропустит ссылки и решит, что снимок пуст.
    let keys_body = keys.join("\n") + "\n";
    crate::fs::atomic_write(
        Path::new(&out_dir).join("objects.txt").as_path(),
        keys_body.as_bytes(),
        0o600,
    )
    .map_err(|e| format!("{out_dir}/objects.txt: {e}"))?;
    Ok(info)
}

/// На какие объекты ссылается этот снимок.
pub fn objects_of(snap_dir: &str) -> Vec<String> {
    let path = format!("{snap_dir}/objects.txt");
    crate::fs::read_regular_limited(Path::new(&path), 16 << 20, false)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .map(|t| t.lines().filter(|l| !l.trim().is_empty()).map(str::to_string).collect())
        .unwrap_or_default()
}

/// Оставляет `keep` новейших снимков и убирает объекты, на которые никто не
/// ссылается.
///
/// ## Почему объекты убираются вторым проходом, а не вместе со снимком
///
/// Объект делится между снимками — в этом весь смысл. Убирать его вместе с тем
/// снимком, который его назвал, значило бы сносить данные, нужные другому.
/// Поэтому сперва уходят снимки, потом пересчитываются ссылки, и уходит только
/// то, на что не сослался **никто** из уцелевших.
///
/// Отдаёт, сколько снимков и сколько объектов убрано.
pub fn prune(dest: &str, keep: usize) -> Result<(usize, usize), String> {
    let mut snaps = list(dest);
    // Новейшие — в конце: порядок задаёт время снятия, при равенстве имя.
    snaps.sort_by(|a, b| a.taken.cmp(&b.taken).then(a.name.cmp(&b.name)));
    let drop_to = snaps.len().saturating_sub(keep);
    let mut gone = 0usize;
    for s in snaps.iter().take(drop_to) {
        let dir = format!("{dest}/{}", s.name);
        std::fs::remove_dir_all(&dir).map_err(|e| format!("{dir}: {e}"))?;
        gone += 1;
    }

    // Кто ещё нужен.
    let mut live: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in list(dest).iter() {
        for k in objects_of(&format!("{dest}/{}", s.name)) {
            live.insert(k);
        }
    }
    let mut freed = 0usize;
    let root = format!("{dest}/{OBJECTS}");
    let Ok(rd) = std::fs::read_dir(&root) else { return Ok((gone, 0)) };
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        // Недописанные попытки убираются заодно: настоящего имени у них нет, и
        // сослаться на них никто не мог.
        let orphan = name.starts_with(".partial-") || !live.contains(&name);
        if orphan && e.path().is_dir() {
            std::fs::remove_dir_all(e.path()).map_err(|e| format!("{name}: {e}"))?;
            freed += 1;
        }
    }
    Ok((gone, freed))
}

#[cfg(test)]
mod incremental_tests {
    use super::*;

    fn corpus(dir: &str, from: u32, to: u32, per: u32) {
        let mut ix = crate::indexer::Indexer::new(dir, vec!["body".into()], vec![1.0], per);
        for i in from..to {
            ix.add(&format!("d{i}"), &[&format!("общее слово{}", i % 5)]);
        }
        ix.flush_now();
    }

    fn tmp(tag: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-inc-{tag}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        d
    }

    fn objects_in(dest: &str) -> usize {
        std::fs::read_dir(format!("{dest}/{OBJECTS}"))
            .map(|rd| {
                rd.flatten()
                    .filter(|e| !e.file_name().to_string_lossy().starts_with(".partial-"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Второй снимок не переснимает то, что не менялось.
    ///
    /// ## Что здесь доказывается
    ///
    /// Не «стало быстрее» — это про скорость и меряется временем, которому на
    /// этой машине верить нельзя. Доказывается **тождество объектов**: сегмент,
    /// не изменившийся между снимками, назван тем же ключом и лежит в одном
    /// экземпляре. Именно из этого и следует, что второй раз он не читался.
    #[test]
    fn a_second_snapshot_reuses_unchanged_segments() {
        let ix = tmp("reuse-ix");
        let dest = tmp("reuse-dest");
        corpus(&ix, 0, 400, 100);

        let first = create_incremental(&ix, &dest, "s1", Effort::Fast).expect("первый снимок");
        let was = objects_in(&dest);
        assert!(was >= 4, "сегментов вышло {was} — проверять нечего");
        let keys1 = objects_of(&format!("{dest}/s1"));
        assert_eq!(keys1.len(), first.segments.len(), "ключей не столько, сколько сегментов");

        // Ничего не меняли — второй снимок обязан назвать те же объекты и не
        // завести ни одного нового.
        let _same = create_incremental(&ix, &dest, "s2", Effort::Fast).expect("второй снимок");
        assert_eq!(objects_in(&dest), was, "неизменный индекс завёл новые объекты");
        assert_eq!(objects_of(&format!("{dest}/s2")), keys1, "ключи разошлись на том же индексе");

        // А дописанные документы дают новые сегменты — и только их.
        corpus(&ix, 400, 600, 100);
        let third = create_incremental(&ix, &dest, "s3", Effort::Fast).expect("третий снимок");
        let now = objects_in(&dest);
        assert!(now > was, "дописанное не завело ни одного объекта");
        let keys3 = objects_of(&format!("{dest}/s3"));
        assert_eq!(keys3.len(), third.segments.len());
        // Старые ключи обязаны быть среди новых: сегменты неизменяемы.
        for k in keys1.iter() {
            assert!(keys3.contains(k), "прежний сегмент получил другой ключ: {k}");
        }
        std::fs::remove_dir_all(&ix).ok();
        std::fs::remove_dir_all(&dest).ok();
    }

    /// Уборка держит `keep` новейших и сносит только то, на что никто не сослался.
    ///
    /// ## Как получается ничейный объект
    ///
    /// Пока в индекс только дописывают, ничейных не бывает вовсе: сегменты
    /// неизменяемы, и каждый следующий снимок называет все прежние. Осиротеть
    /// объект может лишь тогда, когда сегмент **исчез** из индекса — его слили,
    /// вычистили или индекс пересобрали заново. Здесь второе состояние
    /// получается пересборкой: так проверка не зависит от того, когда решит
    /// сработать обслуживание.
    #[test]
    fn pruning_keeps_the_newest_and_frees_only_the_unreferenced() {
        let ix = tmp("prune-ix");
        let dest = tmp("prune-dest");
        corpus(&ix, 0, 200, 100);
        create_incremental(&ix, &dest, "s1", Effort::Fast).expect("s1");
        corpus(&ix, 200, 400, 100);
        create_incremental(&ix, &dest, "s2", Effort::Fast).expect("s2");
        let shared = objects_of(&format!("{dest}/s2"));
        assert!(shared.len() >= 4, "сегментов вышло {} — проверять нечего", shared.len());

        // Индекс пересобран с нуля: прежних сегментов в нём больше нет.
        std::fs::remove_dir_all(&ix).ok();
        corpus(&ix, 1000, 1200, 100);
        create_incremental(&ix, &dest, "s3", Effort::Fast).expect("s3");
        let all = objects_in(&dest);
        let keys3 = objects_of(&format!("{dest}/s3"));
        assert!(all > keys3.len(), "пересборка не оставила чужих объектов");

        // Оставляем два новейших: s1 уходит, но его сегменты ещё нужны s2.
        let (gone, freed) = prune(&dest, 2).expect("уборка");
        assert_eq!(gone, 1, "убрано снимков {gone}, а не один");
        let left: Vec<String> = list(&dest).into_iter().map(|i| i.name).collect();
        assert_eq!(left, vec!["s2".to_string(), "s3".to_string()], "убрали не тот снимок");
        assert_eq!(freed, 0, "уборка забрала объекты, нужные уцелевшим");
        assert_eq!(objects_in(&dest), all, "объектов стало меньше при живых ссылках");

        // Теперь уцелеет только s3, и объекты прежнего индекса станут ничьими.
        let (gone2, freed2) = prune(&dest, 1).expect("уборка");
        assert_eq!(gone2, 1);
        assert!(freed2 > 0, "ничейные объекты не убрались");
        assert_eq!(objects_in(&dest), keys3.len(), "осталось не ровно то, на что ссылается s3");
        std::fs::remove_dir_all(&ix).ok();
        std::fs::remove_dir_all(&dest).ok();
    }

    /// Недописанный объект не занимает настоящего имени и убирается уборкой.
    ///
    /// Возобновляемость стоит ровно на этом: у полуготового объекта нет имени,
    /// по которому его нашли бы, а переименование по готовности атомарно.
    #[test]
    fn a_half_written_object_has_no_real_name() {
        let ix = tmp("partial-ix");
        let dest = tmp("partial-dest");
        corpus(&ix, 0, 200, 100);
        create_incremental(&ix, &dest, "s1", Effort::Fast).expect("s1");
        let was = objects_in(&dest);

        // Подделываем оборванную попытку.
        let junk = format!("{dest}/{OBJECTS}/.partial-999-seg0000-deadbeef");
        std::fs::create_dir_all(&junk).unwrap();
        std::fs::write(format!("{junk}/кусок"), "недописано".as_bytes()).unwrap();
        assert_eq!(objects_in(&dest), was, "оборванная попытка сочлась объектом");

        // Повторное снятие её не замечает и работает как ни в чём не бывало.
        create_incremental(&ix, &dest, "s2", Effort::Fast).expect("s2");
        assert_eq!(objects_in(&dest), was, "повтор завёл лишние объекты");

        // И уборка её убирает, не трогая настоящих.
        let (_, freed) = prune(&dest, 5).expect("уборка");
        assert_eq!(freed, 1, "оборванная попытка не убралась");
        assert!(!std::path::Path::new(&junk).exists());
        assert_eq!(objects_in(&dest), was, "уборка забрала настоящий объект");
        std::fs::remove_dir_all(&ix).ok();
        std::fs::remove_dir_all(&dest).ok();
    }

    /// Ключ объекта меняется, когда меняется сегмент, и только тогда.
    #[test]
    fn the_key_follows_the_segment_identity() {
        let ix = tmp("key-ix");
        corpus(&ix, 0, 100, 1000);
        let names = crate::manifest::segments(&ix);
        assert!(!names.is_empty());
        let seg = crate::manifest::name_of(&names[0]);
        let a = object_key(&ix, &seg);
        let b = object_key(&ix, &seg);
        assert_eq!(a, b, "ключ не воспроизводим");
        assert!(a.starts_with(&seg), "ключ не назвал сегмент: {a}");
        // Сегмента нет вовсе — ключ всё равно считается, но он другой.
        let missing = object_key(&ix, "seg9999");
        assert_ne!(missing, a);
        std::fs::remove_dir_all(&ix).ok();
    }
}
