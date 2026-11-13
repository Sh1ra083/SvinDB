//! Каталог идентификаторов: где искать документ, не спрашивая каждый сегмент.
//!
//! ## Зачем он
//!
//! Чтение по идентификатору обходит **все** сегменты: у каждого свой фильтр
//! Блума и своё отображение, и вопрос «есть ли здесь такой» задаётся каждому.
//! Пока сегментов десятки, это терпимо. На тысячах — а тысячи получаются из
//! миллиардов документов — обход сам становится ответом на вопрос о скорости:
//! замерено, что сегмент стоит запросу микросекунду **до всякой полезной
//! работы**, и тысяча сегментов это миллисекунда на каждое удаление и каждую
//! замену, сколько бы документов в них ни было.
//!
//! Каталог отвечает на тот же вопрос одним чтением: где лежит этот
//! идентификатор.
//!
//! ## Чем он **не** является
//!
//! Это **производный кеш**, и у него ровно одно право — ускорять. Изменить
//! ответ он не вправе ни при каких обстоятельствах:
//!
//! * его может не быть вовсе;
//! * он может устареть — состав изменился, а его не пересобрали;
//! * он может быть обрезан на середине записи;
//! * его содержимое может быть испорчено побайтово.
//!
//! Во **всех** четырёх случаях ответ обязан совпасть с ответом без каталога.
//! Отсюда устройство: каталог не отвечает «документ такой-то», он лишь говорит
//! «посмотри вот здесь», а посмотревший **сверяет** — читает настоящий
//! идентификатор по названному месту и сравнивает. Не сошлось — каталог для
//! этого запроса не существует, и работает обычный обход.
//!
//! Поэтому у него нет ни своего порядка `fsync`, ни записи в журнал, ни участия
//! в восстановлении. Потерять его нельзя — нечего терять.
//!
//! ## Устройство
//!
//! Записи отсортированы по хешу идентификатора, по шестнадцать байт каждая:
//! хеш, номер сегмента в составе, местный номер документа. Файл разбит на
//! страницы; резидентно держится только **решётка** — первый хеш каждой
//! страницы. Поиск: двоичный поиск по решётке даёт страницу, дальше одно чтение
//! и просмотр страницы.
//!
//! Решётка стоит восемь байт на страницу: при странице в 256 записей это
//! тридцать два байта на тысячу документов, то есть тридцать два мегабайта на
//! миллиард. Сам каталог на диске — шестнадцать байт на документ.
//!
//! ## Почему хеш, а не сам идентификатор
//!
//! Идентификаторы бывают длинными и разной длины; хранить их значило бы
//! построить второй `ids.bin` рядом с существующими. Хеш постоянной ширины даёт
//! и сортировку, и решётку, и сравнение одной операцией. Совпадения хешей
//! разрешает сверка — та самая, без которой каталогу нельзя верить в принципе.

/// Записей на страницу. Степень двойки — чтобы номер страницы был сдвигом.
///
/// Двести пятьдесят шесть записей это четыре килобайта: одна страница памяти на
/// большинстве машин и одно обращение к диску.
pub const PER_PAGE: usize = 256;

/// Байт на запись: хеш (8), номер сегмента (4), местный номер (4).
const ENTRY: usize = 16;

/// Текущий формат с числом покрытых сегментов.
const MAGIC: &[u8; 8] = b"svindbc2";

/// Каталог SvinDB 1.4.1. В нём нет явного числа покрытых сегментов, но его
/// stamp позволяет безопасно восстановить покрытый префикс после append-only
/// публикаций. Это делает upgrade быстрым сразу, без многогигабайтной
/// синхронной пересборки каталога на старте.
const LEGACY_MAGIC: &[u8; 8] = b"svindbc1";

/// Байт заголовка: подпись, число записей, подпись состава и число покрытых
/// сегментов. Последнее делает каталог устойчивым к append: неизменившийся
/// префикс остаётся валиден, а новые L0-сегменты проверяются отдельно.
const HEAD: usize = 8 + 8 + 8 + 8;
const LEGACY_HEAD: usize = 8 + 8 + 8;

/// Имя файла каталога в каталоге индекса.
pub const FILE: &str = "catalog.bin";

/// Подпись состава: по ней каталог узнаёт, что он устарел.
///
/// Считается по тождеству каждого сегмента — тому же, каким снимки узнают, что
/// каталог сегмента не менялся. Сегмент неизменяем после записи заголовка,
/// поэтому совпадение подписи означает, что состав тот же самый по составу и по
/// порядку.
///
/// Порядок входит в подпись намеренно: номер сегмента в записи каталога — это
/// место в составе, и переставь их местами, каталог указывал бы в чужой сегмент.
pub fn stamp(idents: &[(u64, i64)]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for (i, (ino, mtime)) in idents.iter().enumerate() {
        h = stamp_one(h, i, (*ino, *mtime));
    }
    // Ноль зарезервирован под «подписи нет»: файл без подписи не должен
    // случайно совпасть с настоящим составом.
    if h == 0 {
        1
    } else {
        h
    }
}

fn stamp_one(mut h: u64, index: usize, ident: (u64, i64)) -> u64 {
    for x in [index as u64, ident.0, ident.1 as u64] {
        h ^= x;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

fn normalized_stamp(h: u64) -> u64 {
    if h == 0 {
        1
    } else {
        h
    }
}

/// Находит самый короткий префикс нынешнего состава с legacy stamp.
///
/// Самый короткий безопаснее при теоретическом совпадении 64-битной подписи:
/// лишние сегменты тогда попадут в точный tail scan. Слишком длинный префикс,
/// наоборот, мог бы пропустить документ.
fn legacy_covered(idents: &[(u64, i64)], want: u64) -> Option<usize> {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for (i, ident) in idents.iter().copied().enumerate() {
        h = stamp_one(h, i, ident);
        if normalized_stamp(h) == want {
            return Some(i + 1);
        }
    }
    None
}

/// Собирает каталог из готовых записей.
///
/// `entries` — тройки «хеш, номер сегмента, местный номер». Порядок неважен:
/// здесь они и сортируются.
pub fn encode(mut entries: Vec<(u64, u32, u32)>, stamp: u64) -> Vec<u8> {
    encode_covered(&mut entries, stamp, 0)
}

fn encode_covered(entries: &mut [(u64, u32, u32)], stamp: u64, covered: usize) -> Vec<u8> {
    entries.sort_unstable();
    let mut out = Vec::with_capacity(HEAD + entries.len() * ENTRY);
    out.extend_from_slice(MAGIC);
    out.extend((entries.len() as u64).to_le_bytes());
    out.extend(stamp.to_le_bytes());
    out.extend((covered as u64).to_le_bytes());
    for (h, seg, doc) in entries.iter() {
        out.extend(h.to_le_bytes());
        out.extend(seg.to_le_bytes());
        out.extend(doc.to_le_bytes());
    }
    out
}

/// Открытый каталог: файл на диске и решётка в памяти.
pub struct Catalog {
    map: crate::mmap::Mapped,
    count: usize,
    /// Первый хеш каждой страницы. Единственное, что живёт резидентно.
    grid: Vec<u64>,
    stamp: u64,
    covered: usize,
    head: usize,
    legacy: bool,
}

impl Catalog {
    /// Открывает каталог и сверяет его с составом.
    ///
    /// `None` — каталога нет, он не тот, устарел или не сходится. Все четыре
    /// случая для вызывающего одно и то же: работать без каталога.
    ///
    /// Проверки здесь — не педантизм. Каталог адресует **чужие** сегменты
    /// номерами, и обрезанный файл, прочитанный как целый, направил бы сверку в
    /// произвольное место. Сверка это переживёт — она на то и есть, — но платить
    /// за неё на каждом запросе не за что.
    pub fn open(dir: &str, want: u64) -> Option<Catalog> {
        let cat = Self::read(dir)?;
        (cat.stamp == want).then_some(cat)
    }

    /// Открывает каталог, покрывающий неизменившийся префикс состава.
    ///
    /// Возвращаемое число — первый сегмент хвоста, который вызывающий обязан
    /// проверить обычным точным путём. Ноль в старых тестовых каталогах не
    /// считается покрытием: их можно открыть только точным [`Catalog::open`].
    pub fn open_prefix(dir: &str, idents: &[(u64, i64)]) -> Option<(Catalog, usize)> {
        Self::open_prefix_guarded(dir, idents, None)
    }

    pub fn open_prefix_guarded(
        dir: &str,
        idents: &[(u64, i64)],
        guard: Option<&crate::work::Guard>,
    ) -> Option<(Catalog, usize)> {
        let mut cat = Self::read_with(dir, || guard.is_none_or(|g| g.check()))?;
        let covered = if cat.legacy { legacy_covered(idents, cat.stamp)? } else { cat.covered };
        if covered == 0 || covered > idents.len() || stamp(&idents[..covered]) != cat.stamp {
            return None;
        }
        cat.covered = covered;
        Some((cat, covered))
    }

    fn read(dir: &str) -> Option<Catalog> {
        Self::read_with(dir, || true)
    }

    /// Проверка перед каждым потенциальным page fault. Отменённая решётка
    /// остаётся локальной и никогда не становится опубликованным каталогом.
    fn read_with(dir: &str, mut checkpoint: impl FnMut() -> bool) -> Option<Catalog> {
        if !checkpoint() {
            return None;
        }
        let path = format!("{dir}/{FILE}");
        let map = crate::mmap::Mapped::open(&path).ok()?;
        let len = map.len_bytes();
        if len < LEGACY_HEAD {
            return None;
        }
        let legacy = map.slice(0, 8) == LEGACY_MAGIC;
        let head = if map.slice(0, 8) == MAGIC {
            HEAD
        } else if legacy {
            LEGACY_HEAD
        } else {
            return None;
        };
        if len < head {
            return None;
        }
        let count = map.u64_at(8) as usize;
        let stamp = map.u64_at(16);
        let covered = if legacy { 0 } else { usize::try_from(map.u64_at(24)).ok()? };
        // Файл обязан содержать ровно столько, сколько обещает заголовок.
        // Обрезанный отвергается целиком, а не читается до места обрыва:
        // «прочитано сколько было» — это тот самый молчаливый неверный ответ,
        // от которого каталог и защищается сверкой, только дороже.
        let expected = head.checked_add(count.checked_mul(ENTRY)?)?;
        if len < expected {
            return None;
        }
        let pages = count.div_ceil(PER_PAGE);
        let mut grid = Vec::with_capacity(pages);
        for p in 0..pages {
            if !checkpoint() {
                return None;
            }
            let hash = map.u64_at(head + p * PER_PAGE * ENTRY);
            // Проверяем порядок в том же отменяемом проходе, без второго
            // полного обхода уже построенной решётки.
            if grid.last().is_some_and(|previous| *previous > hash) {
                return None;
            }
            grid.push(hash);
        }
        crate::stats::catalog_opened(1);
        Some(Catalog { map, count, grid, stamp, covered, head, legacy })
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Сколько байт держится резидентно.
    pub fn resident(&self) -> usize {
        self.grid.len() * 8
    }

    /// Куда смотреть за этим хешем: пары «сегмент, местный номер».
    ///
    /// Отдаёт **кандидатов**, а не ответ. Совпадение хешей здесь законно и
    /// ожидаемо: разрешает его сверка у вызывающего.
    pub fn look(&self, h: u64, out: &mut Vec<(u32, u32)>) {
        out.clear();
        if self.count == 0 {
            return;
        }
        // Страница, где хеш может начаться: последняя, чей первый ключ его не
        // превосходит. Записи с одним хешем соседствуют, но могут перейти на
        // следующую страницу — поэтому просмотр идёт вперёд, пока хеш тот же.
        let mut p = match self.grid.binary_search(&h) {
            Ok(at) => at,
            Err(0) => return,
            Err(at) => at - 1,
        };
        // Тот же хеш мог начаться и на предыдущих страницах, если он занимает
        // их целиком: отступаем, пока первый ключ страницы равен искомому.
        while p > 0 && self.grid[p] == h {
            p -= 1;
        }
        let mut at = p * PER_PAGE;
        while at < self.count {
            let off = self.head + at * ENTRY;
            let got = self.map.u64_at(off);
            if got > h {
                break;
            }
            if got == h {
                out.push((self.map.u32_at(off + 8), self.map.u32_at(off + 12)));
            }
            at += 1;
        }
    }
}

/// Сколько сегментов покрывает существующий каталог без построения его
/// резидентной решётки.
///
/// Maintenance спрашивает только заголовок, чтобы не перечитывать страницу из
/// каждого гигабайтного каталога каждые две секунды. Полная проверка порядка и
/// точная сверка по-прежнему выполняются на настоящем point read.
pub fn coverage(dir: &str, idents: &[(u64, i64)]) -> Option<usize> {
    use std::io::Read;

    let path = format!("{dir}/{FILE}");
    let mut file = std::fs::File::open(path).ok()?;
    let mut header = [0u8; HEAD];
    file.read_exact(&mut header[..LEGACY_HEAD]).ok()?;
    let legacy = &header[..8] == LEGACY_MAGIC;
    let head = if &header[..8] == MAGIC {
        file.read_exact(&mut header[LEGACY_HEAD..HEAD]).ok()?;
        HEAD
    } else if legacy {
        LEGACY_HEAD
    } else {
        return None;
    };
    let count = u64::from_le_bytes(header[8..16].try_into().ok()?);
    let want_len = (head as u64).checked_add(count.checked_mul(ENTRY as u64)?)?;
    if file.metadata().ok()?.len() < want_len {
        return None;
    }
    let got_stamp = u64::from_le_bytes(header[16..24].try_into().ok()?);
    let covered = if legacy {
        legacy_covered(idents, got_stamp)?
    } else {
        usize::try_from(u64::from_le_bytes(header[24..32].try_into().ok()?)).ok()?
    };
    if covered == 0 || covered > idents.len() || stamp(&idents[..covered]) != got_stamp {
        return None;
    }
    Some(covered)
}

/// Собирает каталог по готовому составу.
///
/// Записи берутся из отображений самих сегментов: каталог ничего нового не
/// узнаёт, он лишь складывает известное в один отсортированный файл. Отсюда и
/// его безобидность — он производен по построению, а не по обещанию.
pub fn build(sources: &[&crate::index::Index]) -> Vec<u8> {
    let mut entries: Vec<(u64, u32, u32)> = Vec::new();
    for (s, ix) in sources.iter().enumerate() {
        if ix.idmap.is_empty() {
            continue;
        }
        ix.idmap.each(&mut |h, doc| entries.push((h, s as u32, doc)));
    }
    let idents: Vec<(u64, i64)> = sources.iter().map(|ix| ix.head_id).collect();
    encode_covered(&mut entries, stamp(&idents), sources.len())
}

/// Где искать идентификатор — **со сверкой**.
///
/// ## Почему сверка не необязательна
///
/// Каталог называет место, и место может быть любым: он устарел, обрезан,
/// испорчен или просто содержит другой идентификатор с тем же хешем. Приняв его
/// слово, гашение убило бы невиновного — не отказом, а тихо.
///
/// Поэтому здесь читается **настоящий** идентификатор по названному месту и
/// сравнивается со спрошенным. Не сошлось — места нет.
///
/// `None` — каталог этому запросу не помог, и вызывающий обязан пойти обычным
/// обходом. Именно `None`, а не пустой список: «каталог не знает» и «такого
/// документа нет» — разные вещи, и путать их значит терять документы.
pub fn locate(
    cat: &Catalog,
    sources: &[&crate::index::Index],
    id: &str,
    h: u64,
    scratch: &mut Vec<(u32, u32)>,
) -> Option<Vec<(usize, u32)>> {
    cat.look(h, scratch);
    let mut out: Vec<(usize, u32)> = Vec::new();
    for &(seg, doc) in scratch.iter() {
        let at = seg as usize;
        // Номер сегмента вне состава — каталог не тот. Дальше идти нельзя: он
        // мог разойтись с составом и в тех записях, что ещё указывают внутрь.
        let ix = sources.get(at)?;
        match ix.idmap.id_at(doc) {
            Some(real) if real == id => out.push((at, doc)),
            // Не тот идентификатор — это законно: хеши совпадают. Просто не наш.
            Some(_) => {}
            // Место названо, а прочесть его нечем: каталог разошёлся с сегментом.
            None => return None,
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy_encode(mut entries: Vec<(u64, u32, u32)>, stamp: u64) -> Vec<u8> {
        entries.sort_unstable();
        let mut out = Vec::with_capacity(LEGACY_HEAD + entries.len() * ENTRY);
        out.extend_from_slice(LEGACY_MAGIC);
        out.extend((entries.len() as u64).to_le_bytes());
        out.extend(stamp.to_le_bytes());
        for (hash, segment, doc) in entries {
            out.extend(hash.to_le_bytes());
            out.extend(segment.to_le_bytes());
            out.extend(doc.to_le_bytes());
        }
        out
    }

    fn write(dir: &str, bytes: &[u8]) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(format!("{dir}/{FILE}"), bytes).unwrap();
    }

    fn tmp(tag: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-catalog-{tag}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        d
    }

    /// Каталог находит то, что в нём есть, и молчит про то, чего нет.
    #[test]
    fn cancelling_a_partly_built_grid_discards_it_and_allows_retry() {
        let dir = tmp("cancel-grid");
        let entries: Vec<_> = (0..PER_PAGE * 64).map(|n| (n as u64, 0, n as u32)).collect();
        write(&dir, &encode(entries, 7));
        let guard = crate::work::Guard::new(0);
        let mut checkpoints = 0;
        let stopped = Catalog::read_with(&dir, || {
            checkpoints += 1;
            if checkpoints == 18 {
                guard.cancel();
            }
            guard.check()
        });
        assert!(stopped.is_none(), "частично открытый каталог опубликован");
        assert_eq!(checkpoints, 18, "проверки не проходят через чтение страниц");
        let complete = Catalog::open(&dir, 7).expect("отмена испортила повторное открытие");
        assert_eq!(complete.grid.len(), 64);
        let mut found = Vec::new();
        complete.look((PER_PAGE * 63) as u64, &mut found);
        assert_eq!(found, vec![(0, (PER_PAGE * 63) as u32)]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn it_finds_what_it_holds() {
        let dir = tmp("basic");
        // Больше страницы, чтобы решётка была не из одного элемента.
        let n = PER_PAGE * 5 + 7;
        let entries: Vec<(u64, u32, u32)> = (0..n)
            .map(|i| (crate::bloom::hash(&format!("d{i}")), (i % 4) as u32, i as u32))
            .collect();
        write(&dir, &encode(entries.clone(), 42));
        let c = Catalog::open(&dir, 42).expect("каталог открылся");
        assert_eq!(c.len(), n);

        let mut out = Vec::new();
        for (h, seg, doc) in entries.iter() {
            c.look(*h, &mut out);
            assert!(
                out.contains(&(*seg, *doc)),
                "хеш {h:#x} не нашёлся: отдано {out:?}, ждали ({seg}, {doc})"
            );
        }
        // Чего нет — того нет, и это не отказ.
        c.look(crate::bloom::hash("такого-нет-совсем"), &mut out);
        assert!(out.is_empty(), "нашлось несуществующее: {out:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Один хеш у нескольких документов — все кандидаты обязаны выйти наружу.
    ///
    /// Совпадение хешей здесь не редкость и не ошибка: у одного идентификатора
    /// бывает несколько версий в разных сегментах, и разрешает их сверка. Отдай
    /// каталог только первого — сверка отбросила бы его как чужого и объявила
    /// документ отсутствующим.
    #[test]
    fn every_holder_of_a_hash_comes_out() {
        let dir = tmp("dup");
        let h = crate::bloom::hash("общий");
        let mut entries: Vec<(u64, u32, u32)> =
            (0..PER_PAGE * 3).map(|i| (h, 0, i as u32)).collect();
        entries.push((crate::bloom::hash("другой"), 1, 7));
        write(&dir, &encode(entries, 1));
        let c = Catalog::open(&dir, 1).expect("каталог открылся");
        let mut out = Vec::new();
        c.look(h, &mut out);
        assert_eq!(out.len(), PER_PAGE * 3, "часть держателей хеша потерялась");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Каталог от другого состава не открывается вовсе.
    #[test]
    fn a_catalog_of_another_composition_is_refused() {
        let dir = tmp("stale");
        write(&dir, &encode(vec![(1, 0, 0)], 111));
        assert!(Catalog::open(&dir, 111).is_some(), "свой состав обязан открыться");
        assert!(Catalog::open(&dir, 222).is_none(), "чужой состав открылся");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn append_keeps_a_covered_prefix_generation_valid() {
        let dir = tmp("prefix");
        let base = [(11, 101), (22, 202)];
        let mut entries = vec![(7, 0, 1), (9, 1, 2)];
        write(&dir, &encode_covered(&mut entries, stamp(&base), base.len()));

        let appended = [(11, 101), (22, 202), (33, 303), (44, 404)];
        let (_, covered) = Catalog::open_prefix(&dir, &appended).expect("префикс открылся");
        assert_eq!(covered, 2);

        let reordered = [(22, 202), (11, 101), (33, 303)];
        assert!(Catalog::open_prefix(&dir, &reordered).is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_1_4_catalog_becomes_an_append_safe_prefix_without_rebuild() {
        let dir = tmp("legacy-prefix");
        let base = [(11, 101), (22, 202), (33, 303)];
        let entries = vec![(7, 0, 1), (9, 1, 2), (11, 2, 3)];
        write(&dir, &legacy_encode(entries, stamp(&base)));

        let appended = [(11, 101), (22, 202), (33, 303), (44, 404), (55, 505)];
        let (catalog, covered) =
            Catalog::open_prefix(&dir, &appended).expect("legacy-префикс открылся");
        assert_eq!(covered, base.len());
        assert_eq!(catalog.covered, base.len());
        let mut places = Vec::new();
        catalog.look(9, &mut places);
        assert!(places.contains(&(1, 2)), "legacy entry прочитана с неверного смещения");
        assert_eq!(coverage(&dir, &appended), Some(base.len()));

        let changed = [(11, 101), (99, 999), (33, 303), (44, 404)];
        assert!(Catalog::open_prefix(&dir, &changed).is_none());
        assert_eq!(coverage(&dir, &changed), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Обрезанный, испорченный и чужой файл отвергаются, а не читаются.
    #[test]
    fn a_broken_catalog_is_refused_not_read() {
        let dir = tmp("broken");
        // Хеши начинаются не с нуля: иначе «меньше первого» не выразить.
        let whole = encode((0..PER_PAGE * 2).map(|i| (1000 + i as u64, 0, i as u32)).collect(), 9);

        // Обрезанный на середине.
        write(&dir, &whole[..whole.len() - ENTRY - 3]);
        assert!(Catalog::open(&dir, 9).is_none(), "обрезанный каталог открылся");

        // Чужая подпись файла.
        let mut alien = whole.clone();
        alien[0] = b'X';
        write(&dir, &alien);
        assert!(Catalog::open(&dir, 9).is_none(), "чужой файл открылся");

        // Порядок нарушен: первый ключ второй страницы меньше первого.
        let mut jumbled = whole.clone();
        let second = HEAD + PER_PAGE * ENTRY;
        jumbled[second..second + 8].copy_from_slice(&0u64.to_le_bytes());
        write(&dir, &jumbled);
        assert!(Catalog::open(&dir, 9).is_none(), "каталог с нарушенным порядком открылся");

        // Пустой файл и отсутствующий — тоже просто «нет каталога».
        write(&dir, &[]);
        assert!(Catalog::open(&dir, 9).is_none());
        std::fs::remove_file(format!("{dir}/{FILE}")).ok();
        assert!(Catalog::open(&dir, 9).is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Подпись состава меняется от порядка сегментов, а не только от их набора.
    ///
    /// Номер сегмента в записи — это **место** в составе. Переставь состав, и
    /// та же запись укажет в другой сегмент; подпись обязана это заметить.
    #[test]
    fn the_stamp_notices_a_reordering() {
        let a = stamp(&[(1, 10), (2, 20)]);
        let b = stamp(&[(2, 20), (1, 10)]);
        assert_ne!(a, b, "перестановка состава не изменила подписи");
        assert_eq!(a, stamp(&[(1, 10), (2, 20)]), "подпись не воспроизводима");
        assert_ne!(a, stamp(&[(1, 10)]), "укороченный состав дал ту же подпись");
        assert_ne!(0, stamp(&[]), "пустая подпись равна «подписи нет»");
    }
}
