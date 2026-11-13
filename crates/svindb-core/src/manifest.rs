//! Список сегментов индекса и его подмена целиком.

use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::Path;

/// Имя файла со списком.
pub const FILE: &str = "segments.tsv";

/// Больше этого список не читается и не пишется.
///
/// ## Зачем предел на обеих сторонах, а не на одной
///
/// Читатель ограничивал себя мегабайтом и без этой константы. Писатель — ничем.
/// Значит индекс, доросший до списка крупнее мегабайта, записал бы его успешно,
/// а прочитать не смог бы уже никогда: чтение отказывает, обход каталога
/// подменяет состав чем попало, а виновата запись, случившаяся часом раньше.
/// Отказ обязан приходить **до** записи и с числом.
pub const MAX_BYTES: u64 = 1 << 20;

/// Больше этого числа сегментов в списке не бывает.
///
/// Выведено из [`MAX_BYTES`] и предела длины имени: имя не длиннее
/// [`MAX_NAME`] плюс перевод строки, значит мегабайта хватает на тридцать одну
/// тысячу с лишним. Округлено вниз до тридцати тысяч — запас на заголовок и на
/// то, чтобы отказ по числу приходил раньше отказа по байтам и звучал понятнее.
///
/// Само число велико нарочно: это не политика, а обрыв. Состав из тридцати
/// тысяч сегментов давно неработоспособен по другим причинам (запрос платит
/// микросекунду на сегмент), и упереться сюда узел обязан не раньше, чем в них.
pub const MAX_SEGMENTS: usize = 30_000;

/// Длиннее этого имя сегмента не бывает.
///
/// Было сто двадцать восемь — с запасом «на всякий случай», которого никто не
/// использовал: все имена, какие создаёт дерево, это приставка из трёх букв и
/// номер. Тридцати двух хватает на приставку и двадцать девять знаков номера,
/// то есть на любой номер, какой вообще влезает в `u64`. Сужение здесь — не
/// экономия байт, а сужение того, что придётся разбирать: чем короче
/// допустимое имя, тем меньше форм у мусора, который читатель обязан узнать.
pub const MAX_NAME: usize = 32;

/// Заголовок списка: поколение, число имён, контрольная сумма имён.
///
/// ## Зачем он, если запись атомарна
///
/// `atomic_write` даёт всё или ничего на уровне переименования, и разорванного
/// файла быть не должно. Но «не должно» — это про исправную файловую систему;
/// заголовок ловит два случая, которых атомарность не покрывает. Первое:
/// усечённый хвост, который **всё ещё разбирается** — имена в нём годные,
/// просто их меньше, и без счётчика такой список выглядит как законный.
/// Второе: тихая порча байта внутри имени, оставившая имя годным по виду.
///
/// Поколение не используется для решений и хранится ради диагностики: по нему
/// видно, сколько раз состав переписывался, — и это первое, что спрашивают,
/// когда индекс ведёт себя так, будто откатился во времени.
const HEAD: &str = "#svindb-manifest";

/// Читает список сегментов. Пустой ответ — списка нет.
pub fn read(dir: &str) -> Vec<String> {
    listed(dir).ok().flatten().unwrap_or_default()
}

/// Подменяет список целиком. `names` — имена каталогов без пути.
pub fn write(dir: &str, names: &[String]) -> std::io::Result<()> {
    // Проверки — **до** единого записанного байта. Отказ после записи означал
    // бы список, который уже лёг на диск и уже нечитаем.
    if names.len() > MAX_SEGMENTS {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "в списке {} сегментов при пределе {MAX_SEGMENTS} — \
                 состав такого размера неработоспособен задолго до этого числа",
                names.len()
            ),
        ));
    }
    let mut names_body = Vec::new();
    for n in names {
        if !valid_segment_name(n) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("негодное имя сегмента {n:?}"),
            ));
        }
        writeln!(names_body, "{n}")?;
    }
    let gen = listed_head(dir).map_or(1, |h| h.generation.saturating_add(1));
    let mut body = Vec::new();
    writeln!(body, "{HEAD}\t{gen}\t{}\t{:08x}", names.len(), crate::wal::crc32(&names_body))?;
    body.extend_from_slice(&names_body);
    if body.len() as u64 > MAX_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "список весит {} Б при пределе {MAX_BYTES} Б — записанный, он не прочтётся",
                body.len()
            ),
        ));
    }
    crate::fs::atomic_write(Path::new(dir).join(FILE).as_path(), &body, 0o600)
}

/// Разобранный заголовок списка.
struct Head {
    generation: u64,
    count: usize,
    crc: u32,
}

fn parse_head(line: &str) -> Option<Head> {
    let mut it = line.split('\t');
    if it.next()? != HEAD {
        return None;
    }
    Some(Head {
        generation: it.next()?.parse().ok()?,
        count: it.next()?.parse().ok()?,
        crc: u32::from_str_radix(it.next()?, 16).ok()?,
    })
}

/// Поколение записанного списка — для диагностики и для следующей записи.
fn listed_head(dir: &str) -> Option<Head> {
    let path = format!("{dir}/{FILE}");
    let bytes = crate::fs::read_regular_limited(Path::new(&path), MAX_BYTES, false).ok()?;
    let text = String::from_utf8(bytes).ok()?;
    parse_head(text.lines().next()?)
}

/// Приставки каталогов сегментов: приём, укрупнение, уборка мёртвых.
///
/// Все три — сегменты, и все три обязаны узнаваться везде, где сегменты ищут.
pub const PREFIXES: [&str; 3] = ["seg", "mrg", "cln"];

/// Похоже ли имя каталога на сегмент.
pub fn looks_like_segment(name: &str) -> bool {
    PREFIXES.iter().any(|p| name.starts_with(p))
}

/// Имя из manifest обязано быть одним компонентом и тем же именем, которое
/// способен создать штатный writer.
pub fn valid_segment_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME
        && looks_like_segment(name)
        && name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        && Path::new(name).file_name().is_some_and(|x| x == name)
}

fn listed(dir: &str) -> Result<Option<Vec<String>>, String> {
    let path = format!("{dir}/{FILE}");
    let bytes = match crate::fs::read_regular_limited(Path::new(&path), MAX_BYTES, false) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{path}: {e}")),
    };
    let text = String::from_utf8(bytes).map_err(|e| format!("{path}: не UTF-8: {e}"))?;
    // Заголовок необязателен: списки, записанные до его появления, читаются как
    // читались. Но если он есть — он обязателен к соблюдению, иначе это украшение.
    let head = text.lines().next().and_then(parse_head);
    let body_from = if head.is_some() { text.find('\n').map_or(text.len(), |i| i + 1) } else { 0 };
    let text = &text[body_from..];
    let mut out = Vec::new();
    // Множество, а не линейный поиск, и по **имени**, а не по пути.
    //
    // Прежняя проверка сравнивала `x == name`, где `x` — это уже собранный
    // `"{dir}/{name}"`, а `name` — голое имя. Полный путь никогда не равен
    // имени, поэтому дубликат не ловился ни разу: сегмент, названный дважды,
    // проходил внутрь, получал два начала в общем пространстве номеров и
    // считался за два. Заодно снимается квадрат: на тысячах сегментов линейный
    // поиск давал миллионы сравнений строк на каждое чтение списка, а читается
    // он в том числе фоновым рескан-потоком.
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for (i, raw) in text.lines().enumerate() {
        let name = raw.trim();
        if name.is_empty() {
            continue;
        }
        if !valid_segment_name(name) {
            return Err(format!("{path}: строка {} содержит негодное имя {name:?}", i + 1));
        }
        if !seen.insert(name) {
            return Err(format!("{path}: сегмент {name:?} назван дважды"));
        }
        out.push(format!("{dir}/{name}"));
    }
    if let Some(h) = head {
        // Счётчик ловит усечённый хвост, который **всё ещё разбирается**: имена
        // в нём годные, просто их меньше, и без этой проверки такой список
        // выглядит как законный состав, из которого просто пропали сегменты.
        if h.count != out.len() {
            return Err(format!(
                "{path}: заголовок обещает {} имён, а их {} — список усечён",
                h.count,
                out.len()
            ));
        }
        // Сумма ловит тихую порчу байта внутри имени, оставившую имя годным по
        // виду: `seg0007` вместо `seg0001` — это другой сегмент, и открылся бы
        // он молча.
        let mut body = Vec::new();
        for name in out.iter() {
            body.extend_from_slice(name_of(name).as_bytes());
            body.push(b'\n');
        }
        let got = crate::wal::crc32(&body);
        if got != h.crc {
            return Err(format!(
                "{path}: контрольная сумма имён {got:08x} против записанной {:08x} — \
                 список испорчен",
                h.crc
            ));
        }
    }
    Ok(Some(out))
}

/// Список сегментов как он есть: `None` — файла нет вовсе.
///
/// Отличать «списка нет» от «список пуст» обязан всякий, кто по отсутствию
/// списка переходит на обход каталога. Пустой список — это законный ноль
/// сегментов у индекса, из которого уборка забрала всё; обход же в этот момент
/// подберёт каталоги, снятые с публикации и ждущие удаления, и объявит их
/// сегментами индекса.
pub fn listing(dir: &str) -> Option<Vec<String>> {
    listed(dir).ok().flatten()
}

/// Закрепляет legacy directory listing до запуска первого нового writer job.
/// Вызывающий держит writer/layout lock. После этого завершённый head.tsv
/// непубликованного flush не попадёт в ReadView через fallback scan.
/// Уже существующий manifest не переписывается; ошибки не превращаются в пустой.
pub(crate) fn ensure_listed(dir: &str) -> std::io::Result<()> {
    if listed(dir).map_err(std::io::Error::other)?.is_some() {
        return Ok(());
    }
    let names = scan(dir).iter().map(|path| name_of(path)).collect::<Vec<_>>();
    write(dir, &names)
}

fn valid_segment_dir(path: &str) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_dir())
        && std::fs::symlink_metadata(Path::new(path).join("head.tsv"))
            .is_ok_and(|m| m.file_type().is_file())
}

/// Приставка и номер имени сегмента: `mrg0007` — это `("mrg", 7)`.
///
/// Ключ сортировки обхода. Хвост, который не разобрался в число, идёт после
/// разобравшихся и упорядочивается строкой — имя всё равно негодное, но порядок
/// обязан остаться устойчивым, а не зависеть от того, что вернул `read_dir`.
fn order_key(path: &str) -> (&'static str, u64, String) {
    let name = name_of(path);
    for p in PREFIXES {
        if let Some(tail) = name.strip_prefix(p) {
            return match tail.parse::<u64>() {
                Ok(n) => (p, n, String::new()),
                Err(_) => (p, u64::MAX, tail.to_string()),
            };
        }
    }
    ("", u64::MAX, name)
}

/// Готовые сегменты каталога — на случай, когда списка ещё нет.
///
/// ## Порядок числовой, а не строковый
///
/// Порядок здесь несущий: из него выводятся начала сегментов в общем
/// пространстве номеров ([`crate::segments::bases`]). Сортировка строкой ставит
/// `seg10000` перед `seg9999`, потому что `'1' < '9'` — и до десятитысячного
/// сегмента это незаметно, а после него **все ранее выданные номера документов
/// и курсоры страниц начинают указывать на другие документы**. Молча: индекс
/// открывается, выдача осмысленна, неверна только привязка.
///
/// Имена перерастают четыре знака сами собой — `format!("{prefix}{n:04}")` в
/// [`crate::maintain`] не ограничивает `n`, ноли лишь дополняют до четырёх.
pub fn scan(dir: &str) -> Vec<String> {
    let mut dirs: Vec<String> = std::fs::read_dir(dir)
        .map(|it| {
            it.filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_type().is_ok_and(|t| t.is_dir())
                        && valid_segment_name(&e.file_name().to_string_lossy())
                        && std::fs::symlink_metadata(e.path().join("head.tsv"))
                            .is_ok_and(|m| m.file_type().is_file())
                })
                .map(|e| e.path().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default();
    dirs.sort_by(|a, b| order_key(a).cmp(&order_key(b)));
    dirs
}

/// Состав индекса: список, если он есть, иначе обход каталога.
pub fn segments(dir: &str) -> Vec<String> {
    match listed(dir) {
        Ok(None) => scan(dir),
        Ok(Some(list)) => list.into_iter().filter(|p| valid_segment_dir(p)).collect(),
        Err(_) => Vec::new(),
    }
}

/// То же, но пропажу считает поломкой, а не поводом промолчать.
///
/// `segments` отфильтровывает названный, но отсутствующий сегмент, и это верно
/// для уборки: там надо знать, что живо **сейчас**. Для открытия индекса —
/// неверно: узел поднимется «здоровым», недосчитавшись документов, а номера
/// оставшихся сдвинутся, потому что они выводятся из порядка и размеров.
///
/// Сдвиг номеров хуже самой пропажи: курсоры страниц, выданные до неё, начнут
/// указывать на другие документы, и заметить это можно будет только по жалобам.
pub fn segments_checked(dir: &str) -> Result<Vec<String>, String> {
    let Some(listed) = listed(dir)? else { return Ok(scan(dir)) };
    if listed.is_empty() {
        return Ok(Vec::new());
    }
    let missing: Vec<String> =
        listed.iter().filter(|p| !valid_segment_dir(p)).map(|p| name_of(p)).collect();
    if !missing.is_empty() {
        return Err(format!(
            "в списке названы сегменты, которых нет на диске: {} — \
             индекс неполон, и открывать его как исправный нельзя",
            missing.join(", ")
        ));
    }
    Ok(listed)
}

/// Убирает каталоги, которых нет в списке.
///
/// Звать **только под замком писателя и только на старте**: в работе каталог вне
/// списка — это, наоборот, слияние в полёте, и удалять его нельзя.
pub fn sweep(dir: &str) -> Vec<(String, u64)> {
    let live: std::collections::HashSet<String> =
        segments(dir).iter().map(|p| name_of(p)).collect();
    let mut gone = Vec::new();
    // Живого не набралось — уборка становится разрушительной, потому что «всё,
    // чего нет в списке» означает «весь индекс».
    if live.is_empty() && !scan(dir).is_empty() {
        return gone;
    }
    if live.is_empty() && !read(dir).is_empty() {
        return gone;
    }
    // Держатели проверяются **после** чтения списка. Обратный порядок оставил
    // бы окно: держатель, взятый между двумя действиями, не увиделся бы, а его
    // хозяин прочёл бы список уже без снесённого сегмента.
    if !pinned(dir).is_empty() {
        return gone;
    }
    let Ok(it) = std::fs::read_dir(dir) else { return gone };
    for e in it.filter_map(|e| e.ok()) {
        let p = e.path();
        let name = p.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
        if !p.is_dir() || live.contains(&name) {
            continue;
        }
        if !looks_like_segment(&name) {
            continue;
        }
        let bytes = std::fs::read_dir(&p)
            .map(|f| {
                f.filter_map(|x| x.ok())
                    .filter_map(|x| x.metadata().ok())
                    .map(|m| m.len())
                    .sum::<u64>()
            })
            .unwrap_or(0);
        if std::fs::remove_dir_all(&p).is_ok() {
            gone.push((name, bytes));
        }
    }
    gone.sort();
    gone
}

/// Имя каталога без пути.
pub fn name_of(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-manifest-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().to_string()
    }

    fn seg(dir: &str, name: &str) {
        std::fs::create_dir_all(format!("{dir}/{name}")).unwrap();
        std::fs::write(format!("{dir}/{name}/head.tsv"), "docs\t1\n").unwrap();
    }

    #[test]
    fn writes_and_reads_in_order() {
        let dir = tmp("order");
        seg(&dir, "seg0000");
        seg(&dir, "seg0001");
        write(&dir, &["seg0001".into(), "seg0000".into()]).unwrap();
        let got: Vec<String> = segments(&dir).iter().map(|p| name_of(p)).collect();
        assert_eq!(got, vec!["seg0001", "seg0000"], "порядок берётся из списка, не из имён");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Нечитаемый список — отказ дописывания, а не публикация состава из одного
    /// сегмента.
    ///
    /// Худшее, что умел этот модуль. `append` брал за основу [`segments`], а тот
    /// при ошибке разбора отдаёт пустоту — и на диск ложился манифест ровно с
    /// одним именем, нашим. Весь прежний состав исчезал из индекса при живых
    /// каталогах, а поводом было что угодно: `EMFILE`, файл больше мегабайта,
    /// одна битая строка.
    ///
    /// Проверяется дубликатом: `listed` считает его поломкой, как и всё
    /// остальное, что не разбирается.
    #[test]
    fn an_unreadable_list_refuses_the_append_instead_of_emptying_the_index() {
        let dir = tmp("append-broken");
        for n in ["seg0000", "seg0001", "seg0002"] {
            seg(&dir, n);
        }
        // Список, который не разбирается: имя названо дважды.
        std::fs::write(format!("{dir}/{FILE}"), "seg0000\nseg0001\nseg0002\nseg0001\n").unwrap();
        let before = std::fs::read_to_string(format!("{dir}/{FILE}")).unwrap();

        seg(&dir, "seg0003");
        let e = append(&dir, "seg0003").expect_err("дописывание обязано отказать");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "получили: {e}");

        let after = std::fs::read_to_string(format!("{dir}/{FILE}")).unwrap();
        assert_eq!(after, before, "список переписан поверх нечитаемого");
        assert!(
            after.lines().filter(|l| !l.trim().is_empty()).count() > 1,
            "состав усох до одной строки: {after:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Сегмент, названный дважды, — поломка списка.
    ///
    /// Проверка на это была, но не работала: сравнивался собранный путь
    /// `"{dir}/{name}"` с голым именем, а они не равны никогда. Дубликат
    /// проходил внутрь и получал два начала в общем пространстве номеров, то
    /// есть считался за два сегмента.
    #[test]
    fn a_segment_named_twice_is_refused() {
        let dir = tmp("dup");
        seg(&dir, "seg0000");
        seg(&dir, "seg0001");
        std::fs::write(format!("{dir}/{FILE}"), "seg0000\nseg0001\nseg0000\n").unwrap();
        let e = segments_checked(&dir).expect_err("дубликат обязан быть отказом");
        assert!(e.contains("seg0000") && e.contains("дважды"), "получили: {e}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Разовый сбой `stat` не вычёркивает сегмент из состава.
    ///
    /// Вторая половина того же: `segments` выбрасывает названный сегмент, у
    /// которого прямо сейчас нет каталога, и раньше дописывание брало этот
    /// отфильтрованный ответ за основу. Здесь каталог унесён насовсем — если
    /// имя переживёт дописывание, переживёт и разовую неудачу `symlink_metadata`.
    #[test]
    fn a_segment_whose_directory_is_missing_survives_an_append() {
        let dir = tmp("append-missing");
        for n in ["seg0000", "seg0001"] {
            seg(&dir, n);
        }
        write(&dir, &["seg0000".into(), "seg0001".into()]).unwrap();
        std::fs::remove_dir_all(format!("{dir}/seg0001")).unwrap();

        seg(&dir, "seg0002");
        append(&dir, "seg0002").expect("дописывание");

        let names: Vec<String> = read(&dir).iter().map(|p| name_of(p)).collect();
        assert_eq!(
            names,
            vec!["seg0000", "seg0001", "seg0002"],
            "пропавший каталог вычеркнул имя из списка"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Обход каталога упорядочивает по числу, а не по строке.
    ///
    /// `seg10000` лексикографически меньше `seg9999`, потому что `'1' < '9'`.
    /// Порядок задаёт начала сегментов в общем пространстве номеров, поэтому
    /// строковая сортировка молча сдвигает **все** ранее выданные номера
    /// документов и курсоры страниц.
    #[test]
    fn scanning_orders_by_number_not_by_string() {
        let dir = tmp("order-10k");
        for n in ["seg0002", "seg9999", "seg10000", "seg10001"] {
            seg(&dir, n);
        }
        let got: Vec<String> = scan(&dir).iter().map(|p| name_of(p)).collect();
        assert_eq!(
            got,
            vec!["seg0002", "seg9999", "seg10000", "seg10001"],
            "порядок строковый, а не числовой"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Приставки по-прежнему группируются, и порядок между ними прежний.
    #[test]
    fn scanning_keeps_prefixes_grouped() {
        let dir = tmp("order-prefix");
        for n in ["seg0002", "mrg0000", "cln0001", "mrg10000", "mrg9999"] {
            seg(&dir, n);
        }
        let got: Vec<String> = scan(&dir).iter().map(|p| name_of(p)).collect();
        assert_eq!(got, vec!["cln0001", "mrg0000", "mrg9999", "mrg10000", "seg0002"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Без списка — обход каталога, и только по готовым сегментам.
    #[test]
    fn falls_back_to_scanning() {
        let dir = tmp("scan");
        seg(&dir, "seg0001");
        seg(&dir, "seg0000");
        // Заполняемый сегмент: каталог есть, заголовка нет.
        std::fs::create_dir_all(format!("{dir}/seg0002")).unwrap();
        let got: Vec<String> = segments(&dir).iter().map(|p| name_of(p)).collect();
        assert_eq!(got, vec!["seg0000", "seg0001"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Сегмент, упомянутый в списке, но ещё не дописанный, не показывается.
    #[test]
    fn listed_but_unfinished_is_skipped() {
        let dir = tmp("half");
        seg(&dir, "seg0000");
        std::fs::create_dir_all(format!("{dir}/mrg0000")).unwrap();
        write(&dir, &["seg0000".into(), "mrg0000".into()]).unwrap();
        let got: Vec<String> = segments(&dir).iter().map(|p| name_of(p)).collect();
        assert_eq!(got, vec!["seg0000"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn manifest_refuses_traversal_and_linked_segments() {
        use std::os::unix::fs::symlink;
        let dir = tmp("traversal");
        std::fs::write(format!("{dir}/{FILE}"), "../seg0000\n").unwrap();
        assert!(segments_checked(&dir).is_err());

        let outside = tmp("outside-segment");
        seg(&outside, "seg0000");
        std::fs::remove_file(format!("{dir}/{FILE}")).unwrap();
        symlink(format!("{outside}/seg0000"), format!("{dir}/seg0000")).unwrap();
        std::fs::write(format!("{dir}/{FILE}"), "seg0000\n").unwrap();
        assert!(segments_checked(&dir).is_err());
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&outside).ok();
    }

    /// Замок не зависит от того, что написано в файле замка.
    #[test]
    fn the_lock_does_not_depend_on_what_the_file_says() {
        let dir = tmp("lock");
        let path = format!("{dir}/.writer.lock");
        // Каталог занят замком от процесса, которого нет.
        std::fs::write(&path, "999999\n").unwrap();
        let first = WriterLock::take(&dir).expect("брошенный замок перехватывается");

        // А теперь кто-то — например, второй процесс, успевший решить, что
        // замок брошен, — кладёт в файл мёртвый номер.
        std::fs::write(&path, "999999\n").unwrap();

        let second = WriterLock::take(&dir);
        let e = second.err().expect("второй писатель обязан получить отказ");
        assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock, "получили: {e}");

        // Замок отпущен — следующий его берёт. Вторая половина договора: отказ
        // навсегда был бы не защитой, а поломкой.
        drop(first);
        drop(WriterLock::take(&dir).expect("после снятия замок свободен"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn writer_lock_does_not_follow_a_symlink() {
        use std::os::unix::fs::symlink;
        let dir = tmp("lock-link");
        let target = format!("{dir}/outside");
        std::fs::write(&target, b"untouched").unwrap();
        symlink(&target, format!("{dir}/.writer.lock")).unwrap();
        assert!(WriterLock::take(&dir).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"untouched");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn readers_share_the_lock_but_exclude_a_lifecycle_writer() {
        let dir = tmp("reader-lock");
        // Писатель создаёт сам lock-файл; читатель намеренно не мутирует
        // каталог ради собственного старта.
        let writer = WriterLock::take(&dir).unwrap();
        assert!(ReaderLock::take(&dir).is_err(), "читатель вошёл под DELETE/CREATE");
        drop(writer);

        let first = ReaderLock::take(&dir).unwrap();
        let second = ReaderLock::take(&dir).unwrap();
        assert!(WriterLock::take(&dir).is_err(), "писатель вошёл под живых читателей");
        drop((first, second));
        assert!(WriterLock::take(&dir).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Подмена не оставляет промежуточного состояния.
    #[test]
    fn replacement_is_all_or_nothing() {
        let dir = tmp("swap");
        seg(&dir, "seg0000");
        seg(&dir, "seg0001");
        seg(&dir, "mrg0000");
        write(&dir, &["seg0000".into(), "seg0001".into()]).unwrap();
        write(&dir, &["mrg0000".into()]).unwrap();
        let got: Vec<String> = segments(&dir).iter().map(|p| name_of(p)).collect();
        assert_eq!(got, vec!["mrg0000"]);
        assert!(!std::path::Path::new(&format!("{dir}/.{FILE}.tmp")).exists());
        std::fs::remove_dir_all(&dir).ok();
    }
}

extern "C" {
    fn flock(fd: i32, op: i32) -> i32;
}

/// Взять исключительно и не ждать: `LOCK_EX | LOCK_NB`.
const LOCK_EX_NB: i32 = 2 | 4;
/// Взять совместно и не ждать: `LOCK_SH | LOCK_NB`.
const LOCK_SH_NB: i32 = 1 | 4;

/// Замок писателя: двух пишущих по одному каталогу не бывает.
///
/// Приём и слияние меняют список сегментов, и оба делают это
/// чтением-правкой-записью. Без замка два писателя затёрли бы правки друг друга,
/// и часть сегментов пропала бы из индекса — данные на диске целы, а найти их
/// нельзя. Перекладка (`svindb pack`/`unpack`) и пересборка фильтров (`svindb
/// bloom`) списка не трогают, но подставляют `seg.dat` переименованием и
/// переписывают `head.tsv`, то есть занимают то же место: попади чужой писатель
/// в окно между этими двумя движениями — и сегмент не откроется.
///
/// Держит замок **ядро**, а не содержимое файла.
///
/// `flock` снимает и гонку, и само понятие брошенного замка. Захват неделим —
/// либо он твой, либо нет; а при смерти процесса ядро отпускает его само,
/// закрывая дескриптор. Проверять чужую живость больше не нужно: если файл
/// заперт, значит запиравший жив.
pub struct WriterLock {
    /// Держится открытым всю жизнь замка: закрытие дескриптора снимает `flock`.
    _file: std::fs::File,
}

impl WriterLock {
    pub fn take(dir: &str) -> std::io::Result<WriterLock> {
        std::fs::create_dir_all(dir)?;
        let path = format!("{dir}/.writer.lock");
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        #[cfg(target_os = "macos")]
        const O_NOFOLLOW: i32 = 0x0000_0100;
        #[cfg(target_os = "linux")]
        const O_NOFOLLOW: i32 = 0x0002_0000;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(O_NOFOLLOW)
            .open(&path)?;
        let meta = file.metadata()?;
        if !meta.file_type().is_file() || meta.nlink() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{path}: замок не является отдельным обычным файлом"),
            ));
        }
        // Захват до записи номера: иначе номер успел бы затереть чужой у того,
        // кто замок не получил.
        if unsafe { flock(file.as_raw_fd(), LOCK_EX_NB) } != 0 {
            let held = std::fs::read_to_string(&path)
                .ok()
                .and_then(|s| s.trim().parse::<i32>().ok())
                .map(|p| format!("с номером {p}"))
                .unwrap_or_else(|| "неизвестным".into());
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                format!(
                    "каталог {dir} занят писателем {held}. Пишущие по одному каталогу \
                     идут по одному: приём, слияние, перекладка и пересборка фильтров"
                ),
            ));
        }
        // Номер — надпись для чужого сообщения об отказе, а не механизм.
        let mut f = &file;
        let _ = f.set_len(0);
        let _ = writeln!(f, "{}", std::process::id());
        Ok(WriterLock { _file: file })
    }
}

/// Совместный замок читателя того же каталога.
///
/// Нужен верхнему уровню multi-index: несколько read-only процессов могут
/// читать неизменный каталог вместе, но писатель lifecycle не вправе удалить и
/// заново создать индекс под ними. Оба вида используют один `.writer.lock`,
/// поэтому взаимоисключение обеспечивает ядро, а не проверка PID.
pub struct ReaderLock {
    _file: std::fs::File,
}

impl ReaderLock {
    pub fn take(dir: &str) -> std::io::Result<ReaderLock> {
        let path = format!("{dir}/.writer.lock");
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        #[cfg(target_os = "macos")]
        const O_NOFOLLOW: i32 = 0x0000_0100;
        #[cfg(target_os = "linux")]
        const O_NOFOLLOW: i32 = 0x0002_0000;
        let file = std::fs::OpenOptions::new().read(true).custom_flags(O_NOFOLLOW).open(&path)?;
        let meta = file.metadata()?;
        if !meta.file_type().is_file() || meta.nlink() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{path}: замок не является отдельным обычным файлом"),
            ));
        }
        if unsafe { flock(file.as_raw_fd(), LOCK_SH_NB) } != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                format!(
                    "каталог {dir} занят писателем lifecycle; read-only процесс не откроет состав, который может измениться у него под руками"
                ),
            ));
        }
        Ok(ReaderLock { _file: file })
    }
}

// Снимается закрытием дескриптора — своим `Drop` у `File`, в том числе при
// панике и при завершении процесса. Отдельный `Drop` здесь был бы удалением
// файла, а удалять его нельзя: между `open` и `flock` чужого процесса файл
// исчез бы, тот запер бы уже отвязанный inode и решил, что каталог его.

/// Каталог с держателями.
pub const PINS: &str = ".pins";

/// Держатель: пока он жив, из каталога индекса ничего не удаляется.
///
/// ## Зачем
///
/// Сегменты неизменяемы, список подменяется атомарно — но укрупнение сносит
/// каталоги исходных сегментов через `grace_ms` (две секунды по умолчанию), а
/// копирование гигабайтного файла на снимок идёт дольше. Читателю, который
/// держит файл открытым, это не мешает — на Unix отвязанный файл живёт до
/// закрытия, — а копирующему мешает: он открывает файлы по одному и на
/// втором получает «нет такого файла».
///
/// ## Почему держатель огульный, а не по списку сегментов
///
/// Держатель берётся **до** чтения списка, и именно в этом порядке весь смысл:
/// раз с момента взятия не удаляется ничего, всё, что окажется в прочитанном
/// списке, гарантированно лежит на месте. Держатель со списком пришлось бы
/// сперва прочитать список, а между чтением и записью держателя как раз и
/// открывается окно, которое он закрывает.
///
/// Плата — отложенная уборка: пока идёт снимок, укрупнённые каталоги копятся.
/// Их подберёт `sweep` следующим заходом, так что теряется не место, а время
/// его возврата.
///
/// ## Почему `flock`, а не запись «я жив»
///
/// Убитый процесс не убирает за собой. Отметка времени потребовала бы порога
/// «сколько ждать», а любой порог — это либо ложное освобождение под нагрузкой,
/// либо вечно занятый индекс после падения. `flock` снимает вопрос: ядро само
/// отпускает замок при закрытии дескриптора, в том числе при `kill -9`.
pub struct Pin {
    /// Держится открытым всю жизнь: закрытие дескриптора снимает `flock`.
    _file: std::fs::File,
    path: String,
}

static PIN_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl Pin {
    /// Берёт держателя. `why` попадает в файл и в отчёт `pinned` — чтобы
    /// оператор видел, кто держит уборку, а не только что она стоит.
    pub fn hold(dir: &str, why: &str) -> std::io::Result<Pin> {
        let pins = format!("{dir}/{PINS}");
        std::fs::create_dir_all(&pins)?;
        let n = PIN_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = format!("{}-{n}", std::process::id());
        let tmp = format!("{pins}/.tmp-{id}");
        let path = format!("{pins}/{id}.pin");
        use std::os::unix::fs::OpenOptionsExt;
        #[cfg(target_os = "macos")]
        const O_NOFOLLOW: i32 = 0x0000_0100;
        #[cfg(target_os = "linux")]
        const O_NOFOLLOW: i32 = 0x0002_0000;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(O_NOFOLLOW)
            .open(&tmp)?;
        // Замок берётся **до** переименования, и в этом порядке тоже смысл:
        // всякий файл, видимый под своим именем, уже заперт. Иначе сторож,
        // застав держателя между `open` и `flock`, заперся бы сам, счёл бы
        // держателя брошенным и удалил файл у живого — ровно та ошибка, от
        // которой предостерегает примечание к `WriterLock`.
        if unsafe { flock(file.as_raw_fd(), LOCK_EX_NB) } != 0 {
            let _ = std::fs::remove_file(&tmp);
            return Err(std::io::Error::other(format!("держатель {id} не заперся")));
        }
        {
            let mut f = &file;
            writeln!(f, "{}\t{}", std::process::id(), why.replace(['\n', '\t'], " "))?;
            f.flush()?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(Pin { _file: file, path })
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        // Удалять можно — в отличие от замка писателя: под своим именем файл
        // виден только уже запертым, так что чужого `open` между созданием и
        // захватом не бывает.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Живые держатели каталога: по строке-причине на каждого. Пусто — уборка
/// свободна.
///
/// Заброшенные файлы (процесс умер, замок отпущен ядром) не только не считаются
/// живыми, но и убираются, иначе каталог зарастал бы после каждого падения.
pub fn pinned(dir: &str) -> Vec<String> {
    let pins = format!("{dir}/{PINS}");
    let Ok(it) = std::fs::read_dir(&pins) else { return Vec::new() };
    let mut held = Vec::new();
    for e in it.filter_map(|e| e.ok()) {
        let p = e.path();
        if p.extension().map(|x| x != "pin").unwrap_or(true) {
            continue;
        }
        let Ok(f) = std::fs::File::open(&p) else { continue };
        if unsafe { flock(f.as_raw_fd(), LOCK_EX_NB) } == 0 {
            // Заперлись сами — значит держателя нет.
            let _ = std::fs::remove_file(&p);
            continue;
        }
        let why = std::fs::read_to_string(&p).unwrap_or_default();
        held.push(why.trim().to_string());
    }
    held
}

/// Добавляет сегмент в конец списка.
///
/// ## Почему не через [`segments`]
///
/// `segments` отвечает на вопрос «что живо **сейчас**» и ради этого делает две
/// вещи, которые для дописывания разрушительны: при нечитаемом списке отдаёт
/// пустоту, а из читаемого выбрасывает сегменты, чей `stat` не прошёл прямо
/// сейчас. Дописывание берёт этот ответ за основу нового списка — и обе
/// поблажки превращаются в потерю.
///
/// Первая хуже: список не читается при `EMFILE`, при файле больше мегабайта,
/// при любой битой строке. Пустой ответ плюс наше имя — и на диск ложится
/// манифест **из одного сегмента**, а весь прежний состав исчезает из индекса.
/// Каталоги при этом целы, но поиск их не видит, и до ближайшей уборки они
/// доживают уже как мусор вне списка.
///
/// Вторая тише: `symlink_metadata` может не пройти разово, и тогда живой
/// сегмент вычёркивается из состава при следующем же дописывании.
///
/// Поэтому здесь читается сырой список: отсутствие — это законный случай
/// (первый сегмент индекса, обход каталога), а нечитаемость — отказ. Лучше не
/// опубликовать новый сегмент, чем опубликовать состав без старых.
pub fn append(dir: &str, name: &str) -> std::io::Result<()> {
    let broken = |e: String| std::io::Error::new(std::io::ErrorKind::InvalidData, e);
    let mut names: Vec<String> = match listed(dir).map_err(broken)? {
        // Списка ещё нет — собираем обходом, как и прежде: так индекс, набранный
        // до появления списка, продолжает работать.
        None => scan(dir).iter().map(|p| name_of(p)).collect(),
        // Список есть — берём его **как есть**, без проверки каталогов.
        Some(list) => list.iter().map(|p| name_of(p)).collect(),
    };
    if !names.iter().any(|n| n == name) {
        names.push(name.to_string());
    }
    write(dir, &names)
}

#[cfg(test)]
mod limit_tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-manifest-lim-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Записанный список обязан прочитаться. Предел стоит на обеих сторонах.
    ///
    /// Пока предел был только у читателя, индекс, доросший до списка крупнее
    /// мегабайта, записывал его успешно, а прочитать не мог уже никогда:
    /// чтение отказывает, обход каталога подменяет состав чем попало, а
    /// виновата запись, случившаяся часом раньше.
    #[test]
    fn a_list_that_cannot_be_read_is_not_written() {
        let d = tmp("too-many");
        let many: Vec<String> = (0..MAX_SEGMENTS + 1).map(|i| format!("seg{i:07}")).collect();
        let e = write(&d, &many).expect_err("список сверх предела записался");
        assert!(format!("{e}").contains("при пределе"), "отказ без числа: {e}");
        // И ничего не записано: отказ до первого байта, а не после.
        assert!(!Path::new(&format!("{d}/{FILE}")).exists(), "негодный список всё же лёг");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Сам предел списка — рабочее значение, а не только число в отказе.
    ///
    /// Имена берутся максимальной разрешённой длины: это одновременно
    /// доказывает, что выведенные друг из друга пределы числа и байт не
    /// противоречат друг другу на худшем законном входе.
    #[test]
    fn the_manifest_limit_roundtrips_at_the_exact_boundary() {
        let d = tmp("exact-limit");
        let names: Vec<String> = (0..MAX_SEGMENTS).map(|i| format!("seg{i:029}")).collect();
        assert!(names.iter().all(|name| name.len() == MAX_NAME));
        write(&d, &names).expect("список на самом пределе обязан записаться");
        let path = Path::new(&d).join(FILE);
        assert!(
            path.metadata().expect("метаданные списка").len() <= MAX_BYTES,
            "законный список оказался больше предела чтения"
        );
        let read = listed(&d).expect("список не прочитан").expect("списка нет");
        assert_eq!(read.len(), MAX_SEGMENTS);
        assert_eq!(name_of(&read[0]), names[0]);
        assert_eq!(name_of(&read[MAX_SEGMENTS - 1]), names[MAX_SEGMENTS - 1]);
        std::fs::remove_dir_all(&d).ok();
    }

    /// Читатель не выделяет память по файлу произвольного размера.
    #[test]
    fn an_oversized_manifest_is_refused_before_parsing() {
        let d = tmp("oversized-bytes");
        let path = Path::new(&d).join(FILE);
        std::fs::write(&path, vec![b'x'; MAX_BYTES as usize + 1]).unwrap();
        let error = listed(&d).expect_err("файл сверх предела прочитан");
        assert!(error.contains(FILE), "отказ не называет файл: {error}");
        assert!(
            error.contains(&MAX_BYTES.to_string()) || error.contains("предел"),
            "отказ не объясняет предел: {error}"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Усечённый хвост списка ловится счётчиком, а не проходит как законный.
    #[test]
    fn a_truncated_list_is_refused_not_believed() {
        let d = tmp("cut");
        let names: Vec<String> = (0..5).map(|i| format!("seg{i:04}")).collect();
        write(&d, &names).unwrap();
        let path = format!("{d}/{FILE}");
        let text = std::fs::read_to_string(&path).unwrap();
        // Убираем последнее имя, оставляя заголовок и остальные строки целыми:
        // такой файл разбирается без единой ошибки и выглядит как состав из
        // четырёх сегментов.
        let cut: Vec<&str> = text.lines().take(text.lines().count() - 1).collect();
        std::fs::write(&path, cut.join("\n") + "\n").unwrap();
        let e = listed(&d).expect_err("усечённый список принят за законный");
        assert!(e.contains("усечён"), "отказ не про усечение: {e}");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Порча байта внутри имени ловится суммой, а не открывается молча.
    #[test]
    fn a_silently_corrupted_name_is_refused() {
        let d = tmp("rot");
        write(&d, &["seg0001".to_string(), "seg0002".to_string()]).unwrap();
        let path = format!("{d}/{FILE}");
        let text = std::fs::read_to_string(&path).unwrap();
        // `seg0001` -> `seg0007`: имя осталось годным по виду, но это другой
        // сегмент, и без суммы он открылся бы без единого слова.
        std::fs::write(&path, text.replace("seg0001", "seg0007")).unwrap();
        let e = listed(&d).expect_err("испорченное имя принято");
        assert!(e.contains("контрольная сумма"), "отказ не про сумму: {e}");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Список без заголовка читается как читался: старые индексы не ломаются.
    #[test]
    fn a_list_written_before_the_header_still_reads() {
        let d = tmp("legacy");
        std::fs::write(format!("{d}/{FILE}"), "seg0000\nseg0001\n").unwrap();
        let got = listed(&d).expect("старый список отвергнут").expect("списка нет");
        assert_eq!(got.len(), 2, "старый список прочёлся неверно");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Поколение растёт с каждой перезаписью: по нему видно историю состава.
    #[test]
    fn the_generation_counts_the_rewrites() {
        let d = tmp("gen");
        write(&d, &["seg0000".to_string()]).unwrap();
        assert_eq!(listed_head(&d).expect("заголовка нет").generation, 1);
        write(&d, &["seg0000".to_string(), "seg0001".to_string()]).unwrap();
        assert_eq!(listed_head(&d).expect("заголовка нет").generation, 2);
        std::fs::remove_dir_all(&d).ok();
    }
}

#[cfg(test)]
mod sweep_tests {
    use super::*;

    fn seg_dir(dir: &str, name: &str) {
        std::fs::create_dir_all(format!("{dir}/{name}")).unwrap();
        std::fs::write(format!("{dir}/{name}/head.tsv"), "docs\t1\n").unwrap();
    }

    /// Пока держатель жив, уборка не трогает **ничего** — даже то, что в списке
    /// не значится. Иначе снимок, читающий список после взятия держателя,
    /// увидел бы в нём сегмент, каталог которого уже снесён.
    #[test]
    fn a_held_directory_is_not_swept() {
        let d = tmp("held");
        std::fs::create_dir_all(&d).unwrap();
        seg_dir(&d, "seg0000");
        seg_dir(&d, "seg0001");
        write(&d, &["seg0000".to_string()]).unwrap();

        let pin = Pin::hold(&d, "проверка").unwrap();
        assert_eq!(pinned(&d).len(), 1, "держатель не виден");
        assert!(sweep(&d).is_empty(), "уборка прошла сквозь держателя");
        assert!(Path::new(&format!("{d}/seg0001")).exists());

        drop(pin);
        assert!(pinned(&d).is_empty(), "держатель не снялся");
        let gone = sweep(&d);
        assert_eq!(gone.len(), 1, "после снятия держателя уборка обязана пройти");
        assert_eq!(gone[0].0, "seg0001");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Держатель мёртвого процесса не держит. Проверяется файлом без замка —
    /// ровно то, что остаётся после `kill -9`.
    #[test]
    fn a_dead_holder_does_not_hold() {
        let d = tmp("dead");
        std::fs::create_dir_all(format!("{d}/{PINS}")).unwrap();
        let orphan = format!("{d}/{PINS}/999999-0.pin");
        std::fs::write(&orphan, "999999\tтруп\n").unwrap();
        assert!(pinned(&d).is_empty(), "брошенный файл сошёл за живого держателя");
        assert!(!Path::new(&orphan).exists(), "брошенный файл не убран — каталог зарастёт");
        std::fs::remove_dir_all(&d).ok();
    }

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-sweep-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().to_string()
    }

    fn seg(dir: &str, name: &str) {
        std::fs::create_dir_all(format!("{dir}/{name}")).unwrap();
        std::fs::write(format!("{dir}/{name}/head.tsv"), "docs\t1\n").unwrap();
    }

    /// Потеря списка не должна стирать индекс.
    ///
    /// Проверяются обе половины: что состав виден и что уборка молчит.
    #[test]
    fn a_lost_list_does_not_cost_the_index() {
        let dir = tmp("lost-list");
        for name in ["mrg0000", "cln0001", "seg0002"] {
            seg(&dir, name);
        }
        // Списка нет вовсе — как после потери файла.
        assert!(read(&dir).is_empty());

        let found: Vec<String> = scan(&dir).iter().map(|p| name_of(p)).collect();
        assert_eq!(found, vec!["cln0001", "mrg0000", "seg0002"], "обход обязан узнать все три");

        let gone = sweep(&dir);
        assert!(gone.is_empty(), "уборка снесла готовые сегменты: {gone:?}");
        for name in ["mrg0000", "cln0001", "seg0002"] {
            assert!(
                std::path::Path::new(&format!("{dir}/{name}")).exists(),
                "{name} снесён при потерянном списке"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Недописанный каталог при потерянном списке всё же убирается.
    ///
    /// Обратная половина того же договора: сторож стоит на **готовности**, а не
    /// на наличии списка. Каталог без заголовка недописан, и держать его незачем.
    #[test]
    fn an_unfinished_directory_is_still_swept_without_a_list() {
        let dir = tmp("lost-list-half");
        seg(&dir, "seg0000");
        std::fs::create_dir_all(format!("{dir}/mrg0001")).unwrap();
        let gone = sweep(&dir);
        assert_eq!(gone.len(), 1, "недописанный обязан уйти: {gone:?}");
        assert_eq!(gone[0].0, "mrg0001");
        assert!(std::path::Path::new(&format!("{dir}/seg0000")).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Недописанное слияние не должно оставлять после себя место.
    #[test]
    fn incomplete_merge_is_swept_but_listed_segments_survive() {
        let dir = std::env::temp_dir().join("svindb_sweep");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();

        for n in ["seg0000", "seg0001"] {
            std::fs::create_dir_all(dir.join(n)).unwrap();
            std::fs::write(dir.join(n).join("head.tsv"), b"1\n").unwrap();
        }
        // Слияние успело создать каталог и умерло до записи head.tsv.
        std::fs::create_dir_all(dir.join("mrg0000")).unwrap();
        std::fs::write(dir.join("mrg0000").join("postings.bin"), vec![0u8; 4096]).unwrap();
        // Чужой каталог не наш и трогать его нельзя.
        std::fs::create_dir_all(dir.join("backup")).unwrap();

        write(&d, &["seg0000".to_string(), "seg0001".to_string()]).unwrap();
        let gone = sweep(&d);

        assert_eq!(gone.len(), 1, "убрать надо ровно недописанное слияние");
        assert_eq!(gone[0].0, "mrg0000");
        assert_eq!(gone[0].1, 4096, "размер считается по тому, что реально лежало");
        assert!(dir.join("seg0000").exists() && dir.join("seg0001").exists());
        assert!(dir.join("backup").exists(), "чужие каталоги не наше дело");
        assert!(!dir.join("mrg0000").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Испорченный список не должен превращать уборку в снос индекса.
    #[test]
    fn a_broken_list_stops_the_sweep_instead_of_emptying_the_index() {
        let dir = std::env::temp_dir().join("svindb_sweep_broken");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();
        for n in ["seg0000", "seg0001"] {
            std::fs::create_dir_all(dir.join(n)).unwrap();
            std::fs::write(dir.join(n).join("head.tsv"), b"1\n").unwrap();
        }
        // Список называет то, чего нет: ни один сегмент по нему не находится.
        write(&d, &["seg9998".to_string(), "seg9999".to_string()]).unwrap();

        assert!(sweep(&d).is_empty(), "при непонятном списке не трогаем ничего");
        assert!(dir.join("seg0000").exists() && dir.join("seg0001").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod lock_tests {
    use super::*;

    /// Замок мёртвого процесса перехватывается, живого — нет.
    ///
    /// Проверка того, что узел поднимается после `kill -9` сам. Требовать
    /// удаления файла руками значило бы, что журнал приёма бесполезен: он
    /// восстановит всё, до чего дойдёт очередь, а очередь не дойдёт.
    #[test]
    fn a_lock_left_by_a_dead_process_is_taken_over() {
        let dir = std::env::temp_dir().join("svindb-lock-stale");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();
        let path = format!("{d}/.writer.lock");

        // Номер, которого заведомо нет: за пределом допустимых.
        std::fs::write(&path, "2147483000\n").unwrap();
        let lock = WriterLock::take(&d).expect("брошенный замок обязан перехватиться");
        assert_eq!(std::fs::read_to_string(&path).unwrap().trim(), std::process::id().to_string());

        // Свой собственный — живой, и повторный захват обязан отказать.
        let again = WriterLock::take(&d);
        assert!(again.is_err(), "живой замок перехватывать нельзя");
        drop(lock);

        // Пустой замок без номера — тоже брошенный.
        std::fs::write(&path, "").unwrap();
        assert!(WriterLock::take(&d).is_ok());

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod missing_tests {
    /// Сегмент, названный в списке и пропавший с диска, — поломка, а не повод
    /// промолчать.
    #[test]
    fn a_listed_but_missing_segment_is_refused_not_skipped() {
        let dir = std::env::temp_dir().join("svindb-manifest-missing");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();

        for part in 0..3 {
            let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 100);
            for i in 0..20 {
                ix.add(&format!("{part}-{i}"), &[&format!("слово{i}")]);
            }
            ix.flush();
        }
        let names: Vec<String> = super::segments(&d).iter().map(|p| super::name_of(p)).collect();
        assert_eq!(names.len(), 3);
        super::write(&d, &names).unwrap();
        let before = crate::segments::Segments::load(&d).expect("целый индекс").list.len();
        assert_eq!(before, 3);

        // Пропал средний: номера двух оставшихся сдвинулись бы молча.
        std::fs::remove_dir_all(format!("{d}/{}", names[1])).unwrap();
        let e = crate::segments::Segments::load(&d).err().expect("неполный индекс обязан отказать");
        let msg = e.to_string();
        assert!(msg.contains(&names[1]), "отказ не называет пропавший: {msg}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
