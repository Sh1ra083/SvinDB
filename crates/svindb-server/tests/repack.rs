//! `svindb unpack` и `svindb pack`: вход оператора к раскладке контейнера.
//!
//! Сверяется не то, что команды отработали, а то, что после них **индекс
//! отвечает то же самое**. Опись кусков может сойтись и у сегмента, который
//! перестал искать: кусок, уехавший в контейнер под чужим именем, даст ту же
//! сводку и молча пустую выдачу.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

struct Stand {
    dir: String,
    idx: String,
}

impl Drop for Stand {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("свободный порт")
        .local_addr()
        .expect("адрес")
        .port()
}

/// Индекс из `docs` документов, нарезанный по `per_segment`.
fn stand(name: &str, docs: usize, per_segment: usize) -> Stand {
    let dir = std::env::temp_dir()
        .join(format!("svindb-repack-{name}-{}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).expect("каталог стенда");
    let ndjson = format!("{dir}/c.ndjson");
    let mut f = std::io::BufWriter::new(std::fs::File::create(&ndjson).expect("корпус"));
    for i in 0..docs {
        writeln!(
            f,
            "{{\"id\":\"d{i}\",\"title\":\"т{i}\",\"body\":\"дом у дороги {i} общее слово\"}}"
        )
        .expect("строка корпуса");
    }
    drop(f);
    let idx = format!("{dir}/index");
    let out = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["index", &ndjson, &idx, &per_segment.to_string()])
        .output()
        .expect("сборка индекса");
    assert!(out.status.success(), "индекс не собрался: {out:?}");
    Stand { dir, idx }
}

/// Запросы, которыми сверяется индекс до и после перекладки.
///
/// Их несколько, и это не щедрость: запрос по словам читает списки, словарь и
/// нормы — и **не** читает ни хранимое содержимое, ни карту идентификаторов.
/// Кусок, потерянный или переехавший под чужим именем, такой запрос назвал бы
/// «ничего не изменилось». Поэтому каждый запрос ходит в свои куски: слова — в
/// списки и словарь, фраза — в позиции (`pos.bin`), выдача полей — в хранимое
/// (`.sbin`, `.smrk`, `.sdic`), `ids` — в карту идентификаторов, подсветка — в
/// позиции вместе с хранимым.
const QUERIES: [&str; 4] = [
    "{\"k\":10,\"words\":[{\"field\":1,\"terms\":[\"дом\",\"дороги\"]}],\"ids\":true}",
    "{\"k\":10,\"phrases\":[{\"field\":1,\"terms\":[\"дом\",\"у\",\"дороги\"],\"slop\":0}]}",
    "{\"k\":5,\"words\":[{\"field\":1,\"terms\":[\"общее\"]}],\"fields\":[\"title\",\"body\"]}",
    "{\"k\":5,\"words\":[{\"field\":1,\"terms\":[\"слово\"]}],\"highlight\":{\"fields\":[\"body\"]}}",
];

/// Время и наблюдаемый I/O зависят от прогрева и не являются результатом поиска.
/// Остальные поля, включая hits, полноту и логические счётчики, сверяем целиком.
fn comparable_reply(reply: &str) -> serde_json::Value {
    let mut value: serde_json::Value = serde_json::from_str(reply).expect("корректный JSON ответа");
    value.as_object_mut().expect("объект ответа").remove("took_ms");
    if let Some(stats) = value.get_mut("stats").and_then(serde_json::Value::as_object_mut) {
        for key in
            ["core_ms", "hydrate_ms", "aggregate_ms", "thread_read_bytes", "thread_write_bytes"]
        {
            stats.remove(key);
        }
    }
    value
}

#[test]
fn comparison_ignores_timing_but_keeps_document_and_completion_changes() {
    let a = r#"{"took_ms":1,"hits":[{"id":"a","score":2}],"complete":true,"stats":{"core_ms":1,"source_bytes":3}}"#;
    let b = a.replace("\"took_ms\":1", "\"took_ms\":9").replace("\"core_ms\":1", "\"core_ms\":7");
    assert_eq!(comparable_reply(a), comparable_reply(&b));
    for changed in [
        a.replace("\"id\":\"a\"", "\"id\":\"b\""),
        a.replace("\"complete\":true", "\"complete\":false"),
        a.replace("\"source_bytes\":3", "\"source_bytes\":4"),
    ] {
        assert_ne!(comparable_reply(a), comparable_reply(&changed));
    }
}

/// Поднимает узел, задаёт все запросы [`QUERIES`] и гасит узел.
///
/// Узел поднимается и гасится на каждый замер, потому что перекладка правит
/// каталог сегмента: держать его открытым в это время значило бы мерить не то,
/// что лежит на диске.
fn ask(idx: &str) -> String {
    let port = free_port();
    let mut child = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["serve", idx, &port.to_string(), "--read-only"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("узел");
    let mut out = String::new();
    for (n, body) in QUERIES.iter().enumerate() {
        let mut answer = None;
        for _ in 0..200 {
            if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
                let req = format!(
                    "POST / HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                s.write_all(req.as_bytes()).expect("запрос");
                let mut got = String::new();
                let mut r = BufReader::new(s);
                let mut line = String::new();
                // Заголовки до пустой строки, дальше тело: сверяется именно оно.
                while r.read_line(&mut line).unwrap_or(0) > 0 {
                    if line == "\r\n" {
                        break;
                    }
                    line.clear();
                }
                r.read_to_string(&mut got).expect("тело ответа");
                answer = Some(got);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        let got = answer.expect("узел не поднялся");
        out.push_str(&format!("[{n}] {}\n", comparable_reply(&got)));
    }
    let _ = child.kill();
    let _ = child.wait();
    out
}

fn run(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(args)
        .output()
        .expect("запуск команды");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// Сводка сумм сегмента строками «имя куска — длина и сумма».
///
/// По ней и видно, тронут сегмент или нет: подмена куска и упаковка обе
/// переписывают сводку, а имена в ней говорят и о виде сегмента — у упакованного
/// куски идут под `seg.dat/`, у рассыпанного своими именами.
fn sums(seg: &str) -> Vec<(String, String)> {
    std::fs::read_to_string(format!("{seg}/sums.tsv"))
        .expect("сводка")
        .lines()
        .filter_map(|l| l.split_once('\t').map(|(n, rest)| (n.to_string(), rest.to_string())))
        .collect()
}

/// Строка сводки про кусок с таким именем — или паника с внятным текстом.
fn sum_of(seg: &str, piece: &str) -> String {
    sums(seg)
        .into_iter()
        .find(|(n, _)| n == piece)
        .map(|(_, rest)| rest)
        .unwrap_or_else(|| panic!("в сводке {seg} нет куска {piece}"))
}

fn names_in(dir: &str) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .expect("каталог сегмента")
        .map(|e| e.expect("запись").file_name().to_string_lossy().to_string())
        .collect();
    v.sort();
    v
}

/// Раскладка и обратная упаковка не меняют в индексе ничего, кроме вида файлов.
#[test]
fn an_index_answers_the_same_after_unpacking_and_packing() {
    let s = stand("roundtrip", 60, 20);
    let seg = format!("{}/seg0000", s.idx);
    let before = ask(&s.idx);
    // Каждый запрос обязан что-то найти: молчаливо пустая выдача сошлась бы сама
    // с собой и до, и после перекладки, ничего не проверив.
    for (n, line) in before.lines().enumerate() {
        assert!(line.contains("\"doc\""), "запрос {n} ничего не нашёл: {line}");
    }
    assert!(before.contains("\"id\""), "идентификаторы не дошли до ответа: {before}");
    assert!(before.contains("\"fields\""), "хранимое не дошло до ответа: {before}");
    assert_eq!(
        names_in(&seg),
        vec!["head.tsv", "live.bin", "seg.dat", "sums.tsv"],
        "приём собрал сегмент не контейнером"
    );

    let (ok, said, err) = run(&["unpack", &s.idx]);
    assert!(ok, "раскладка отказала: {said}{err}");
    assert!(said.contains("seg0000: разложен"), "раскладка не отчиталась по сегменту: {said}");
    assert!(!names_in(&seg).contains(&"seg.dat".to_string()), "контейнер остался");
    assert!(names_in(&seg).contains(&"postings.bin".to_string()), "куски не легли файлами");
    let (ok, said, _) = run(&["check", &s.idx, "--full"]);
    assert!(ok, "разложенный индекс объявлен сломанным: {said}");
    assert_eq!(ask(&s.idx), before, "россыпь отвечает не то, что контейнер");

    // Второй заход не работа: сегмент уже в нужном виде, и трогать его нельзя.
    let (ok, said, _) = run(&["unpack", &s.idx]);
    assert!(ok, "повторная раскладка отказала: {said}");
    assert!(said.contains("уже разложен"), "повторная раскладка сделала вид, что работала: {said}");

    let (ok, said, err) = run(&["pack", &s.idx]);
    assert!(ok, "упаковка отказала: {said}{err}");
    assert!(said.contains("seg0000: упакован"), "упаковка не отчиталась по сегменту: {said}");
    assert_eq!(
        names_in(&seg),
        vec!["head.tsv", "live.bin", "seg.dat", "sums.tsv"],
        "рядом с контейнером остались разложенные файлы"
    );
    let (ok, said, _) = run(&["check", &s.idx, "--full"]);
    assert!(ok, "упакованный заново индекс объявлен сломанным: {said}");
    assert_eq!(ask(&s.idx), before, "после перекладки выдача изменилась");

    let (ok, said, _) = run(&["pack", &s.idx]);
    assert!(ok, "повторная упаковка отказала: {said}");
    assert!(said.contains("уже упакован"), "повторная упаковка сделала вид, что работала: {said}");
}

/// Обрыв подмены куска чинится перекладкой — так, как это обещано текстом.
///
/// Подмена куска (`container::replace`, она же `svindb bloom` на упакованном
/// сегменте) меняет два файла: `seg.dat` и длину в `head.tsv`. Двух файлов одним
/// движением файловая система не меняет, и между ними остаётся окно: контейнер
/// новый и целый, а заголовок называет прежнюю длину. Обещано, что такой сегмент
/// не открывается **внятно** и чинится раскладкой с обратной упаковкой — потому
/// что раскладка смотрит на сам контейнер, а не на обещание заголовка. Здесь это
/// обещание и проверяется: без него текст был бы утешением, а не ремонтом.
#[test]
fn an_interrupted_piece_swap_is_named_and_then_repaired_by_a_round_trip() {
    let s = stand("window", 60, 20);
    let seg = format!("{}/seg0001", s.idx);
    let before = ask(&s.idx);

    // Ровно то состояние, в каком оставил бы обрыв: длина в заголовке отстала.
    let head = format!("{seg}/head.tsv");
    let text = std::fs::read_to_string(&head).expect("заголовок");
    let (before_line, rest) = text.split_once('\n').expect("заголовок из одной строки");
    let fields: Vec<&str> = before_line.split('\t').collect();
    assert_eq!(fields.first().copied(), Some("container"), "строка container не первая: {text}");
    let was: u64 = fields.get(2).and_then(|x| x.parse().ok()).expect("длина контейнера");
    std::fs::write(&head, format!("container\tseg.dat\t{}\n{rest}", was - 64)).expect("заголовок");

    // Проверка обязана назвать обе длины: это единственное, по чему оператор
    // поймёт, что случилось.
    let (ok, said, _) = run(&["check", &s.idx, "--full"]);
    assert!(!ok, "расхождение длин объявлено целым индексом: {said}");
    assert!(said.contains("обещает"), "проверка не назвала расхождение: {said}");
    assert!(said.contains(&was.to_string()), "проверка не назвала настоящую длину: {said}");

    // И `bloom` не станет пересобирать поверх недоделанного — причём не тронув
    // ни одного сегмента.
    //
    // Отказывает здесь опрос первым проходом: расхождение длин ловит само
    // открытие сегмента. Поэтому сверяется не код возврата — он был бы красным и
    // от отказа на середине обхода, — а то, что здоровый сегмент, идущий в обходе
    // ПЕРВЫМ, остался с прежним фильтром. Убери опрос, и `seg0000` получил бы
    // новое число бит прежде, чем обход дошёл до оборванного `seg0001`.
    // Собственный сторож `replace` («N байт, а head.tsv обещает M») — второй
    // рубеж, на случай подмены контейнера уже после опроса; отсюда он недостижим
    // и проверяется юнит-тестом `replacing_names_exactly_what_went_wrong`.
    let healthy_was = sums(&format!("{}/seg0000", s.idx));
    let (ok, said, err) = run(&["bloom", &s.idx, "48"]);
    assert!(!ok, "пересборка фильтра пошла поверх оборванной подмены");
    assert!(err.contains("seg0001"), "отказ не назвал сегмент: {err}");
    assert!(!said.contains("пересобраны"), "команда отчиталась о работе: {said}");
    assert_eq!(
        sums(&format!("{}/seg0000", s.idx)),
        healthy_was,
        "фильтр пересобран у здорового сегмента прежде, чем обход встал на оборванном"
    );
    // След обрыва на месте: заголовок по-прежнему отстаёт. Затри его — и о том,
    // что сегмент недоделан, узнать было бы нечем.
    assert!(
        std::fs::read_to_string(&head).expect("заголовок").contains(&(was - 64).to_string()),
        "отказавшая пересборка всё-таки переписала отставший заголовок"
    );

    // Ремонт: раскладка читает контейнер, сверяя суммы, обратная упаковка
    // возвращает вид — и заголовок снова говорит правду.
    let (ok, said, err) = run(&["unpack", &s.idx]);
    assert!(ok, "раскладка не починила отставший заголовок: {said}{err}");
    let (ok, said, err) = run(&["pack", &s.idx]);
    assert!(ok, "обратная упаковка отказала: {said}{err}");
    let (ok, said, _) = run(&["check", &s.idx, "--full"]);
    assert!(ok, "после ремонта индекс всё ещё сломан: {said}");
    assert_eq!(ask(&s.idx), before, "ремонт изменил выдачу");
}

/// Перекладка не идёт вторым писателем по каталогу.
///
/// ## Что ломалось бы без замка
///
/// Упаковка подставляет `seg.dat` переименованием, а во временном имени стоит
/// номер процесса — то есть два одновременных захода **никогда** не столкнутся
/// именами: каждый доведёт свой контейнер до диска, каждый переименует, и
/// `seg.dat` останется от одного захода, а длина в `head.tsv` — от другого.
/// Сегмент после такого не открывается. По той же причине перекладку нельзя
/// пускать по каталогу живого пишущего узла и по каталогу идущего слияния: они
/// держат этот же замок, а окно между переименованием и заголовком открыто для
/// каждого, кто в это время переоткрывает сегменты.
///
/// Здесь замок держит сам тест — тем же `WriterLock`, каким его берут приём и
/// слияние, — и сверяется не только отказ, но и то, что каталог сегмента не
/// тронут: команда, отказавшая после первого переименования, была бы хуже
/// молчаливой.
#[test]
fn repacking_refuses_while_another_writer_holds_the_directory() {
    let s = stand("lock", 40, 20);
    let seg = format!("{}/seg0000", s.idx);
    let was = names_in(&seg);
    let lock = svindb_core::manifest::WriterLock::take(&s.idx).expect("замок свободен");
    for cmd in ["unpack", "pack"] {
        let (ok, said, err) = run(&[cmd, &s.idx]);
        assert!(!ok, "{cmd} пошла по занятому каталогу: {said}{err}");
        assert!(err.contains("занят писателем"), "{cmd}: отказ не назвал причину: {err}");
        assert!(!said.contains("разложен") && !said.contains("упакован"), "{cmd}: {said}");
        assert_eq!(names_in(&seg), was, "{cmd} тронула сегмент по занятому каталогу");
    }
    // Вторая половина договора: отказ навсегда был бы не защитой, а поломкой.
    drop(lock);
    let (ok, said, err) = run(&["unpack", &s.idx]);
    assert!(ok, "после снятия замка раскладка всё равно отказала: {said}{err}");
    assert!(said.contains("seg0000: разложен"), "раскладка не отчиталась: {said}");
}

/// Смешанный индекс — часть сегментов упакована, часть россыпью — обслуживается
/// целиком, обеими ветками в одном проходе.
///
/// ## Почему это состояние штатное
///
/// Его создаёт любой отказ посреди обхода `pack`/`unpack`: доделанное осталось
/// целым, недоделанное не начато. Именно смешанным состоянием оправданы и опрос
/// первым проходом у `bloom`, и остановка обхода на первом отказе — а до сих пор
/// ни один тест на нём не бывал: все работали либо на полностью упакованном
/// индексе, либо на полностью разложенном.
///
/// ## Что тут проверяется
///
/// Что `bloom` в одном прогоне выбирает разные ветки по разным сегментам —
/// упакованному подменяет кусок внутри контейнера, рассыпанному пишет файл, — и
/// считает первые отдельно; что рядом с контейнером от этого не завелось имени
/// `idmap.bloom` (положи его туда — и настройка молча не подействовала бы, потому
/// что читатель контейнера к файлам рядом не обращается); что `pack` по
/// смешанному индексу докладывает и о работе, и о том, что трогать не стал; и
/// что выдача от всего этого не меняется ни разу.
#[test]
fn a_mixed_index_is_served_by_both_branches_in_one_walk() {
    let s = stand("mixed", 60, 12);
    let packed = format!("{}/seg0000", s.idx);
    let loose = format!("{}/seg0002", s.idx);
    let before = ask(&s.idx);
    assert!(before.contains("\"doc\""), "запросы стенда ничего не нашли: {before}");

    let (ok, said, err) = run(&["unpack", &s.idx]);
    assert!(ok, "раскладка отказала: {said}{err}");

    // Смешанное состояние получается штатным путём: каталог среди кусков — не
    // обычный файл, упаковка встаёт ровно на нём и дальше не идёт.
    let surprise = format!("{loose}/сюрприз");
    std::fs::create_dir(&surprise).expect("каталог-помеха");
    let (ok, said, err) = run(&["pack", &s.idx]);
    assert!(!ok, "упаковка проглотила каталог среди кусков: {said}{err}");
    assert!(err.contains("seg0002"), "отказ не назвал сегмент: {err}");
    assert!(said.contains("seg0000: упакован"), "первые сегменты не упакованы: {said}");
    assert!(said.contains("seg0001: упакован"), "первые сегменты не упакованы: {said}");
    assert!(!said.contains("seg0003"), "обход пошёл дальше отказавшего сегмента: {said}");
    std::fs::remove_dir(&surprise).expect("помеха убрана");

    // Вот он, смешанный индекс: два вида сегмента в одном каталоге.
    assert!(names_in(&packed).contains(&"seg.dat".to_string()), "seg0000 не упакован");
    assert!(!names_in(&loose).contains(&"seg.dat".to_string()), "seg0002 всё-таки упакован");
    assert!(names_in(&loose).contains(&"postings.bin".to_string()), "seg0002 не рассыпан");
    let (ok, said, _) = run(&["check", &s.idx, "--full"]);
    assert!(ok, "смешанный индекс объявлен сломанным: {said}");
    assert_eq!(ask(&s.idx), before, "смешанный индекс отвечает не то же самое");

    // Пересборка фильтров: у упакованного кусок меняется внутри контейнера, у
    // рассыпанного — файлом, и обе ветки идут в одном прогоне.
    let packed_names = names_in(&packed);
    let in_container = sum_of(&packed, "seg.dat/idmap.bloom");
    let as_file = sum_of(&loose, "idmap.bloom");
    let (ok, said, err) = run(&["bloom", &s.idx, "48"]);
    assert!(ok, "пересборка фильтров по смешанному индексу отказала: {said}{err}");
    assert!(
        said.contains("5 сегментов (2 с пересборкой контейнера)"),
        "отчёт не разделил ветки: {said}"
    );
    assert_ne!(
        sum_of(&packed, "seg.dat/idmap.bloom"),
        in_container,
        "у упакованного сегмента кусок внутри контейнера не сменился"
    );
    assert_ne!(sum_of(&loose, "idmap.bloom"), as_file, "у рассыпанного сегмента файл не сменился");
    assert_eq!(
        names_in(&packed),
        packed_names,
        "рядом с контейнером завелось имя: фильтр лёг файлом, которого читатель не увидит"
    );
    let (ok, said, _) = run(&["check", &s.idx, "--full"]);
    assert!(ok, "после пересборки смешанный индекс объявлен сломанным: {said}");
    assert_eq!(ask(&s.idx), before, "пересборка фильтров изменила выдачу");

    // Упаковка по смешанному индексу: часть сегментов работа, часть — нет, и
    // отчёт обязан различать эти два исхода.
    let (ok, said, err) = run(&["pack", &s.idx]);
    assert!(ok, "упаковка смешанного индекса отказала: {said}{err}");
    assert!(said.contains("seg0000: уже упакован"), "об упакованном не сказано: {said}");
    assert!(said.contains("seg0002: упакован,"), "рассыпанный не упакован: {said}");
    assert!(
        said.contains("упаковано: 3 сегментов, 2 и так были в нужном виде"),
        "итог не сошёлся с работой: {said}"
    );
    let (ok, said, _) = run(&["check", &s.idx, "--full"]);
    assert!(ok, "собранный обратно индекс объявлен сломанным: {said}");
    assert_eq!(ask(&s.idx), before, "перекладка смешанного индекса изменила выдачу");
}

/// Отказ на одном сегменте обязан остановить обход и назвать его.
///
/// Полуразобранный индекс хуже неразобранного: часть сегментов рассыпана, часть
/// упакована, и по каталогу этого не видно. Здесь второй сегмент испорчен в
/// теле куска — длина цела, оглавление сходится, доложить о порче может только
/// сумма при раскладке, — и обход обязан на нём встать, а не пойти дальше и не
/// разложить порченое набело.
#[test]
fn a_refusal_stops_the_walk_and_names_the_segment() {
    let s = stand("halt", 60, 20);
    let victim = format!("{}/seg0001", s.idx);

    // Портится байт в середине контейнера — заведомо в теле куска, а не в
    // заголовке и не в оглавлении: они лежат по краям файла.
    let path = format!("{victim}/seg.dat");
    let mut bytes = std::fs::read(&path).expect("контейнер");
    let at = bytes.len() / 2;
    bytes[at] ^= 0x40;
    std::fs::write(&path, &bytes).expect("порча");

    let (ok, said, err) = run(&["unpack", &s.idx]);
    assert!(!ok, "порченый сегмент разложен: {said}{err}");
    assert!(err.contains("seg0001"), "отказ не назвал сегмент: {err}");
    assert!(said.contains("seg0000: разложен"), "первый сегмент не разложен: {said}");
    assert!(!said.contains("seg0002"), "обход пошёл дальше порченого сегмента: {said}");
    // Порченый остался упакованным и нетронутым: раскладка отказывает до того,
    // как заголовок перестанет называть контейнер.
    assert!(names_in(&victim).contains(&"seg.dat".to_string()), "контейнер порченого снесён");
    assert!(
        !names_in(&victim).contains(&"postings.bin".to_string()),
        "куски порченого всё-таки разложены"
    );
}
