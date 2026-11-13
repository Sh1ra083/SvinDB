//! `svindb bloom` на упакованном сегменте: настоящая подмена куска, а не отказ.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_svindb");

/// Запуск команды: код возврата, вывод, поток отказов.
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

/// Строит индекс из `docs` документов и отдаёт путь к нему.
fn corpus(name: &str, docs: usize, per_segment: usize) -> (String, String) {
    let dir = std::env::temp_dir()
        .join(format!("svindb-bloom-{name}-{}", std::process::id()))
        .to_string_lossy()
        .to_string();
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).expect("каталог стенда");
    let ndjson = format!("{dir}/c.ndjson");
    let mut f = std::io::BufWriter::new(std::fs::File::create(&ndjson).expect("корпус"));
    for i in 0..docs {
        writeln!(f, "{{\"id\":\"d{i}\",\"title\":\"т{i}\",\"body\":\"дом у дороги {i}\"}}")
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
    (dir, idx)
}

/// Строки сводки сумм сегмента: имя куска — длина и сумма.
fn sums(seg: &str) -> Vec<(String, String)> {
    std::fs::read_to_string(format!("{seg}/sums.tsv"))
        .expect("сводка")
        .lines()
        .filter_map(|l| l.split_once('\t').map(|(n, rest)| (n.to_string(), rest.to_string())))
        .collect()
}

fn names_in(dir: &str) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .expect("каталог сегмента")
        .map(|e| e.expect("запись").file_name().to_string_lossy().to_string())
        .collect();
    v.sort();
    v
}

/// Настройка обязана действовать, а не печататься.
///
/// ## Что ломалось бы без этого
///
/// Фильтр Блума — кусок сегмента, и у упакованного он лежит внутри `seg.dat`.
/// Пока подмены куска не было, команда отказывала — честно, но на **всяком**
/// боевом сегменте: приём и слияние пишут только контейнеры. То есть числа бит
/// на документ у работающего индекса поменять было нечем вовсе.
///
/// Положить пересобранный фильтр отдельным файлом рядом — не выход, а худший
/// исход: читатель контейнера к файлам рядом не обращается никогда, `check` на
/// такое не жалуется (имя `idmap.bloom` есть в описи известных), и команда
/// печатала бы «фильтры пересобраны», ничего не изменив.
///
/// Поэтому здесь сверяется не код возврата, а последствия: кусок внутри
/// контейнера стал другой длины, соседние куски остались до байта прежними, в
/// каталоге не завелось ни одного нового имени, и `check --full` доволен.
#[test]
fn rebuilding_a_filter_inside_a_container_replaces_the_piece_itself() {
    let (dir, idx) = corpus("packed", 40, 15);
    let seg = format!("{idx}/seg0000");
    let before = names_in(&seg);
    assert!(before.contains(&"seg.dat".to_string()), "приём собрал сегмент россыпью: {before:?}");
    let was = sums(&seg);
    let bloom_was = was
        .iter()
        .find(|(n, _)| n == "seg.dat/idmap.bloom")
        .cloned()
        .expect("фильтра нет в сводке");

    let out = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["bloom", &idx, "48"])
        .output()
        .expect("запуск bloom");
    let said = String::from_utf8_lossy(&out.stdout).to_string();
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(out.status.success(), "пересборка фильтра отказала: {said}{err}");
    assert!(said.contains("пересобраны"), "команда промолчала о работе: {said}");

    // Кусок и правда стал другим — и другой длины, потому что бит на документ
    // теперь больше. Совпади он со старым, вся проверка была бы ни о чём.
    let now = sums(&seg);
    let bloom_now = now
        .iter()
        .find(|(n, _)| n == "seg.dat/idmap.bloom")
        .cloned()
        .expect("фильтр пропал из сводки");
    assert_ne!(bloom_now.1, bloom_was.1, "фильтр в контейнере не изменился");

    // Все прочие куски — до байта прежние: подмена одного не вправе трогать
    // остальные, а перенос их сумм из старого оглавления сверяется по дороге.
    let others = |v: &Vec<(String, String)>| -> Vec<(String, String)> {
        v.iter()
            .filter(|(n, _)| n.starts_with("seg.dat/") && n != "seg.dat/idmap.bloom")
            .cloned()
            .collect()
    };
    assert_eq!(others(&now), others(&was), "подмена фильтра задела соседние куски");

    // Ни одного нового имени: фильтр не лёг рядом, временных остатков нет.
    assert_eq!(names_in(&seg), before, "каталог сегмента обзавёлся лишним");

    let check = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["check", &idx, "--full"])
        .output()
        .expect("проверка");
    assert!(
        check.status.success(),
        "после пересборки индекс объявлен сломанным: {}",
        String::from_utf8_lossy(&check.stdout)
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Пересборка не идёт вторым писателем по каталогу.
///
/// ## Что ломалось бы без замка
///
/// Подмена куска подставляет новый `seg.dat` переименованием, а во временном
/// имени стоит номер процесса — то есть два одновременных захода **никогда** не
/// столкнутся именами: каждый доведёт свой контейнер до диска, каждый
/// переименует, и `seg.dat` останется от одного захода, а длина в `head.tsv` — от
/// другого. Сегмент после такого не открывается вовсе. По той же причине команду
/// нельзя пускать по каталогу живого пишущего узла и по каталогу идущего слияния:
/// они держат этот же замок, а окно между переименованием и заголовком открыто
/// для каждого, кто в это время переоткрывает сегменты.
///
/// Здесь замок держит сам тест — тем же `WriterLock`, каким его берут приём и
/// слияние. Сверяется и отказ, и то, что сегмент не тронут, и то, что после
/// снятия замка команда работает: отказ навсегда был бы не защитой, а поломкой.
#[test]
fn rebuilding_filters_refuses_while_another_writer_holds_the_directory() {
    let (dir, idx) = corpus("lock", 40, 15);
    let seg = format!("{idx}/seg0000");
    let was = sums(&seg);
    let before = names_in(&seg);

    let lock = svindb_core::manifest::WriterLock::take(&idx).expect("замок свободен");
    let out = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["bloom", &idx, "48"])
        .output()
        .expect("запуск bloom");
    let said = String::from_utf8_lossy(&out.stdout).to_string();
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(!out.status.success(), "пересборка пошла по занятому каталогу: {said}{err}");
    assert!(err.contains("занят писателем"), "отказ не назвал причину: {err}");
    assert!(!said.contains("пересобраны"), "команда отчиталась о работе: {said}");
    assert_eq!(sums(&seg), was, "сегмент тронут по занятому каталогу");
    assert_eq!(names_in(&seg), before, "в каталоге сегмента завелось лишнее имя");

    drop(lock);
    let out = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["bloom", &idx, "48"])
        .output()
        .expect("запуск bloom");
    let said = String::from_utf8_lossy(&out.stdout).to_string();
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(out.status.success(), "после снятия замка пересборка всё равно отказала: {said}{err}");
    assert_ne!(sums(&seg), was, "замок снят, а фильтр так и не сменился");
    std::fs::remove_dir_all(&dir).ok();
}

/// Отказ посреди обхода обязан сказать, к скольким сегментам настройка успела
/// примениться.
///
/// ## Почему одного опроса первым проходом мало
///
/// Опрос видит ровно то, что сегмент открывается. Сама пересборка требует
/// большего: полного чтения сегмента и второго его объёма свободного места (у
/// упакованного — целого нового контейнера рядом). Ни ENOSPC, ни EACCES, ни EIO
/// опрос предсказать не может, значит отказ на середине обхода возможен — и тогда
/// у части сегментов новое число бит на документ, а у остальных прежнее. По
/// каталогу этого не видно: у фильтра нет признака, по которому можно спросить,
/// каким числом бит он собран. Отказ, промолчавший об этом, оставил бы оператора
/// думать, что не сделано ничего.
///
/// Отказ здесь настоящий и снаружи: у каталога сегмента снято право записи, так
/// что файл фильтра лечь не может. Сверяется, что доклад назвал число уже
/// пересобранных, что первый сегмент и правда пересобран, а третий — нет.
#[test]
fn a_refusal_midway_reports_how_many_segments_already_took_the_setting() {
    let (dir, idx) = corpus("partial", 45, 15);
    // Пересборка у рассыпанного сегмента — запись файла в каталог, и отнять
    // право записи проще, чем занять диск. Упакованному это не помогло бы:
    // контейнер собирается рядом, и отказ пришёл бы там же.
    let (ok, said, err) = run(&["unpack", &idx]);
    assert!(ok, "раскладка отказала: {said}{err}");

    let first = format!("{idx}/seg0000");
    let victim = format!("{idx}/seg0001");
    let last = format!("{idx}/seg0002");
    let (was_first, was_last) = (sums(&first), sums(&last));

    let ro = std::fs::Permissions::from_mode(0o500);
    std::fs::set_permissions(&victim, ro).expect("права каталога сняты");
    let (ok, said, err) = run(&["bloom", &idx, "48"]);
    // Права возвращаются сразу: без них не убрать ни стенд, ни сам каталог.
    std::fs::set_permissions(&victim, std::fs::Permissions::from_mode(0o700)).expect("права");

    assert!(!ok, "пересборка отчиталась об успехе поверх отказа: {said}{err}");
    assert!(err.contains("seg0001"), "отказ не назвал сегмент: {err}");
    assert!(
        err.contains("применена к 1 сегментам из 3"),
        "отказ промолчал о частичном применении: {err}"
    );
    assert!(!said.contains("пересобраны"), "команда отчиталась о работе целиком: {said}");
    assert_ne!(sums(&first), was_first, "доклад назвал применённым сегмент, который не тронут");
    assert_eq!(sums(&last), was_last, "обход пошёл дальше отказавшего сегмента");
    std::fs::remove_dir_all(&dir).ok();
}

/// Сегмент, который не открывается, обязан остановить работу до первой правки.
///
/// Настройка, применённая к половине сегментов, — состояние, о котором потом
/// никто не догадается: по каталогу не видно, у каких сегментов фильтр
/// пересобран, а у каких нет. Поэтому опрос идёт первым проходом, и здесь
/// сверяется именно это: у здорового сегмента, который в обходе **первый**,
/// фильтр остался прежним.
#[test]
fn a_segment_that_will_not_open_stops_the_work_before_the_first_change() {
    let (dir, idx) = corpus("halt", 40, 15);
    let healthy = format!("{idx}/seg0000");
    let victim = format!("{idx}/seg0001");
    let was = sums(&healthy);

    // Контейнер короче, чем обещает `head.tsv`: сегмент не откроется, а причина
    // будет названа длинами. Порча снаружи, а не выдуманный отказ.
    let path = format!("{victim}/seg.dat");
    let mut bytes = std::fs::read(&path).expect("контейнер");
    bytes.truncate(bytes.len() - 1);
    std::fs::write(&path, &bytes).expect("обрез");

    let out = Command::new(BIN)
        // Запас места сведён к минимуму: проверки поднимают узлы на той же
        // машине, где идёт сборка, а правило «пять процентов ёмкости» на
        // полутерабайтном томе резервирует двадцать три гигабайта. Узел,
        // отказывающий от всякой записи, проверял бы не то, ради чего заведён.
        .env("SVINDB_DISK_FLOOR_MB", "1")
        .args(["bloom", &idx, "48"])
        .output()
        .expect("запуск bloom");
    let said = String::from_utf8_lossy(&out.stdout).to_string();
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(!out.status.success(), "индекс с непригодным сегментом принял пересборку: {said}");
    assert!(err.contains("seg0001"), "отказ не назвал сегмент: {err}");
    assert!(!said.contains("пересобраны"), "команда отчиталась о работе: {said}");
    assert_eq!(sums(&healthy), was, "у здорового сегмента фильтр всё-таки тронут");
    std::fs::remove_dir_all(&dir).ok();
}
