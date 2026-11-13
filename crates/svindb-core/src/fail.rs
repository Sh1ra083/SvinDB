//! Точки отказа: управляемое падение узла ровно там, где это опасно.
//!
//! Кампания на десяти миллионах нашла дубль после `kill -9` одним прогоном и
//! одной случайной точкой во времени. Так дефекты долговечности не проверяют:
//! опасных мест на пути записи около двух десятков, и попасть в конкретное
//! окно случайным сигналом — вопрос везения, а не покрытия. Между публикацией
//! сегмента и переносом отметки журнала проходят миллисекунды; чтобы падение
//! случилось именно там, его надо назвать по имени.
//!
//! # Почему не образец `container::head_fault`
//!
//! В дереве уже есть впрыск отказа — одноразовый `thread_local` под
//! `#[cfg(test)]` ([`crate::container`]). Для своей задачи он верен, но здесь
//! непригоден: проверки долговечности поднимают **настоящий бинарник**
//! (`env!("CARGO_BIN_EXE_svindb")`) отдельным процессом, и переключатель внутри
//! тестового процесса до него не дотягивается. Управление обязано приходить
//! снаружи, через среду.
//!
//! # Почему возможность сборки, а не проверка во время работы
//!
//! Обещание отчёта — «числа сняты с того же двоичного файла, который уходит в
//! релиз». Постоянная ветка на пути приёма это обещание рушит, даже если она
//! никогда не срабатывает: она занимает место в кеше команд и мешает
//! встраиванию. Без возможности `failpoints` каждый вызов [`point`] — пустая
//! функция без единой команды, и релизная сборка побайтово равна сборке без
//! этого модуля вовсе. Проверяется это в CI, а не на слово.
//!
//! # Чего этот механизм **не** моделирует
//!
//! `_exit` — аналог `kill -9`: процесс исчезает, не исполняя деструкторов и не
//! сбрасывая буферов. Это ровно та потеря, от которой защищает журнал. Но
//! данные, которые файловая система уже подтвердила, а на пластину ещё не
//! унесла, так не теряются — для них нужен обрыв питания или `dm-flakey`, и они
//! проверяются отдельно. Не путать одно с другим: зелёная матрица здесь не
//! означает устойчивости к отказу носителя.
//!
//! # Как задавать
//!
//! Переменная среды `SVINDB_FAILPOINTS`, точки разделены `;`:
//!
//! ```text
//! SVINDB_FAILPOINTS='manifest.after_publish=kill'
//! SVINDB_FAILPOINTS='wal.after_fsync=kill,after=3;flush.before_head=err(нет места)'
//! ```
//!
//! Действия:
//!
//! | запись | что делает |
//! |---|---|
//! | `kill` | немедленный `_exit(137)`: ни деструкторов, ни сброса буферов |
//! | `err(текст)` | [`point_io`] возвращает `Other` с этим текстом |
//! | `enospc` | [`point_io`] возвращает `StorageFull` |
//! | `sleep(мс)` | задержка: расширяет окно, чтобы в него попал соседний поток |
//! | `hang` | вечный сон: проверка сторожа зависания |
//!
//! Уточнения: `after=N` пропускает первые `N` попаданий, `count=N` ограничивает
//! число срабатываний. Счётчик попаданий — общий на процесс, а не на поток:
//! окна, которые мы ловим, живут между потоками, и потоко-местный счётчик
//! пропускал бы ровно те из них, ради которых всё это написано.

/// Отмечает опасное место.
///
/// Имя — константа, а не строка: точки перечислены в матрице отказов, и
/// опечатка в имени обязана быть ошибкой сборки, а не молчаливо неработающей
/// проверкой.
#[inline(always)]
pub fn point(name: &'static str) {
    #[cfg(feature = "failpoints")]
    armed::hit(name);
    #[cfg(not(feature = "failpoints"))]
    let _ = name;
}

/// То же, но место умеет ещё и **вернуть** ошибку вместо падения.
///
/// Нужно там, где проверяется не потеря процесса, а обработка отказа носителя:
/// `ENOSPC` посреди слияния, отказ `fsync`, обрыв записи заголовка.
#[inline(always)]
pub fn point_io(name: &'static str) -> std::io::Result<()> {
    #[cfg(feature = "failpoints")]
    {
        armed::hit_io(name)
    }
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = name;
        Ok(())
    }
}

#[cfg(feature = "failpoints")]
mod armed {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    // Немедленный конец процесса без разматывания и без сброса буферов.
    //
    // `std::process::abort` не подошёл: он поднимает `SIGABRT`, и на стенде это
    // означает попытку снять дамп — секунды на каждом из сотен прогонов матрицы,
    // да ещё и файлы в несколько гигабайт. `_exit` уходит из процесса той же
    // системной службой, что и `kill -9`, и ровно с тем же следствием для
    // незаписанного.
    extern "C" {
        fn _exit(code: i32) -> !;
    }

    enum Act {
        /// Код 137 — соглашение оболочки для снятого `SIGKILL` (128 + 9).
        Kill,
        Err(String),
        NoSpace,
        Sleep(u64),
        Hang,
    }

    struct Armed {
        name: String,
        act: Act,
        after: u64,
        count: u64,
        hits: AtomicU64,
    }

    static POINTS: OnceLock<Vec<Armed>> = OnceLock::new();

    fn points() -> &'static [Armed] {
        POINTS.get_or_init(parse).as_slice()
    }

    fn parse() -> Vec<Armed> {
        let Some(spec) = std::env::var_os("SVINDB_FAILPOINTS") else {
            return Vec::new();
        };
        let Some(spec) = spec.to_str() else {
            eprintln!("SVINDB_FAILPOINTS: значение не UTF-8, точки отказа не взведены");
            return Vec::new();
        };
        let mut out = Vec::new();
        for one in spec.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            match parse_one(one) {
                Ok(a) => out.push(a),
                // Молчаливо пропустить разобранную с ошибкой точку значило бы
                // отдать зелёный прогон, в котором отказ просто не случился.
                Err(why) => {
                    eprintln!("SVINDB_FAILPOINTS: «{one}» не разобрано: {why}");
                    unsafe { _exit(2) }
                }
            }
        }
        out
    }

    fn parse_one(one: &str) -> Result<Armed, String> {
        let (name, rest) = one.split_once('=').ok_or("нет «=» между именем и действием")?;
        let name = name.trim();
        if name.is_empty() {
            return Err("пустое имя точки".into());
        }
        let mut parts = split_top(rest);
        let act = parse_act(parts.next().ok_or("нет действия")?.trim())?;
        let (mut after, mut count) = (0u64, u64::MAX);
        for p in parts {
            let (k, v) =
                p.trim().split_once('=').ok_or_else(|| format!("«{p}» не «ключ=значение»"))?;
            let n: u64 = v.trim().parse().map_err(|_| format!("«{v}» не число"))?;
            match k.trim() {
                "after" => after = n,
                "count" => count = n,
                other => return Err(format!("неизвестное уточнение «{other}»")),
            }
        }
        Ok(Armed { name: name.to_string(), act, after, count, hits: AtomicU64::new(0) })
    }

    fn parse_act(s: &str) -> Result<Act, String> {
        if s == "kill" {
            return Ok(Act::Kill);
        }
        if s == "hang" {
            return Ok(Act::Hang);
        }
        if s == "enospc" {
            return Ok(Act::NoSpace);
        }
        if let Some(text) = s.strip_prefix("err(").and_then(|t| t.strip_suffix(')')) {
            return Ok(Act::Err(text.to_string()));
        }
        if let Some(ms) = s.strip_prefix("sleep(").and_then(|t| t.strip_suffix(')')) {
            let ms = ms.trim().parse().map_err(|_| format!("«{ms}» не число миллисекунд"))?;
            return Ok(Act::Sleep(ms));
        }
        Err(format!("неизвестное действие «{s}»"))
    }

    /// Делит по запятым **верхнего уровня**: `err(нет, места)` — одно поле.
    fn split_top(s: &str) -> impl Iterator<Item = &str> {
        let mut out = Vec::new();
        let (mut start, mut depth) = (0usize, 0i32);
        for (at, c) in s.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    out.push(&s[start..at]);
                    start = at + 1;
                }
                _ => {}
            }
        }
        out.push(&s[start..]);
        out.into_iter()
    }

    /// Отдаёт действие, если эта точка взведена и её очередь настала.
    fn due(name: &str) -> Option<&'static Act> {
        let all = points();
        if all.is_empty() {
            return None;
        }
        let a = all.iter().find(|a| a.name == name)?;
        // `fetch_add` до сравнения: два потока в одной точке обязаны получить
        // разные номера попаданий, иначе `count=1` сработал бы дважды.
        let n = a.hits.fetch_add(1, Ordering::SeqCst);
        (n >= a.after && n - a.after < a.count).then_some(&a.act)
    }

    pub(super) fn hit(name: &'static str) {
        let Some(act) = due(name) else { return };
        match act {
            Act::Kill => {
                eprintln!("SVINDB_FAILPOINTS: точка «{name}» снимает процесс");
                unsafe { _exit(137) }
            }
            Act::Sleep(ms) => std::thread::sleep(std::time::Duration::from_millis(*ms)),
            Act::Hang => loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            },
            // Отказ без возврата значения ждёт своего вызывающего: место
            // объявлено через `point`, а не `point_io`. Сообщаем, а не роняем
            // молча — иначе прогон был бы зелёным по недоразумению.
            Act::Err(_) | Act::NoSpace => {
                eprintln!(
                    "SVINDB_FAILPOINTS: точка «{name}» объявлена как `point`, ей нужен `point_io`"
                );
                unsafe { _exit(2) }
            }
        }
    }

    pub(super) fn hit_io(name: &'static str) -> std::io::Result<()> {
        let Some(act) = due(name) else { return Ok(()) };
        match act {
            Act::Err(text) => Err(std::io::Error::other(text.clone())),
            Act::NoSpace => {
                Err(std::io::Error::new(std::io::ErrorKind::StorageFull, "нет места на устройстве"))
            }
            Act::Kill => {
                eprintln!("SVINDB_FAILPOINTS: точка «{name}» снимает процесс");
                unsafe { _exit(137) }
            }
            Act::Sleep(ms) => {
                std::thread::sleep(std::time::Duration::from_millis(*ms));
                Ok(())
            }
            Act::Hang => loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            },
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn a_point_without_a_name_is_refused() {
            assert!(parse_one("=kill").is_err());
            assert!(parse_one("kill").is_err());
        }

        #[test]
        fn an_unknown_action_is_refused_rather_than_ignored() {
            assert!(parse_act("kil").is_err());
            assert!(parse_act("sleep(много)").is_err());
            assert!(parse_one("a=kill,when=3").is_err());
        }

        #[test]
        fn a_comma_inside_the_message_stays_in_the_message() {
            let a = parse_one("wal.after_fsync=err(нет места, совсем),after=2,count=1").unwrap();
            assert_eq!(a.name, "wal.after_fsync");
            assert_eq!((a.after, a.count), (2, 1));
            match &a.act {
                Act::Err(t) => assert_eq!(t, "нет места, совсем"),
                _ => panic!("разобрано не как err"),
            }
        }

        /// `after` пропускает ровно столько попаданий, сколько названо, а
        /// `count` ограничивает срабатывания — вместе они вырезают из потока
        /// попаданий один конкретный отрезок.
        #[test]
        fn after_and_count_cut_out_exactly_the_named_window() {
            let a = parse_one("p=sleep(0),after=2,count=3").unwrap();
            let fired: Vec<bool> = (0..8)
                .map(|_| {
                    let n = a.hits.fetch_add(1, Ordering::SeqCst);
                    n >= a.after && n - a.after < a.count
                })
                .collect();
            assert_eq!(fired, [false, false, true, true, true, false, false, false]);
        }
    }
}
