//! Реестр полей индекса: путь к полю — устойчивый номер.

use crate::index::{valid_field_name, Kind};
use crate::json::{self, obj, Json};

/// Имя файла реестра в корне индекса.
pub const FILE: &str = "schema.json";

/// Больше этого реестр полей не читается.
///
/// Реестр — это описание схемы, а не данные: сотня полей укладывается в
/// десятки килобайт, тысяча — в сотни. Четыре мегабайта это тысячекратный
/// запас и одновременно обрыв: файл в каталоге индекса может оказаться каким
/// угодно (подменённым, дописанным, просто чужим), а `read_to_string` читал
/// его целиком, сколько бы там ни было. Разбор JSON на гигабайте — это не
/// отказ, это узел, который не поднялся и не сказал почему.
pub const MAX_BYTES: u64 = 4 << 20;

/// Наибольшее число полей, которое способен представить заголовок сегмента.
///
/// Это предел формата, а не рекомендация по размеру схемы. Обычный индекс
/// упрётся в [`MAX_BYTES`] раньше, но проверка по числу нужна отдельно: реестр
/// можно получить не только чтением файла, а `as u16` при записи заголовка
/// превратил бы 65 536 полей в ноль без единого отказа.
pub const MAX_FIELDS: usize = u16::MAX as usize;

/// Наибольшее число групп, которое способен представить заголовок сегмента.
pub const MAX_GROUPS: usize = u16::MAX as usize;

/// Версия формата самого файла. Меняется, когда меняется его раскладка.
pub const FORMAT: u32 = 1;

/// Сколько частей допускается в пути.
pub const MAX_DEPTH: usize = 16;

/// Потолок длины пути в байтах.
pub const MAX_PATH: usize = 255;

/// Объявленное поле.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldDef {
    /// Устойчивый номер. Выдан один раз и не переиспользуется.
    pub id: u32,
    /// Путь в документе: части через точку.
    pub path: String,
    pub kind: Kind,
    /// Номер группы. Ноль — корень, то есть одно значение на документ.
    pub group: u32,
    pub boost: f64,
    pub store: bool,
    pub positions: bool,
    /// Ищется ли по полю. `false` — объявлено `noindex`: содержимое лежит и
    /// отдаётся, но ни списка в словаре, ни колонки у поля нет.
    pub index: bool,
    /// Версия реестра, в которой поле появилось.
    pub since: u32,
    /// Версия, в которой похоронено. `None` — живое.
    pub dead: Option<u32>,
}

/// Основа имён файлов поля в каталоге сегмента.
///
/// ## Почему имя поля и имя файла — разные вещи
///
/// Путь поля может содержать точки (`media.duration`), а имя файла — нет:
/// у составных полей части зовутся `поле.lo` и `поле.hi`, и объявленное
/// `a.lo` слилось бы с частью поля `a`. Отсюда правило: путь с точкой
/// получает основу `f<номер>`, простой остаётся собой.
///
/// Правило живёт **здесь одно**, и его же зовёт [`FieldDef::stem`]. Дай
/// индексатору свою копию — и однажды две копии разойдутся, а разойдясь,
/// дадут сегмент, файлы которого зовутся не так, как их ищет читатель.
pub fn stem_of(path: &str, id: u32) -> String {
    if path.contains('.') {
        format!("f{id}")
    } else {
        path.to_string()
    }
}

impl FieldDef {
    /// Основа имён файлов этого поля в каталоге сегмента.
    pub fn stem(&self) -> String {
        stem_of(&self.path, self.id)
    }
}

/// Объявленная группа — путь массива. Ноль всегда корень.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupDef {
    pub id: u32,
    pub path: String,
    pub since: u32,
}

/// Реестр индекса.
#[derive(Clone, Debug)]
pub struct Registry {
    pub version: u32,
    next_field: u32,
    next_group: u32,
    fields: Vec<FieldDef>,
    groups: Vec<GroupDef>,
}

impl Default for Registry {
    fn default() -> Self {
        Registry::new()
    }
}

/// Годится ли путь поля.
///
/// Части — по правилам имени поля ([`valid_field_name`]), соединённые точкой.
/// Пустая часть запрещена, а значит `..`, ведущая и хвостовая точки невозможны;
/// `/`, `\`, таб и перевод строки не входят в алфавит части.
pub fn valid_field_path(path: &str) -> Result<(), String> {
    if path.len() > MAX_PATH {
        return Err(format!("путь длиннее {MAX_PATH} байт: {path:?}"));
    }
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() > MAX_DEPTH {
        return Err(format!("в пути {path:?} больше {MAX_DEPTH} частей"));
    }
    for p in parts {
        valid_field_name(p).map_err(|e| format!("путь {path:?}: {e}"))?;
    }
    Ok(())
}

impl Registry {
    /// Пустой реестр с одной корневой группой.
    pub fn new() -> Registry {
        Registry {
            version: 1,
            next_field: 0,
            next_group: 1,
            fields: Vec::new(),
            groups: vec![GroupDef { id: 0, path: String::new(), since: 1 }],
        }
    }

    pub fn fields(&self) -> &[FieldDef] {
        &self.fields
    }

    pub fn groups(&self) -> &[GroupDef] {
        &self.groups
    }

    /// Живые поля в порядке объявления.
    pub fn live(&self) -> impl Iterator<Item = &FieldDef> {
        self.fields.iter().filter(|f| f.dead.is_none())
    }

    /// Поле по устойчивому номеру — в том числе похороненное.
    pub fn by_id(&self, id: u32) -> Option<&FieldDef> {
        self.fields.iter().find(|f| f.id == id)
    }

    /// Номер живого поля по пути.
    pub fn id_of(&self, path: &str) -> Option<u32> {
        self.fields.iter().find(|f| f.dead.is_none() && f.path == path).map(|f| f.id)
    }

    /// Объявляет поле или сверяет уже объявленное.
    ///
    /// Отдаёт устойчивый номер. Повторное объявление с тем же типом и той же
    /// группой — не ошибка: так поднимается тот же индекс во второй раз.
    /// Расхождение по типу или группе — отказ: молча принять его значило бы
    /// собрать колонку из двух несовместимых половин при первом же слиянии.
    // Восемь доводов — это восемь независимых свойств поля, а не забытая
    // структура: собери их в одну, и вызывающий сможет объявить поле, забыв
    // задать `index` или `positions`, — сейчас компилятор ему этого не даёт.
    #[allow(clippy::too_many_arguments)]
    pub fn declare(
        &mut self,
        path: &str,
        kind: Kind,
        group: u32,
        boost: f64,
        store: bool,
        positions: bool,
        index: bool,
    ) -> Result<u32, String> {
        valid_field_path(path)?;
        // Имя вида `f7` без точки заняло бы основу файлов, которую [`stem_of`]
        // выдаёт полю с точкой и номером семь. Столкновение ловится и при
        // открытии сегмента — но **после** того, как он записан, то есть
        // писатель уже произвёл то, чего никто не откроет. Ловить надо здесь.
        if !path.contains('.')
            && path.len() > 1
            && path.starts_with('f')
            && path[1..].bytes().all(|b| b.is_ascii_digit())
        {
            return Err(format!(
                "поле {path:?}: имена вида f<число> отведены под основу файлов полей с точкой \
                 в пути — возьмите другое имя"
            ));
        }
        if !self.groups.iter().any(|g| g.id == group) {
            return Err(format!("поле {path:?}: группы {group} нет в реестре"));
        }
        if let Some(f) = self.fields.iter_mut().find(|f| f.path == path && f.dead.is_none()) {
            if f.kind != kind {
                return Err(format!(
                    "поле {path:?} объявлено как {}, а было {}: смена типа — это новое поле, \
                     а старое хоронится",
                    kind.name(),
                    f.kind.name()
                ));
            }
            if f.group != group {
                return Err(format!(
                    "поле {path:?} объявлено в группе {group}, а было в {}: смена группы — \
                     это новое поле",
                    f.group
                ));
            }
            // Вес и хранимость менять можно: они действуют только на сегменты,
            // собранные после, и лежат в заголовке каждого отдельно.
            f.boost = boost;
            f.store = store;
            f.positions = positions;
            f.index = index;
            return Ok(f.id);
        }
        if self.fields.len() >= MAX_FIELDS {
            return Err(format!(
                "полей уже {}, а формат сегмента допускает не больше {MAX_FIELDS}",
                self.fields.len()
            ));
        }
        let id = self.next_field;
        // Счётчики считаются с проверкой: переполнение здесь означало бы, что
        // новое поле получило номер уже живущего, а версия схемы уехала назад.
        // Ни то, ни другое не отказ — это молча испорченный индекс.
        self.next_field = self
            .next_field
            .checked_add(1)
            .ok_or_else(|| format!("номера полей досчитали до предела {}", u32::MAX))?;
        self.version = self
            .version
            .checked_add(1)
            .ok_or_else(|| format!("версия схемы досчитала до предела {}", u32::MAX))?;
        self.fields.push(FieldDef {
            id,
            path: path.to_string(),
            kind,
            group,
            boost,
            store,
            positions,
            index,
            since: self.version,
            dead: None,
        });
        Ok(id)
    }

    /// Объявляет группу или отдаёт номер уже объявленной.
    /// Путь группы по её номеру. `None` — такой группы в реестре нет.
    ///
    /// Нулевая отдаёт пустую строку: корень — это сам документ.
    pub fn group_path(&self, id: u32) -> Option<String> {
        if id == 0 {
            return Some(String::new());
        }
        self.groups.iter().find(|g| g.id == id).map(|g| g.path.clone())
    }

    pub fn declare_group(&mut self, path: &str) -> Result<u32, String> {
        if path.is_empty() {
            return Ok(0);
        }
        valid_field_path(path)?;
        if let Some(g) = self.groups.iter().find(|g| g.path == path) {
            return Ok(g.id);
        }
        if self.groups.len() >= MAX_GROUPS {
            return Err(format!(
                "групп уже {}, а формат сегмента допускает не больше {MAX_GROUPS}",
                self.groups.len()
            ));
        }
        let id = self.next_group;
        self.next_group = self
            .next_group
            .checked_add(1)
            .ok_or_else(|| format!("номера групп досчитали до предела {}", u32::MAX))?;
        self.version = self
            .version
            .checked_add(1)
            .ok_or_else(|| format!("версия схемы досчитала до предела {}", u32::MAX))?;
        self.groups.push(GroupDef { id, path: path.to_string(), since: self.version });
        Ok(id)
    }

    /// Хоронит поле. Номер после этого не выдаётся никому и никогда.
    pub fn bury(&mut self, path: &str) -> Result<u32, String> {
        // Поле ищется **до** правки версии, и это не порядок ради порядка.
        //
        // Прежде версия росла первой строкой, а поле могло не найтись — и
        // отказ оставлял за собой сдвинутую версию. Снаружи это выглядит как
        // изменение схемы, которого не было: читатели, сравнивающие версии,
        // перечитывают схему на ровном месте, а опечатка в имени поля,
        // повторённая тысячу раз, тысячу раз сдвигает версию. Правка обязана
        // случаться только вместе с тем изменением, ради которого её делают.
        let at = self
            .fields
            .iter()
            .position(|f| f.path == path && f.dead.is_none())
            .ok_or_else(|| format!("поле {path:?} не объявлено или уже похоронено"))?;
        let v = self
            .version
            .checked_add(1)
            .ok_or_else(|| format!("версия схемы досчитала до предела {}", u32::MAX))?;
        self.version = v;
        self.fields[at].dead = Some(v);
        Ok(self.fields[at].id)
    }

    // -- запись и чтение -----------------------------------------------------

    pub fn to_json(&self) -> Json {
        let groups = self
            .groups
            .iter()
            .map(|g| {
                obj(vec![
                    ("id", json::i(g.id as i64)),
                    ("path", json::s(&g.path)),
                    ("since", json::i(g.since as i64)),
                ])
            })
            .collect();
        let fields = self
            .fields
            .iter()
            .map(|f| {
                let mut kv = vec![
                    ("id", json::i(f.id as i64)),
                    ("path", json::s(&f.path)),
                    // `name()` годится для сообщений, но намеренно теряет
                    // параметры вида. В постоянной схеме это недопустимо:
                    // `dense_vector:384` без размерности уже нельзя открыть,
                    // а `date:iso` превращается в обычный `date` и меняет
                    // единицу времени. Здесь нужен полный синтаксис типа.
                    ("kind", json::s(&f.kind.spec())),
                    ("group", json::i(f.group as i64)),
                    ("boost", Json::Num(f.boost)),
                    ("store", Json::Bool(f.store)),
                    ("positions", Json::Bool(f.positions)),
                    ("index", Json::Bool(f.index)),
                    ("since", json::i(f.since as i64)),
                ];
                if let Some(v) = f.dead {
                    kv.push(("dead", json::i(v as i64)));
                }
                obj(kv)
            })
            .collect();
        obj(vec![
            ("format", json::i(FORMAT as i64)),
            ("version", json::i(self.version as i64)),
            ("next_field", json::i(self.next_field as i64)),
            ("next_group", json::i(self.next_group as i64)),
            ("groups", Json::Arr(groups)),
            ("fields", Json::Arr(fields)),
        ])
    }

    pub fn from_json(v: &Json) -> Result<Registry, String> {
        let need = |k: &str| -> Result<&Json, String> {
            v.get(k).ok_or_else(|| format!("{FILE}: нет ключа {k:?}"))
        };
        let format = need("format")?.as_i64().unwrap_or(0);
        if format != FORMAT as i64 {
            return Err(format!(
                "{FILE}: раскладка {format}, а читается {FORMAT} — реестр от другой версии"
            ));
        }
        let num = |o: &Json, k: &str| -> Result<u32, String> {
            o.get(k)
                .and_then(|x| x.as_i64())
                .filter(|x| (0..=u32::MAX as i64).contains(x))
                .map(|x| x as u32)
                .ok_or_else(|| format!("{FILE}: у {k:?} нет годного числа"))
        };
        let group_values = need("groups")?
            .as_arr()
            .ok_or_else(|| format!("{FILE}: ключ \"groups\" обязан быть массивом"))?;
        if group_values.len() > MAX_GROUPS {
            return Err(format!(
                "{FILE}: групп {} при пределе формата {MAX_GROUPS}",
                group_values.len()
            ));
        }
        let mut groups = Vec::with_capacity(group_values.len());
        for g in group_values {
            groups.push(GroupDef {
                id: num(g, "id")?,
                path: g.get("path").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                since: num(g, "since").unwrap_or(1),
            });
        }
        if groups.len() != groups.iter().map(|g| g.id as usize).max().map_or(0, |v| v + 1)
            || groups.iter().enumerate().any(|(id, g)| g.id as usize != id)
        {
            return Err(format!(
                "{FILE}: группы обязаны иметь плотные номера от нуля в порядке объявления"
            ));
        }
        if groups.first().is_none_or(|g| !g.path.is_empty()) {
            return Err(format!("{FILE}: нет корневой группы с номером 0 и пустым путём"));
        }
        let mut group_paths = std::collections::HashSet::with_capacity(groups.len());
        for group in groups.iter() {
            if group.id > 0 {
                valid_field_path(&group.path)
                    .map_err(|e| format!("{FILE}: путь группы {}: {e}", group.id))?;
            }
            if !group_paths.insert(group.path.as_str()) {
                return Err(format!("{FILE}: путь группы {:?} объявлен дважды", group.path));
            }
        }
        let field_values = need("fields")?
            .as_arr()
            .ok_or_else(|| format!("{FILE}: ключ \"fields\" обязан быть массивом"))?;
        if field_values.len() > MAX_FIELDS {
            return Err(format!(
                "{FILE}: полей {} при пределе формата {MAX_FIELDS}",
                field_values.len()
            ));
        }
        let mut fields = Vec::with_capacity(field_values.len());
        for f in field_values {
            let path = f
                .get("path")
                .and_then(|x| x.as_str())
                .ok_or_else(|| format!("{FILE}: у поля нет пути"))?
                .to_string();
            valid_field_path(&path).map_err(|e| format!("{FILE}: поле: {e}"))?;
            let kname = f.get("kind").and_then(|x| x.as_str()).unwrap_or("");
            let kind = Kind::parse(kname)
                .ok_or_else(|| format!("{FILE}: поле {path:?} объявлено типом {kname:?}"))?;
            let dead = match f.get("dead") {
                Some(_) => Some(num(f, "dead")?),
                None => None,
            };
            fields.push(FieldDef {
                id: num(f, "id")?,
                path,
                kind,
                group: num(f, "group").unwrap_or(0),
                boost: f.get("boost").and_then(|x| x.as_f64()).unwrap_or(1.0),
                store: f.get("store").and_then(|x| x.as_bool()).unwrap_or(true),
                positions: f.get("positions").and_then(|x| x.as_bool()).unwrap_or(true),
                // Умолчание — «ищется»: реестры, записанные до появления
                // признака, описывают поля, по которым искали.
                index: f.get("index").and_then(|x| x.as_bool()).unwrap_or(true),
                since: num(f, "since").unwrap_or(1),
                dead,
            });
        }
        // Повтор номера — то, ради предотвращения чего реестр и заведён.
        for (i, f) in fields.iter().enumerate() {
            if fields[..i].iter().any(|p| p.id == f.id) {
                return Err(format!("{FILE}: номер {} выдан дважды", f.id));
            }
        }
        let next_field = num(v, "next_field")?;
        if fields.len() != next_field as usize
            || fields.iter().enumerate().any(|(id, f)| f.id as usize != id)
        {
            return Err(format!(
                "{FILE}: номера полей обязаны плотно покрывать 0..{next_field} и не \
                 выдаваться второй раз"
            ));
        }
        let next_group = num(v, "next_group").unwrap_or(1);
        if groups.len() != next_group as usize {
            return Err(format!(
                "{FILE}: групп {}, а следующий свободный номер {next_group}",
                groups.len()
            ));
        }
        let version = num(v, "version").unwrap_or(1);
        let known_groups: std::collections::HashSet<u32> = groups.iter().map(|g| g.id).collect();
        let mut live_paths = std::collections::HashSet::new();
        for field in fields.iter() {
            if !known_groups.contains(&field.group) {
                return Err(format!(
                    "{FILE}: поле {:?} ссылается на несуществующую группу {}",
                    field.path, field.group
                ));
            }
            if field.since == 0 || field.since > version {
                return Err(format!(
                    "{FILE}: поле {:?} появилось в версии {}, а версия реестра {version}",
                    field.path, field.since
                ));
            }
            if let Some(dead) = field.dead {
                if dead < field.since || dead > version {
                    return Err(format!(
                        "{FILE}: поле {:?} похоронено в версии {dead} при диапазоне {}..={version}",
                        field.path, field.since
                    ));
                }
            } else if !live_paths.insert(field.path.as_str()) {
                return Err(format!("{FILE}: живое поле {:?} объявлено дважды", field.path));
            }
        }
        Ok(Registry { version, next_field, next_group, fields, groups })
    }

    /// Читает реестр из корня индекса. `None` — файла нет.
    pub fn load(dir: &str) -> std::io::Result<Option<Registry>> {
        let path = format!("{dir}/{FILE}");
        let bytes =
            match crate::fs::read_regular_limited(std::path::Path::new(&path), MAX_BYTES, false) {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e),
            };
        let text = String::from_utf8(bytes)
            .map_err(|e| std::io::Error::other(format!("{path}: не UTF-8: {e}")))?;
        let mut v =
            json::parse(&text).map_err(|e| std::io::Error::other(format!("{path}: {e}")))?;
        recover_legacy_kinds(dir, &mut v).map_err(std::io::Error::other)?;
        Registry::from_json(&v).map(Some).map_err(|e| std::io::Error::other(format!("{path}: {e}")))
    }

    /// Пишет реестр в корень индекса.
    pub fn save(&self, dir: &str) -> std::io::Result<()> {
        let mut body = self.to_json().to_pretty().into_bytes();
        body.push(b'\n');
        if body.len() as u64 > MAX_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "реестр весит {} Б при пределе {MAX_BYTES} Б — записанный, он не прочтётся",
                    body.len()
                ),
            ));
        }
        crate::fs::atomic_write(std::path::Path::new(dir).join(FILE).as_path(), &body, 0o600)
    }
}

/// Восстанавливает параметры типов, которые реестр ранних сборок потерял.
///
/// Старый писатель клал в `schema.json` короткое [`Kind::name`]. Для обычных
/// типов этого достаточно, но `dense_vector:N` становился просто
/// `dense_vector`, а `date:iso` — `date`. Заголовок каждого сегмента всё это
/// время хранил полный [`Kind::spec`], поэтому готовый индекс можно исправить
/// без догадок. Если сегмента ещё нет, размерность действительно утрачена и
/// чтение по-прежнему честно откажет: выдумывать её нельзя.
fn recover_legacy_kinds(dir: &str, root: &mut Json) -> Result<(), String> {
    let Some(fields) = root.get("fields").and_then(Json::as_arr) else { return Ok(()) };
    let legacy: Vec<(u32, String)> = fields
        .iter()
        .filter_map(|f| {
            let kind = f.get("kind")?.as_str()?;
            if kind != "dense_vector" && kind != "date" {
                return None;
            }
            let id = f.get("id")?.as_i64()?;
            (0..=u32::MAX as i64).contains(&id).then(|| (id as u32, kind.to_string()))
        })
        .collect();
    if legacy.is_empty() {
        return Ok(());
    }

    let mut found: std::collections::HashMap<u32, Kind> = std::collections::HashMap::new();
    // Список читается с проверкой: испорченный означал бы, что виды полей
    // восстанавливаются по неполному составу, а недостающий вид тихо остаётся
    // прежним — то есть неверным.
    let listed = crate::manifest::segments_checked(dir).map_err(|e| format!("{dir}: {e}"))?;
    for segment in listed {
        let Ok(head) = crate::index::read_head(&segment) else { continue };
        for (id, old) in legacy.iter() {
            let Some(field) = head.fields.iter().find(|f| f.fid == *id) else { continue };
            let candidate = match (old.as_str(), field.kind) {
                ("dense_vector", Kind::Dense(_)) => Some(field.kind),
                ("date", Kind::Date | Kind::DateIso) => Some(field.kind),
                _ => None,
            };
            let Some(candidate) = candidate else { continue };
            if let Some(previous) = found.insert(*id, candidate) {
                if previous != candidate {
                    return Err(format!(
                        "{FILE}: сегменты расходятся в типе поля с номером {id}: {} и {}",
                        previous.spec(),
                        candidate.spec()
                    ));
                }
            }
        }
    }

    let Json::Obj(root_pairs) = root else { return Ok(()) };
    let Some(Json::Arr(fields)) =
        root_pairs.iter_mut().find(|(key, _)| key == "fields").map(|(_, value)| value)
    else {
        return Ok(());
    };
    for field in fields.iter_mut() {
        let Json::Obj(pairs) = field else { continue };
        let id = pairs
            .iter()
            .find(|(key, _)| key == "id")
            .and_then(|(_, value)| value.as_i64())
            .and_then(|id| u32::try_from(id).ok());
        let Some(kind) = id.and_then(|id| found.get(&id)).copied() else { continue };
        if let Some((_, value)) = pairs.iter_mut().find(|(key, _)| key == "kind") {
            *value = Json::Str(kind.spec());
        }
    }
    Ok(())
}

/// Одно объявленное поле — то, что оператор написал о нём снаружи.
///
/// Отдельным типом, а не семью параллельными срезами: срезы разной длины
/// молча дают полю чужой тип или чужую группу, и заметить это можно только по
/// выдаче.
#[derive(Clone, Debug, PartialEq)]
pub struct Decl {
    /// Путь поля в документе, части через точку.
    pub path: String,
    pub kind: Kind,
    /// Путь массива, внутри которого лежит поле. Пусто — поле корневое.
    pub group: String,
    pub boost: f64,
    pub store: bool,
    pub positions: bool,
    pub index: bool,
}

impl Decl {
    /// Корневое поле с обычными признаками.
    pub fn root(path: &str, kind: Kind) -> Decl {
        Decl {
            path: path.to_string(),
            kind,
            group: String::new(),
            boost: 1.0,
            store: true,
            positions: true,
            index: true,
        }
    }
}

/// Что вышло из объявления: номера полей, их группы и пути групп.
#[derive(Debug)]
pub struct Declared {
    pub fids: Vec<u32>,
    /// Номер группы каждого поля, по порядку объявления.
    pub groups: Vec<u32>,
    /// Путь группы по её номеру. Нулевая — пустая строка.
    pub group_paths: Vec<String>,
}

/// Объявляет набор полей с группами и отдаёт их номера.
///
/// ## Почему группа берётся из реестра, а не из аргумента
///
/// Смена группы у объявленного поля запрещена навсегда: колонка группового
/// поля адресуется ординалом элемента, а корневого — номером документа, и
/// «то же поле в другой группе» — это другое поле. Пока единственный путь
/// объявления зашивал группу нолём, всякий индекс имел все поля прибитыми к
/// корню, а индекс с непустой группой в реестре **переставал подниматься**:
/// повторное объявление на старте требовало нуля и получало отказ.
///
/// Здесь группа приходит из того же объявления, что и в прошлый раз, поэтому
/// повторный старт тождествен первому.
pub fn declare_all(dir: &str, decls: &[Decl]) -> std::io::Result<Declared> {
    std::fs::create_dir_all(dir)?;
    let mut reg = Registry::load(dir)?.unwrap_or_default();
    // Сперва группы: поле не объявить в группу, которой ещё нет в реестре.
    let mut groups = Vec::with_capacity(decls.len());
    for d in decls.iter() {
        // Путь группы обязан быть началом пути поля, иначе объявление лжёт:
        // `reactions.count` в группе `comments` означал бы, что счётчик лежит
        // в массиве, которого в его собственном пути нет.
        if !d.group.is_empty() {
            let inside = d.path == d.group
                || d.path.strip_prefix(&d.group).is_some_and(|r| r.starts_with('.'));
            if !inside {
                return Err(std::io::Error::other(format!(
                    "поле {:?} объявлено в массиве {:?}, а его путь в этот массив не ведёт",
                    d.path, d.group
                )));
            }
        }
        let g = reg.declare_group(&d.group).map_err(std::io::Error::other)?;
        groups.push(g);
    }
    // Спутники точки заводятся сами — объявлять их руками нельзя.
    //
    // У поля `geo_point` есть скрытые спутники `<поле>.lat` и `<поле>.lon`:
    // приём заполняет их из самой точки, и колонки под них пишутся вместе с
    // ней. Объявленный рядом `место.lat` получает собственное место в схеме и
    // собственную колонку с тем же именем файла — а контейнер отвергает кусок,
    // записанный дважды.
    //
    // Отвергать надо здесь, при объявлении схемы, а не там. Проверено: узел
    // принимал такую схему, отвечал на пачку `{"added":1}` с кодом 200, падал
    // на первом же сбросе с «кусок f2.col записан дважды» и останавливал приём.
    // То есть данные принимались и терялись, а клиенту говорилось об успехе —
    // худший из возможных отказов.
    let points: Vec<&str> = decls
        .iter()
        .filter(|d| d.kind == crate::index::Kind::Geo)
        .map(|d| d.path.as_str())
        .collect();
    for d in decls.iter() {
        for suffix in [".lat", ".lon"] {
            if let Some(base) = d.path.strip_suffix(suffix) {
                if points.contains(&base) {
                    // Сетевой и offline-разбор схемы добавляют спутники сами.
                    // Их отличает не имя (оно как раз то же), а закрытый для
                    // пользовательского синтаксиса набор свойств: числовая
                    // колонка без хранения и позиций, но с индексом. Ручное
                    // `place.lat:double` приходит с `positions=true` и по-
                    // прежнему отвергается. Прежде проверка отвергала и свой
                    // собственный спутник, поэтому любая схема с geo_point не
                    // могла запустить узел вообще.
                    let generated = d.kind == crate::index::Kind::Double
                        && !d.store
                        && !d.positions
                        && d.index
                        && decls
                            .iter()
                            .find(|point| {
                                point.path == base && point.kind == crate::index::Kind::Geo
                            })
                            .is_some_and(|point| point.group == d.group);
                    if generated {
                        continue;
                    }
                    return Err(std::io::Error::other(format!(
                        "поле {:?} объявлять не нужно и нельзя: это спутник точки \
                         {base:?}, он заводится сам и заполняется из неё. Объявленный \
                         рядом, он даёт вторую колонку с тем же именем файла, и запись \
                         сегмента отказывает уже после того, как документы приняты. \
                         Уберите его из схемы — отбор по {:?} работает и без объявления",
                        d.path, d.path
                    )));
                }
            }
        }
    }

    let mut fids = Vec::with_capacity(decls.len());
    for (d, g) in decls.iter().zip(groups.iter()) {
        let id = reg
            .declare(&d.path, d.kind, *g, d.boost, d.store, d.positions, d.index)
            .map_err(std::io::Error::other)?;
        fids.push(id);
    }
    let mut group_paths =
        vec![String::new(); reg.groups.iter().map(|g| g.id as usize + 1).max().unwrap_or(1)];
    for g in reg.groups.iter() {
        if let Some(slot) = group_paths.get_mut(g.id as usize) {
            *slot = g.path.clone();
        }
    }
    reg.save(dir)?;
    Ok(Declared { fids, groups, group_paths })
}

/// Объявляет плоский набор полей и отдаёт их устойчивые номера.
///
/// Реестр читается из корня индекса, дополняется и записывается обратно. Если
/// реестра нет — заводится новый; каталога нет — создаётся.
///
/// Это точка, где приём перестаёт назначать номера позицией. Пока схема не
/// менялась, разницы нет: первое объявление выдаёт `0, 1, 2, …`, то есть ровно
/// позиции. Разница появляется после первой правки схемы — и появляется
/// **тихо и правильно**, а не отказом при открытии.
pub fn declare_flat(
    dir: &str,
    names: &[String],
    kinds: &[Kind],
    boosts: &[f64],
    stored: &[bool],
    positioned: &[bool],
    searched: &[bool],
) -> std::io::Result<Vec<u32>> {
    let decls: Vec<Decl> = names
        .iter()
        .enumerate()
        .map(|(i, name)| Decl {
            path: name.clone(),
            kind: kinds.get(i).copied().unwrap_or(Kind::Text),
            group: String::new(),
            boost: boosts.get(i).copied().unwrap_or(1.0),
            store: stored.get(i).copied().unwrap_or(true),
            positions: positioned.get(i).copied().unwrap_or(true),
            index: searched.get(i).copied().unwrap_or(true),
        })
        .collect();
    Ok(declare_all(dir, &decls)?.fids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-schema-{name}"));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().to_string()
    }

    /// Реестр переживает круг «запись — чтение» без единой потери.
    #[test]
    fn a_registry_survives_the_round_trip() {
        let dir = tmp("round");
        let mut r = Registry::new();
        let g = r.declare_group("reactions").unwrap();
        r.declare("title", Kind::Text, 0, 2.0, true, true, true).unwrap();
        r.declare("price", Kind::Double, 0, 1.0, true, false, true).unwrap();
        r.declare("reactions.emoji", Kind::Keyword, g, 1.0, false, true, true).unwrap();
        r.bury("price").unwrap();
        r.save(&dir).unwrap();

        let back = Registry::load(&dir).unwrap().expect("реестр на месте");
        assert_eq!(back.fields(), r.fields());
        assert_eq!(back.groups(), r.groups());
        assert_eq!(back.version, r.version);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Параметризованные типы обязаны переживать реестр целиком.
    ///
    /// Короткое имя теряло размерность плотного вектора и признак ISO-даты:
    /// первый индекс после перезапуска не открывался, второй молча менял
    /// миллисекунды на пользовательскую единицу обычного `date`.
    #[test]
    fn parameterized_kinds_survive_the_registry_round_trip() {
        let dir = tmp("parameterized-round");
        let mut r = Registry::new();
        r.declare("embedding", Kind::Dense(4), 0, 1.0, false, true, true).unwrap();
        r.declare("created", Kind::DateIso, 0, 1.0, true, true, true).unwrap();
        r.save(&dir).unwrap();

        let text = std::fs::read_to_string(format!("{dir}/{FILE}")).unwrap();
        assert!(text.contains("dense_vector:4"), "размерность не записана: {text}");
        assert!(text.contains("date:iso"), "единица даты не записана: {text}");
        let back = Registry::load(&dir).unwrap().unwrap();
        assert_eq!(back.fields()[0].kind, Kind::Dense(4));
        assert_eq!(back.fields()[1].kind, Kind::DateIso);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Реестр ранней сборки чинится по полному типу из заголовка сегмента.
    #[test]
    fn legacy_parameterized_kinds_are_recovered_from_a_segment_head() {
        let dir = tmp("legacy-parameterized");
        let segment = format!("{dir}/seg0000");
        std::fs::create_dir(&segment).unwrap();
        std::fs::write(format!("{dir}/segments.tsv"), "seg0000\n").unwrap();
        std::fs::write(
            format!("{segment}/head.tsv"),
            concat!(
                "docs\t1\nblocks\t1\nterms\t1\n",
                "field\tembedding\t1\t1\tdense_vector:4\t1\t0\t1\t0\t0\tembedding\tnone\tbits\t0\t0\t1\t\t\n",
                "field\tcreated\t1\t1\tdate:iso\t1\t1\t1\t1\t1\tcreated\tdense\tall\t0\t1\t1\t0\t0\n",
            ),
        )
        .unwrap();
        std::fs::write(
            format!("{dir}/{FILE}"),
            r#"{"format":1,"version":3,"next_field":2,"next_group":1,
              "groups":[{"id":0,"path":"","since":1}],
              "fields":[{"id":0,"path":"embedding","kind":"dense_vector","group":0,"since":2},
                        {"id":1,"path":"created","kind":"date","group":0,"since":3}]}"#,
        )
        .unwrap();

        let back = Registry::load(&dir).unwrap().unwrap();
        assert_eq!(back.fields()[0].kind, Kind::Dense(4));
        assert_eq!(back.fields()[1].kind, Kind::DateIso);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Номер не переиспользуется: похороненное поле держит свой навсегда.
    #[test]
    fn a_buried_number_is_never_handed_out_again() {
        let mut r = Registry::new();
        let a = r.declare("price", Kind::Int64, 0, 1.0, true, false, true).unwrap();
        r.bury("price").unwrap();
        // То же имя, тот же тип — но это уже другое поле.
        let b = r.declare("price", Kind::Int64, 0, 1.0, true, false, true).unwrap();
        assert_ne!(a, b, "номер выдан второй раз");
        // Версия растёт на каждое изменение: объявление дало 2, похороны — 3.
        assert_eq!(r.by_id(a).unwrap().dead, Some(3), "старое осталось похороненным");
        assert_eq!(r.id_of("price"), Some(b), "по имени находится живое");
        let c = r.declare("cost", Kind::Int64, 0, 1.0, true, false, true).unwrap();
        assert!(c != a && c != b);
    }

    /// Смена типа у живого поля — отказ, а не тихая перезапись.
    #[test]
    fn changing_the_type_of_a_live_field_is_refused() {
        let mut r = Registry::new();
        r.declare("price", Kind::Double, 0, 1.0, true, false, true).unwrap();
        let e = r.declare("price", Kind::Keyword, 0, 1.0, true, false, true).unwrap_err();
        assert!(e.contains("смена типа"), "невнятный отказ: {e}");
        // Повтор с тем же типом — законен: так поднимается тот же индекс.
        assert_eq!(r.declare("price", Kind::Double, 0, 2.0, true, false, true), Ok(0));
        assert_eq!(r.by_id(0).unwrap().boost, 2.0, "вес менять можно");
    }

    /// Путь с точкой не становится именем файла.
    #[test]
    fn a_dotted_path_never_becomes_a_file_name() {
        let mut r = Registry::new();
        let g = r.declare_group("media").unwrap();
        let plain = r.declare("price", Kind::Int64, 0, 1.0, true, false, true).unwrap();
        let dotted = r.declare("media.duration", Kind::Double, g, 1.0, true, false, true).unwrap();
        assert_eq!(r.by_id(plain).unwrap().stem(), "price", "простое имя остаётся читаемым");
        assert_eq!(r.by_id(dotted).unwrap().stem(), format!("f{dotted}"));
        assert!(!r.by_id(dotted).unwrap().stem().contains('.'));
    }

    /// Негодный путь отвергается на объявлении, а не на записи файла.
    #[test]
    fn a_bad_path_is_refused() {
        let mut r = Registry::new();
        for bad in ["", "a..b", ".a", "a.", "a/b", "a\tb", "a\nb", "../escape"] {
            assert!(
                r.declare(bad, Kind::Text, 0, 1.0, true, true, true).is_err(),
                "путь {bad:?} прошёл, а не должен"
            );
        }
        assert!(r
            .declare(&"a".repeat(MAX_PATH + 1), Kind::Text, 0, 1.0, true, true, true)
            .is_err());
        let deep = vec!["a"; MAX_DEPTH + 1].join(".");
        assert!(
            r.declare(&deep, Kind::Text, 0, 1.0, true, true, true).is_err(),
            "глубина без предела"
        );
    }

    /// Реестр с повторённым номером не читается.
    #[test]
    fn a_duplicate_number_is_refused_on_read() {
        let text = r#"{"format":1,"version":3,"next_field":2,"next_group":1,
          "groups":[{"id":0,"path":"","since":1}],
          "fields":[{"id":0,"path":"a","kind":"text","group":0,"since":1},
                    {"id":0,"path":"b","kind":"text","group":0,"since":2}]}"#;
        let e = Registry::from_json(&json::parse(text).unwrap()).unwrap_err();
        assert!(e.contains("дважды"), "невнятный отказ: {e}");
    }

    /// Номер не меньше следующего свободного — тоже отказ: он был бы выдан снова.
    #[test]
    fn a_number_beyond_the_counter_is_refused() {
        let text = r#"{"format":1,"version":2,"next_field":1,"next_group":1,
          "groups":[{"id":0,"path":"","since":1}],
          "fields":[{"id":7,"path":"a","kind":"text","group":0,"since":1}]}"#;
        let e = Registry::from_json(&json::parse(text).unwrap()).unwrap_err();
        assert!(e.contains("второй раз"), "невнятный отказ: {e}");
    }

    /// Повторное объявление номеров не двигает, а вставка в середину не двигает
    /// соседей.
    ///
    /// Это и есть то, ради чего реестр заведён: приём объявляет поля так, как
    /// они написаны в схеме, а номер поле получает **один раз и навсегда**.
    /// Двинься номер — и сегмент, собранный вчера, начал бы отвечать на него
    /// другим полем.
    #[test]
    fn declaring_the_same_fields_twice_keeps_their_numbers() {
        let dir = tmp("flat");
        let names: Vec<String> = ["title", "price", "ts"].iter().map(|s| s.to_string()).collect();
        let kinds = vec![Kind::Text, Kind::Int32, Kind::Date];
        let b = vec![1.0; 3];
        let st = vec![true; 3];
        let ps = vec![true; 3];

        let first = declare_flat(&dir, &names, &kinds, &b, &st, &ps, &[true; 8]).unwrap();
        assert_eq!(first, vec![0, 1, 2], "первое объявление даёт позиции");
        let again = declare_flat(&dir, &names, &kinds, &b, &st, &ps, &[true; 8]).unwrap();
        assert_eq!(again, first, "повтор выдал другие номера");

        // Поле вставлено вторым — а номер получает следующий свободный.
        let names2: Vec<String> =
            ["title", "extra", "price", "ts"].iter().map(|s| s.to_string()).collect();
        let kinds2 = vec![Kind::Text, Kind::Int32, Kind::Int32, Kind::Date];
        let got =
            declare_flat(&dir, &names2, &kinds2, &[1.0; 4], &[true; 4], &[true; 4], &[true; 8])
                .unwrap();
        assert_eq!(got, vec![0, 3, 1, 2], "вставка сдвинула номера соседей");

        // И реестр на диске это помнит.
        let reg = Registry::load(&dir).unwrap().unwrap();
        assert_eq!(reg.id_of("price"), Some(1));
        assert_eq!(reg.id_of("extra"), Some(3));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Смена типа поля при повторном объявлении — отказ, а не тихая порча.
    #[test]
    fn redeclaring_a_field_with_another_type_is_refused() {
        let dir = tmp("flat-type");
        let names = vec!["price".to_string()];
        declare_flat(&dir, &names, &[Kind::Int32], &[1.0], &[true], &[true], &[true; 8]).unwrap();
        let e = declare_flat(&dir, &names, &[Kind::Keyword], &[1.0], &[true], &[true], &[true; 8])
            .unwrap_err();
        assert!(format!("{e}").contains("смена типа"), "невнятный отказ: {e}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_missing_registry_is_not_an_error() {
        let dir = tmp("missing");
        assert!(Registry::load(&dir).unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Имя вида `f<число>` отведено под основу файлов полей с точкой.
    ///
    /// Проверка двусторонняя, и это важно: односторонняя прошла бы и у запрета,
    /// который отвергает всё подряд, начинающееся с `f`.
    #[test]
    fn a_name_shaped_like_a_file_stem_is_refused_but_a_dotted_one_is_not() {
        let dir = std::env::temp_dir().join("svindb-schema-stem-clash");
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let mut r = Registry::new();

        let e = r.declare("f3", Kind::Text, 0, 1.0, true, true, true).expect_err("имя f3 принято");
        assert!(e.contains("основу файлов"), "не тот отказ: {e}");

        // А вот путь с точкой, оканчивающийся так же, законен: основой он не
        // становится — её выдаёт `stem_of` по номеру.
        r.declare("a.f3", Kind::Text, 0, 1.0, true, true, true)
            .expect("путь с точкой отвергнут напрасно");
        // И простое имя, лишь начинающееся с `f`, тоже законно.
        r.declare("field", Kind::Text, 0, 1.0, true, true, true)
            .expect("обычное имя отвергнуто напрасно");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Повторное объявление тождественно первому — иначе индекс с массивом не
    /// поднялся бы во второй раз.
    ///
    /// Смена группы у объявленного поля запрещена навсегда, и это правильно.
    /// Но пока единственный путь объявления зашивал группу нолём, запрет бил не
    /// по ошибке, а по замыслу: реестр с непустой группой требовал на старте
    /// нуля и получал отказ. Второе утверждение теста — про старый плоский путь
    /// — и делает первое непустым.
    #[test]
    fn declaring_the_same_groups_again_is_a_no_op_but_flattening_them_is_refused() {
        let dir = tmp("declare-all-again");
        let decls = vec![
            Decl::root("text", Kind::Text),
            Decl { group: "reactions".into(), ..Decl::root("reactions.emoji", Kind::Keyword) },
            Decl { group: "reactions".into(), ..Decl::root("reactions.count", Kind::Int32) },
        ];

        let first = declare_all(&dir, &decls).expect("первое объявление");
        assert_eq!(first.groups, vec![0, 1, 1], "поля разошлись по группам не так");
        assert_eq!(first.group_paths, vec!["".to_string(), "reactions".to_string()]);

        let again = declare_all(&dir, &decls).expect("повторное объявление отвергнуто");
        assert_eq!(again.fids, first.fids, "номера полей поехали");
        assert_eq!(again.groups, first.groups, "группы поехали");

        // А вот плоское объявление тех же путей — отказ: оно требует нуля.
        let e = declare_flat(
            &dir,
            &["text".into(), "reactions.emoji".into(), "reactions.count".into()],
            &[Kind::Text, Kind::Keyword, Kind::Int32],
            &[1.0; 3],
            &[true; 3],
            &[true; 3],
            &[true; 3],
        )
        .expect_err("плоское объявление поля из массива сошло с рук");
        assert!(e.to_string().contains("смена группы"), "не тот отказ: {e}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Путь группы обязан вести к полю, а не быть посторонним.
    #[test]
    fn a_group_that_is_not_a_prefix_of_the_field_path_is_refused() {
        let dir = tmp("declare-all-alien-group");
        let e = declare_all(
            &dir,
            &[Decl { group: "comments".into(), ..Decl::root("reactions.count", Kind::Int32) }],
        )
        .expect_err("чужой массив принят");
        assert!(e.to_string().contains("не ведёт"), "не тот отказ: {e}");

        // А своя группа и совпадение пути с путём группы — законны: так
        // объявляется массив скаляров.
        declare_all(
            &dir,
            &[
                Decl { group: "reactions".into(), ..Decl::root("reactions.count", Kind::Int32) },
                Decl { group: "tags".into(), ..Decl::root("tags", Kind::Keyword) },
            ],
        )
        .expect("своя группа отвергнута напрасно");
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod limit_tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-schema-lim-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Отказ похоронить несуществующее поле не двигает версию схемы.
    ///
    /// Прежде версия росла первой строкой, до поиска поля, и неудачная попытка
    /// оставляла за собой сдвинутую версию. Снаружи это изменение схемы,
    /// которого не было: читатели, сравнивающие версии, перечитывают её на
    /// ровном месте, а опечатка в имени, повторённая тысячу раз, тысячу раз
    /// сдвигает версию.
    #[test]
    fn a_refused_burial_does_not_move_the_version() {
        let mut r = Registry::default();
        r.declare("тело", Kind::Text, 0, 1.0, false, true, true).unwrap();
        let was = r.version;
        assert!(r.bury("нет-такого").is_err(), "похоронили несуществующее");
        assert_eq!(r.version, was, "отказ сдвинул версию схемы");
        // А удачные — двигают, иначе проверка выше проходила бы и на мёртвом коде.
        r.bury("тело").expect("живое поле обязано хорониться");
        assert_eq!(r.version, was + 1, "удачная правка версию не двинула");
    }

    /// Реестр читается с пределом: файл в каталоге индекса может быть любым.
    #[test]
    fn an_oversized_registry_is_refused_not_parsed() {
        let d = tmp("huge");
        // Заведомо больше предела и при этом синтаксически похоже на JSON:
        // без предела это ушло бы в разбор целиком.
        let mut body = String::from("{\"поля\":[");
        while body.len() as u64 <= MAX_BYTES {
            body.push_str("{\"путь\":\"а\"},");
        }
        body.push_str("]}");
        std::fs::write(format!("{d}/{FILE}"), &body).unwrap();
        let e = Registry::load(&d).expect_err("реестр сверх предела разобрался");
        let text = format!("{e}");
        assert!(
            text.contains("велик") || text.contains("предел") || text.contains("байт"),
            "отказ не про размер: {text}"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Писатель соблюдает тот же предел, что читатель.
    ///
    /// Односторонний предел хуже отсутствующего: узел успешно сохраняет схему,
    /// а после первого restart сам отказывается читать собственный файл. В
    /// каталоге заранее лежит маркер, чтобы проверить и вторую половину
    /// атомарности — отказ не заменяет прежний годный реестр.
    #[test]
    fn a_registry_too_large_to_read_is_not_written() {
        let d = tmp("write-huge");
        let path = format!("{d}/{FILE}");
        let old = b"previous-valid-registry\n";
        std::fs::write(&path, old).unwrap();

        let mut r = Registry::new();
        // Прямое наполнение здесь намеренно: проверяется слой постоянного
        // формата, а не квадратичная стоимость двадцати тысяч публичных
        // declare. Все отдельные значения при этом представимы форматом.
        for id in 0..20_000u32 {
            r.fields.push(FieldDef {
                id,
                path: format!("p{id}_{}", "a".repeat(200)),
                kind: Kind::Text,
                group: 0,
                boost: 1.0,
                store: true,
                positions: true,
                index: true,
                since: 1,
                dead: None,
            });
        }
        r.next_field = r.fields.len() as u32;
        let error = r.save(&d).expect_err("нечитаемо большой реестр записан");
        let message = error.to_string();
        assert!(message.contains("предел") && message.contains("не прочтётся"), "{message}");
        assert_eq!(std::fs::read(&path).unwrap(), old, "отказ затронул прежний реестр");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Повреждённые связи схемы отказывают при открытии, до первого документа.
    #[test]
    fn corrupted_group_links_versions_and_tombstones_are_refused() {
        let cases = [
            (
                "дырка в группах",
                r#"{"format":1,"version":2,"next_field":1,"next_group":2,
                    "groups":[{"id":0,"path":"","since":1},{"id":2,"path":"items","since":2}],
                    "fields":[{"id":0,"path":"title","kind":"text","group":0,"since":2}]}"#,
            ),
            (
                "несуществующая группа",
                r#"{"format":1,"version":2,"next_field":1,"next_group":1,
                    "groups":[{"id":0,"path":"","since":1}],
                    "fields":[{"id":0,"path":"title","kind":"text","group":7,"since":2}]}"#,
            ),
            (
                "отрицательная версия похорон",
                r#"{"format":1,"version":2,"next_field":1,"next_group":1,
                    "groups":[{"id":0,"path":"","since":1}],
                    "fields":[{"id":0,"path":"title","kind":"text","group":0,"since":1,"dead":-1}]}"#,
            ),
            (
                "будущая версия поля",
                r#"{"format":1,"version":2,"next_field":1,"next_group":1,
                    "groups":[{"id":0,"path":"","since":1}],
                    "fields":[{"id":0,"path":"title","kind":"text","group":0,"since":3}]}"#,
            ),
            (
                "негодный путь из файла",
                r#"{"format":1,"version":2,"next_field":1,"next_group":1,
                    "groups":[{"id":0,"path":"","since":1}],
                    "fields":[{"id":0,"path":"../title","kind":"text","group":0,"since":2}]}"#,
            ),
        ];
        for (name, text) in cases {
            assert!(
                Registry::from_json(&json::parse(text).unwrap()).is_err(),
                "повреждение {name} принято"
            );
        }
    }
}

#[cfg(test)]
mod geo_satellite_tests {
    use super::*;

    /// Спутник точки, объявленный руками, отвергается при объявлении схемы.
    ///
    /// У `geo_point` есть скрытые спутники `<поле>.lat` и `<поле>.lon`: приём
    /// заполняет их из самой точки, а колонки под них пишутся вместе с ней.
    /// Объявленный рядом `место.lat` даёт вторую колонку с тем же именем файла
    /// — имя поля с точкой сводится к `f{номер}` ([`stem_of`]), — и контейнер
    /// отвергает кусок, записанный дважды.
    ///
    /// Отвергать надо здесь, а не там. Прежде узел принимал такую схему,
    /// отвечал на пачку `{"added":1}` с кодом 200, падал на первом сбросе и
    /// останавливал приём: документы приняты, потеряны, а клиенту сказано об
    /// успехе. Проверка закрепляет обе стороны — и отказ, и то, что сама точка
    /// объявляется по-прежнему свободно.
    #[test]
    fn a_hand_declared_geo_satellite_is_refused_before_any_document_is_taken() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-geosat-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        let decl = |path: &str, kind: crate::index::Kind| Decl {
            path: path.to_string(),
            kind,
            group: String::new(),
            boost: 1.0,
            store: true,
            positions: true,
            index: true,
        };
        for bad in [".lat", ".lon"] {
            std::fs::remove_dir_all(&dir).ok();
            let out = declare_all(
                &dir,
                &[
                    decl("place", crate::index::Kind::Geo),
                    decl(&format!("place{bad}"), crate::index::Kind::Double),
                ],
            );
            let why = out.err().map(|e| e.to_string()).unwrap_or_default();
            assert!(
                why.contains("спутник"),
                "схема со спутником {bad:?} принята — документы будут приняты и потеряны: {why:?}"
            );
        }
        // Сама точка и поле с точкой в имени, не относящееся к ней, законны.
        std::fs::remove_dir_all(&dir).ok();
        assert!(
            declare_all(
                &dir,
                &[
                    decl("place", crate::index::Kind::Geo),
                    decl("other.lat", crate::index::Kind::Double),
                ],
            )
            .is_ok(),
            "чужое поле с окончанием .lat к точке отношения не имеет и обязано объявляться"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Спутники, которые добавляет разбор публичной схемы, проходят тот же
    /// `declare_all`, но не считаются ручным повторным объявлением.
    #[test]
    fn generated_geo_satellites_are_declared_with_their_point() {
        let dir = std::env::temp_dir()
            .join(format!("svindb-geosat-generated-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&dir).ok();
        let point = Decl::root("place", crate::index::Kind::Geo);
        let satellite = |part: &str| Decl {
            path: format!("place.{part}"),
            kind: crate::index::Kind::Double,
            group: String::new(),
            boost: 1.0,
            store: false,
            positions: false,
            index: true,
        };
        let declared = declare_all(&dir, &[point, satellite("lat"), satellite("lon")])
            .expect("свои спутники точки отвергнуты");
        assert_eq!(declared.fids.len(), 3);
        std::fs::remove_dir_all(&dir).ok();
    }
}
