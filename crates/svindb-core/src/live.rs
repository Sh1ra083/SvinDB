//! Гашение документов: карта живых.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Имя файла карты.
pub const FILE: &str = "live.bin";

/// Карта живых документов сегмента.
pub struct Live {
    map: crate::mmap::Mapped,
    docs: usize,
    dead: AtomicUsize,
    /// Номер слова с признаком «в сегменте есть погашенные».
    ///
    /// Признак лежит **в самом файле**, последним словом за битовой картой, и
    /// это не украшение. Отдельный флаг в структуре был бы у каждого экземпляра
    /// свой, а экземпляров одного сегмента бывает несколько: тот, по которому
    /// идёт поиск, и тот, который только что открыло обслуживание. Гашение
    /// через один из них не поднимало бы флаг у других, и быстрый путь начал бы
    /// врать. Ровно это и поймал тест на двух читателей.
    ///
    /// Счётчик мёртвых для этого не годится: его строка пишется на каждое
    /// гашение и под нагрузкой мечется между ядрами. Это слово пишется один раз
    /// за всю жизнь сегмента и потому читается даром.
    ///
    /// `None` — карта старого образца, без признака. Тогда быстрого пути нет:
    /// медленно, но верно.
    flag: Option<usize>,
    /// Номер слова со счётчиком печати. `None` — карта старого образца.
    seal: Option<usize>,
    /// Сошлась ли сумма при открытии. См. [`Live::intact`].
    intact: bool,
    writable: bool,
    /// Менялась ли карта с прошлого уноса на диск.
    ///
    /// ## Зачем он рядом с `any_dead`
    ///
    /// `any_dead` отвечает «в сегменте есть погашенные» и, раз став правдой,
    /// правдой и остаётся. По нему один раз погашенный сегмент уносился на диск
    /// на **каждом** сбросе до конца жизни узла, хотя с прошлого раза в нём
    /// ничего не менялось. На сотне сегментов это незаметно; на индексе, где их
    /// тысячи, это тысяча системных вызовов `msync` на каждый сброс — работа,
    /// которой нет соответствующей правки.
    ///
    /// Признак живёт в памяти, а не в файле, и это верно: он отвечает не «что
    /// лежит на диске», а «делали ли мы что-то с прошлого раза». Гонка ему не
    /// страшна — гашение, случившееся посреди уноса, снова поднимет его, и
    /// следующий унос заберёт правку.
    dirty: std::sync::atomic::AtomicBool,
    /// Serialize sync callers so a clean flag never bypasses an in-flight msync.
    sync_gate: std::sync::Mutex<()>,
    /// Process-local поколение каждой успешной правки бита. Seal counter не
    /// подходит: после первого kill печать остаётся снятой до `sync`, и десятки
    /// следующих kill не меняют её число. Immutable ReadView обязан заметить
    /// каждый из них.
    generation: std::sync::atomic::AtomicU64,
}

/// Готовое содержимое карты, где живы все.
///
/// Хвост последнего слова гасится сразу: биты за числом документов не должны
/// считаться живыми, иначе подсчёт мёртвых поедет на любом сегменте, чей размер
/// не кратен шестидесяти четырём.
pub fn all_live(docs: usize) -> Vec<u8> {
    with_dead(docs, &[])
}

/// Готовое содержимое карты, где погашено отмеченное в `dead`.
pub fn with_dead(docs: usize, dead: &[bool]) -> Vec<u8> {
    let words = docs.div_ceil(64);
    let mut out = vec![u64::MAX; words];
    let rest = docs % 64;
    if rest != 0 {
        if let Some(last) = out.last_mut() {
            *last = (1u64 << rest) - 1;
        }
    }
    let mut any = 0u64;
    for (d, _) in dead.iter().take(docs).enumerate().filter(|(_, &x)| x) {
        out[d / 64] &= !(1u64 << (d % 64));
        any = 1;
    }
    // За картой — слово признака «есть погашенные», за ним печать.
    out.push(any);
    out.push(MAGIC);
    out.push(1); // печать поставлена: счётчик нечётный
    out.push(checksum(&out[..words]));
    out.iter().flat_map(|w| w.to_le_bytes()).collect()
}

/// Опознаватель печати. Отличает печать от случайных байт и ловит чужой порядок.
pub const MAGIC: u64 = 0x5356_494E_4C56_4531;

/// Сколько слов занимает печать: опознаватель, счётчик поколений, сумма.
pub const SEAL_WORDS: usize = 3;

/// Что печать говорит о карте.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Seal {
    /// Печати нет вовсе — карта написана до её появления. Проверять нечем.
    Absent,
    /// Печать снята: карту правят прямо сейчас, либо узел убили между гашением
    /// и сбросом. Судить о целости нельзя, и это не поломка.
    Open,
    /// Печать на месте и сходится.
    Ok,
    /// Печать на месте и **не** сходится: карта изменилась мимо гашения.
    Mismatch { want: u64, got: u64 },
}

/// Сумма слов карты — то, чем ловится порча битов внутри неё.
///
/// ## Почему сумма, а не настоящая контрольная сумма
///
/// Карту нельзя накрыть суммой из `sums.tsv`: та относится к состоянию на момент
/// сборки сегмента, а карта меняется **после** публикации, когда документы гасят.
/// Значит проверяемая целость обязана жить внутри самой карты.
///
/// Против злого умысла сложение слабо — кто может переписать карту, перепишет и
/// сумму. Но угроза здесь не умысел, а порча на диске, и против неё сложение
/// ловит всякое одиночное изменение бита: сумма меняется на степень двойки.
pub fn checksum(words: &[u64]) -> u64 {
    words.iter().fold(0u64, |a, w| a.wrapping_add(*w))
}

/// Сверяет печать, не мешая гашению.
///
/// ## Почему счётчик читается дважды
///
/// Гашение правит карту на месте, и проверяющий может застать её посередине:
/// бит уже сброшен, сумма ещё старая. Отличить это от порчи по одному снимку
/// нельзя — а объявить поломкой живую работу значит завести ложную тревогу,
/// которая обесценит и настоящие.
///
/// Поэтому счётчик читается до прохода и после. Вердикт выносится, только если
/// оба чтения дали одно и то же **нечётное** значение: нечётное — печать стоит,
/// одинаковое — за время прохода её не снимали.
pub fn verify_map(map: &crate::mmap::Mapped, docs: usize) -> Seal {
    let words = docs.div_ceil(64);
    if map.len_bytes() < (words + 1 + SEAL_WORDS) * 8 {
        return Seal::Absent;
    }
    let at = |w: usize| map.u64_at(w * 8);
    if at(words + 1) != MAGIC {
        return Seal::Absent;
    }
    let before = at(words + 2);
    if before % 2 == 0 {
        return Seal::Open;
    }
    let mut got = 0u64;
    for w in 0..words {
        got = got.wrapping_add(at(w));
    }
    let want = at(words + 3);
    if at(words + 2) != before {
        return Seal::Open;
    }
    if want == got {
        Seal::Ok
    } else {
        Seal::Mismatch { want, got }
    }
}

impl Live {
    /// Открывает карту сегмента. `None` — файла нет или он мал.
    ///
    /// Отсутствие файла означает «живы все» и обрабатывается вызывающим: так
    /// работают сегменты, собранные до появления карты, и так же обходится
    /// лишнее обращение в память на каждом документе там, где удалять нечего.
    ///
    /// Открыть пытаемся на запись, при отказе — на чтение: узел-реплика может
    /// держать каталог только на чтение, и поиск от этого страдать не должен.
    pub fn open(dir: &str, docs: usize) -> Option<Live> {
        let path = format!("{dir}/{FILE}");
        let map = crate::mmap::Mapped::open_rw(&path)
            .or_else(|_| crate::mmap::Mapped::open(&path))
            .ok()?;
        Live::over(map, docs)
    }

    /// Офлайновый аудит не вправе поправлять даже служебный флаг карты.
    pub fn open_read_only(dir: &str, docs: usize) -> Option<Live> {
        let map = crate::mmap::Mapped::open(&format!("{dir}/{FILE}")).ok()?;
        let live = Live::over(map, docs)?;
        // Writable open умеет восстановить flag, read-only обязан отказать,
        // иначе быстрый is_live сочтёт погашенные документы живыми.
        (!(live.dead() > 0 && !live.any_dead())).then_some(live)
    }

    /// Карта сегмента, собранного в память.
    ///
    /// Правится так же, как файловая, и по тому же праву: гашение это атомарный
    /// сброс бита в области, держатель которой один на всех читателей сегмента.
    /// Долговечности у неё нет — за неё отвечает журнал, и отметку журнала
    /// нельзя двигать за гашением, которое живёт только здесь.
    pub fn in_memory(bytes: &[u8], docs: usize) -> Option<Live> {
        Live::over(crate::mmap::Mapped::of_bytes(bytes), docs)
    }

    /// Разбирает и проверяет уже открытую карту — одним кодом на файл и память.
    fn over(map: crate::mmap::Mapped, docs: usize) -> Option<Live> {
        let words = docs.div_ceil(64);
        // Длина обязана быть **одной из двух**, а не «хотя бы такой».
        //
        // Карта с печатью длиннее карты без неё на три слова. Принимай «хотя бы
        // столько» — и обрезанная новая карта прошла бы как старая: печати нет,
        // проверять нечем, порча внутри не видна. То есть обрезание файла
        // отключало бы проверку, ради которой печать и заведена.
        //
        // Слово-признак требуется в обоих случаях: приём пишет его всегда, и его
        // отсутствие — это обрезанный файл, а не старый формат. Без признака
        // гашения перестают замечаться, и сегмент выглядит нетронутым.
        let len = map.len_bytes();
        let plain = (words + 1) * 8;
        let sealed_len = (words + 1 + SEAL_WORDS) * 8;
        if len != plain && len < sealed_len {
            return None;
        }
        // Куча правится наравне с разделяемым отображением: см.
        // [`crate::mmap::Mapped::is_memory`]. Отображение на чтение — нет.
        let writable = map.is_shared() || map.is_memory();
        let flag = Some(words);
        // Печать — за признаком: опознаватель, счётчик поколений, сумма.
        let sealed =
            map.len_bytes() >= (words + 1 + SEAL_WORDS) * 8 && map.u64_at((words + 1) * 8) == MAGIC;
        let seal = sealed.then_some(words + 2);
        let mut live = 0usize;
        for w in 0..words {
            let mut x = map.u64_at(w * 8);
            // Хвост последнего слова в счёт не идёт, чем бы он ни был заполнен.
            if w + 1 == words && !docs.is_multiple_of(64) {
                x &= (1u64 << (docs % 64)) - 1;
            }
            live += x.count_ones() as usize;
        }
        let intact = !matches!(verify_map(&map, docs), Seal::Mismatch { .. });
        let dead = docs - live;
        // Признак и счёт обязаны сойтись. Файл могли открыть, погасить в нём и
        // не дописать признак — например, старым движком.
        if dead > 0 {
            if let Some(w) = flag {
                if let Some(word) = map.word(w) {
                    if writable {
                        word.store(1, Ordering::Relaxed);
                    }
                }
            }
        }
        let generation = seal.map(|word| map.u64_at(word * 8)).unwrap_or(dead as u64);
        Some(Live {
            map,
            docs,
            dead: AtomicUsize::new(dead),
            flag,
            seal,
            intact,
            writable,
            // Первый унос делается всегда: открытие могло поднять признак
            // «есть погашенные» у карты, где его не было.
            dirty: std::sync::atomic::AtomicBool::new(true),
            sync_gate: std::sync::Mutex::new(()),
            generation: std::sync::atomic::AtomicU64::new(generation),
        })
    }

    /// Создаёт карту, где живы все, и открывает её.
    pub fn create(dir: &str, docs: usize) -> std::io::Result<Live> {
        std::fs::write(format!("{dir}/{FILE}"), all_live(docs))?;
        Live::open(dir, docs)
            .ok_or_else(|| std::io::Error::other("карта живых не открылась после записи"))
    }

    pub fn docs(&self) -> usize {
        self.docs
    }

    pub fn dead(&self) -> usize {
        self.dead.load(Ordering::Relaxed)
    }

    /// Можно ли править на месте. `false` — карта открыта только на чтение.
    pub fn writable(&self) -> bool {
        self.writable
    }

    /// Есть ли в сегменте погашенные. Признак общий для всех экземпляров.
    #[inline(always)]
    pub fn any_dead(&self) -> bool {
        match self.flag.and_then(|w| self.map.word(w)) {
            Some(word) => word.load(Ordering::Relaxed) != 0,
            // Без признака судить не по чему — считаем, что погашенные есть.
            None => true,
        }
    }

    #[inline(always)]
    pub fn is_live(&self, doc: u32) -> bool {
        if !self.any_dead() {
            return true;
        }
        self.map.u64_at((doc as usize >> 6) * 8) >> (doc & 63) & 1 == 1
    }

    /// Гасит документ. `true` — он был жив, то есть это настоящее удаление.
    ///
    /// Сброс бита идёт `fetch_and`, а не «прочитать, изменить, записать»: в
    /// одном слове лежат шестьдесят четыре документа, и два потока, гасящие
    /// соседние, иначе затёрли бы работу друг друга. Возврат `fetch_and` заодно
    /// отвечает, был ли документ жив, — без отдельного чтения и без зазора между
    /// проверкой и правкой.
    pub fn kill(&self, doc: u32) -> bool {
        if doc as usize >= self.docs || !self.writable {
            return false;
        }
        let Some(word) = self.map.word(doc as usize >> 6) else {
            return false;
        };
        // Печать снимается **до** правки бита, а не после. Обратный порядок
        // оставлял бы окно, в котором карта уже изменена, а печать ещё стоит:
        // проверяющий застал бы это и объявил порчей живую работу.
        self.unseal();
        let mask = 1u64 << (doc & 63);
        let was = word.fetch_and(!mask, Ordering::Relaxed);
        if was & mask == 0 {
            return false;
        }
        self.dead.fetch_add(1, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Release);
        self.dirty.store(true, Ordering::Relaxed);
        // Порядок важен: признак ставится **после** сброса бита. Обратный
        // порядок открывал бы окно, в котором читатель уже свернул с быстрого
        // пути, а бита в карте ещё нет.
        if let Some(f) = self.flag.and_then(|w| self.map.word(w)) {
            if f.load(Ordering::Relaxed) == 0 {
                f.store(1, Ordering::Relaxed);
            }
        }
        true
    }

    /// Уносит правки на диск. Зовётся на сброс, а не на каждое удаление.
    ///
    /// Карта без погашенных не сбрасывается вовсе, и это не поблажка, а
    /// тождество: править её на месте умеет только [`Live::kill`], и он же
    /// поднимает признак. Признак чист — значит в отображение никто не писал, и
    /// `msync` над ним ничего не уносит.
    ///
    /// Считается это по всем сегментам сразу на каждом сбросе, поэтому разница
    /// не косметическая: индекс без удалений делал тысячи `msync` с ожиданием
    /// вместо ноля. А удаления есть далеко не у всех.
    pub fn sync(&self) -> std::io::Result<()> {
        self.sync_with(|| self.map.sync())
    }

    fn sync_with(&self, sync: impl FnOnce() -> std::io::Result<()>) -> std::io::Result<()> {
        let _gate = self.sync_gate.lock().unwrap_or_else(|error| error.into_inner());
        if !self.any_dead() {
            return Ok(());
        }
        // Признак снимается **до** уноса, а не после. Гашение, случившееся
        // посреди него, поднимет его снова, и следующий унос заберёт правку;
        // обратный порядок терял бы такую правку до самого перезапуска.
        if !self.dirty.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        // Печать ставится **перед** уносом на диск: иначе на диск легла бы карта
        // со снятой печатью, и следующее открытие не смогло бы о ней судить.
        self.reseal();
        if let Err(error) = sync() {
            // An unsuccessful msync is still dirty. A retry must physically
            // synchronize the map before its WAL can be checkpointed.
            self.dirty.store(true, Ordering::SeqCst);
            return Err(error);
        }
        Ok(())
    }

    /// Снимает печать: счётчик из нечётного в чётное.
    ///
    /// Прибавлением через сравнение с обменом, а не безусловным: два потока,
    /// увидевшие нечётное, безусловным прибавлением вернули бы счётчик в
    /// нечётное — то есть «печать стоит» посреди правки.
    fn unseal(&self) {
        let Some(word) = self.seal.and_then(|w| self.map.word(w)) else { return };
        let mut cur = word.load(Ordering::Relaxed);
        while cur % 2 == 1 {
            match word.compare_exchange_weak(cur, cur + 1, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(now) => cur = now,
            }
        }
    }

    /// Ставит печать заново: считает сумму и делает счётчик нечётным.
    ///
    /// Порядок несущий: сперва сумма, потом счётчик. Обратный объявил бы карту
    /// запечатанной раньше, чем печать сошлась.
    fn reseal(&self) {
        let (Some(sw), true) = (self.seal, self.writable) else { return };
        let words = self.docs.div_ceil(64);
        let sum = (0..words).fold(0u64, |a, w| a.wrapping_add(self.map.u64_at(w * 8)));
        if let Some(word) = self.map.word(sw + 1) {
            word.store(sum, Ordering::Relaxed);
        }
        if let Some(word) = self.map.word(sw) {
            let cur = word.load(Ordering::Relaxed);
            word.store(cur | 1, Ordering::Relaxed);
        }
    }

    /// Сошлась ли печать при открытии карты.
    ///
    /// `false` значит одно из двух, и различить их отсюда нельзя:
    ///
    /// * порча на диске — бит в карте изменился сам по себе;
    /// * узел убили между гашением и сбросом, и на диск успели лечь не все
    ///   страницы карты. Тогда журнал приёма гашение восстановит, и расхождение
    ///   исчезнет при первом же сбросе.
    ///
    /// Поэтому открытие на этом **не отказывает**: воскресший или пропавший
    /// документ хуже, чем неоткрывшийся индекс, но не настолько, чтобы ронять
    /// узел там, где виноват обрыв. Сообщает об этом `check`.
    pub fn intact(&self) -> bool {
        self.intact
    }

    /// Есть ли у карты печать вообще. `false` — карта старого образца.
    pub fn has_checksum(&self) -> bool {
        self.seal.is_some()
    }

    /// Сколько байт занимает карта.
    pub fn bytes(&self) -> usize {
        self.map.len_bytes()
    }

    /// Поколение карты для кеша immutable ReadView.
    ///
    /// Process-local счётчик начинается с persisted seal/dead и растёт на
    /// каждом успешном kill. Новый экземпляр после reopen снова начинает с
    /// состояния файла, а опубликованный ReadView и все его запросы используют
    /// один и тот же `Live`, поэтому ни одна правка его кеш не минует.
    pub fn version(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Снимает копию карты на этот миг. См. [`Snapshot`].
    pub fn snapshot(&self) -> Snapshot {
        // Живы все — копировать нечего: пустой вектор и означает «все живы».
        if !self.any_dead() {
            return Snapshot { words: Vec::new(), docs: self.docs, dead: 0 };
        }
        let n = self.docs.div_ceil(64);
        let mut words = Vec::with_capacity(n);
        let mut live = 0usize;
        for w in 0..n {
            let mut x = self.map.u64_at(w * 8);
            // Хвост последнего слова в счёт не идёт: биты за числом документов
            // ничьи, и считать их живыми значило бы разойтись с `Live::open`.
            if w + 1 == n && !self.docs.is_multiple_of(64) {
                x &= (1u64 << (self.docs % 64)) - 1;
            }
            live += x.count_ones() as usize;
            words.push(x);
        }
        Snapshot { words, docs: self.docs, dead: self.docs - live }
    }
}

/// Копия карты живых на определённый миг.
///
/// ## Зачем копия, когда есть сама карта
///
/// Слияние идёт **без замка состава** и длится минутами. Всё это время удаления
/// продолжают гасить документы в его источниках, и карта под ним меняется.
/// Слияние по живой карте от этого читало бы разное на разных фазах: документ,
/// живой при сборке норм, мог оказаться мёртвым при сборке колонок, — и слитый
/// сегмент вышел бы несогласованным с самим собой.
///
/// Поэтому слияние берёт копию и идёт по ней от начала до конца, а публикация
/// сравнивает нынешнюю карту с копией и гасит в выходе то, что умерло за это
/// время. Так же поступает Lucene.
///
/// Стоит копия `docs/8` байт на источник — 8 МБ на 64 млн документов, — и то
/// лишь когда в сегменте есть погашенные: чистая карта копируется в пустой
/// вектор, потому что «живы все» проверяется признаком, а не битами.
pub struct Snapshot {
    /// Слова карты. Пусто — живы все.
    words: Vec<u64>,
    docs: usize,
    dead: usize,
}

impl Snapshot {
    /// Копия карты сегмента, у которого её нет вовсе: живы все.
    pub fn all(docs: usize) -> Snapshot {
        Snapshot { words: Vec::new(), docs, dead: 0 }
    }

    #[inline(always)]
    pub fn is_live(&self, doc: u32) -> bool {
        if self.words.is_empty() {
            return true;
        }
        match self.words.get(doc as usize >> 6) {
            Some(w) => w >> (doc & 63) & 1 == 1,
            // За пределами карты живых нет: документа с таким номером в сегменте
            // не было, и объявить его живым значило бы завести в слитом лишний.
            None => false,
        }
    }

    /// Все ли документы были живы на миг снятия.
    #[inline(always)]
    /// Тот же снимок, но живыми считаются только документы отрезка.
    ///
    /// ## Зачем это уборке
    ///
    /// Уборка крупного сегмента упирается в память: расход растёт вместе с
    /// размером источника, а источник тем крупнее, чем дольше живёт индекс.
    /// Разделив выход на части, узел платит за каждую отдельно — и платит
    /// столько, сколько сам назначил, независимо от того, до какого размера
    /// дорос сегмент.
    ///
    /// Отрезок задаётся **номерами документов источника**, а не порядковыми
    /// номерами живых: слияние обходит источник по номерам, и всякая другая
    /// нумерация потребовала бы второго прохода по карте.
    pub fn only(&self, from: u32, upto: u32) -> Snapshot {
        let mut words = vec![0u64; self.docs.div_ceil(64)];
        let mut dead = 0usize;
        for d in 0..self.docs as u32 {
            if d >= from && d < upto && self.is_live(d) {
                words[d as usize >> 6] |= 1u64 << (d & 63);
            } else {
                dead += 1;
            }
        }
        Snapshot { words, docs: self.docs, dead }
    }

    pub fn all_live(&self) -> bool {
        self.words.is_empty()
    }

    pub fn dead(&self) -> usize {
        self.dead
    }

    pub fn docs(&self) -> usize {
        self.docs
    }

    /// Куча, удерживаемая immutable-снимком. У полностью живого сегмента ноль:
    /// пустой вектор и означает «живы все».
    pub fn bytes(&self) -> usize {
        self.words.len().saturating_mul(std::mem::size_of::<u64>())
    }

    /// Перебирает документы, живые на миг снятия и погашенные к **сейчас**.
    ///
    /// Ровно то, что случилось с сегментом, пока по копии шло слияние. Гасить
    /// их в выходе обязан тот, кто слияние публикует: иначе удаление, попавшее
    /// в это окно, воскресло бы вместе со слитым сегментом.
    ///
    /// Пословно, а не побитно: обычно разницы нет вовсе, и шестьдесят четыре
    /// документа отсекаются одним сравнением. На чистом сегменте не читается и
    /// этого — признак «есть погашенные» лежит в самом файле карты.
    pub fn newly_dead(&self, now: &Live, mut f: impl FnMut(u32)) {
        if !now.any_dead() {
            return;
        }
        let words = self.docs.div_ceil(64);
        for w in 0..words {
            // Карта короче ожидаемого — читать нечего. Случиться этого не может:
            // число документов сегмента неизменно, а карта заведена по нему.
            let Some(word) = now.map.word(w) else { return };
            let was = match self.words.get(w) {
                Some(x) => *x,
                // Копия пуста — на миг снятия были живы все. Хвост последнего
                // слова при этом ничей, и объявлять его живым нельзя.
                None if w + 1 == words && !self.docs.is_multiple_of(64) => {
                    (1u64 << (self.docs % 64)) - 1
                }
                None => u64::MAX,
            };
            let mut gone = was & !word.load(Ordering::Relaxed);
            while gone != 0 {
                let bit = gone.trailing_zeros();
                gone &= gone - 1;
                f((w * 64) as u32 + bit);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-live-{name}"));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().to_string()
    }

    #[test]
    fn without_a_file_there_is_no_map_at_all() {
        let dir = tmp("none");
        assert!(Live::open(&dir, 200).is_none(), "нет файла — нет и карты");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn readonly_never_repairs_or_resurrects_a_legacy_flag() {
        let dir = tmp("readonly-legacy-flag");
        let path = format!("{dir}/{FILE}");
        let bytes: Vec<_> = [0b1110u64, 0].into_iter().flat_map(u64::to_le_bytes).collect();
        std::fs::write(&path, &bytes).unwrap();
        assert!(Live::open_read_only(&dir, 4).is_none());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        std::fs::write(&path, with_dead(4, &[true])).unwrap();
        let live = Live::open_read_only(&dir, 4).unwrap();
        assert!(!live.writable());
        assert!(!live.is_live(0));
        assert!(live.is_live(1));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Сброс карты без погашенных не делает ничего, с погашенными — уносит всё.
    ///
    /// Проверяется не скорость, а тождество, на котором стоит пропуск: править
    /// карту на месте умеет только `kill`, и он же поднимает признак. Пока
    /// признак чист, в отображение никто не писал, и `msync` над ним пустой.
    /// Промах здесь означал бы потерю удалений при потере питания, поэтому
    /// проверяются обе половины: и что чистая карта пропускается, и что грязная
    /// доходит до диска.
    #[test]
    fn a_clean_map_needs_no_sync_but_a_dirty_one_does() {
        let dir = tmp("sync-skip");
        let live = Live::create(&dir, 256).unwrap();
        assert!(!live.any_dead(), "свежая карта обязана быть чистой");
        live.sync().expect("сброс чистой карты — не отказ");

        // Ничего не изменилось: признак по-прежнему чист.
        assert!(!live.any_dead());
        assert_eq!(live.dead(), 0);

        // Гашение поднимает признак, и тогда сброс обязан дойти до диска.
        assert!(live.kill(7));
        assert!(live.any_dead(), "гашение обязано поднять признак");
        live.sync().expect("сброс грязной карты");

        // Проверка не в том, что `sync` вернул `Ok`, а в том, что правка
        // читается заново открытой картой.
        let back = Live::open(&dir, 256).unwrap();
        assert!(back.any_dead(), "признак не дошёл до диска");
        assert!(!back.is_live(7), "гашение не дошло до диска");
        assert_eq!(back.dead(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_failed_sync_stays_dirty_until_a_real_retry_succeeds() {
        let dir = tmp("sync-retry");
        let live = Live::create(&dir, 128).unwrap();
        assert!(live.kill(7));
        let calls = std::cell::Cell::new(0);
        assert!(live
            .sync_with(|| {
                calls.set(calls.get() + 1);
                Err(std::io::Error::other("injected sync failure"))
            })
            .is_err());
        assert!(live.dirty.load(Ordering::SeqCst));
        live.sync_with(|| {
            calls.set(calls.get() + 1);
            live.map.sync()
        })
        .unwrap();
        assert_eq!(calls.get(), 2, "the retry must reach msync");
        live.sync_with(|| panic!("clean map must skip msync")).unwrap();
        assert!(!Live::open(&dir, 128).unwrap().is_live(7));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn concurrent_sync_cannot_skip_an_inflight_failed_sync() {
        let dir = tmp("sync-concurrent-retry");
        let live = std::sync::Arc::new(Live::create(&dir, 128).unwrap());
        assert!(live.kill(7));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first = std::sync::Arc::clone(&live);
        let first = std::thread::spawn(move || {
            first.sync_with(|| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Err(std::io::Error::other("first sync failed"))
            })
        });
        entered_rx.recv().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (retried_tx, retried_rx) = std::sync::mpsc::channel();
        let second = std::sync::Arc::clone(&live);
        let second = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            second.sync_with(|| {
                retried_tx.send(()).unwrap();
                second.map.sync()
            })
        });
        started_rx.recv().unwrap();
        assert!(matches!(retried_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)));
        release_tx.send(()).unwrap();
        assert!(first.join().unwrap().is_err());
        second.join().unwrap().unwrap();
        retried_rx.recv().unwrap();
        assert!(!live.dirty.load(Ordering::SeqCst));
        assert!(!Live::open(&dir, 128).unwrap().is_live(7));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn kill_survives_a_round_trip() {
        let dir = tmp("kill");
        let live = Live::create(&dir, 1000).unwrap();
        assert!(live.writable());
        assert!(live.kill(0));
        assert!(live.kill(999));
        assert!(live.kill(500));
        assert!(!live.kill(500), "повторное гашение не считается удалением");
        assert!(!live.kill(1000), "за границей сегмента гасить нечего");
        assert_eq!(live.dead(), 3);
        live.sync().unwrap();

        let back = Live::open(&dir, 1000).unwrap();
        assert_eq!(back.dead(), 3);
        assert!(!back.is_live(0) && !back.is_live(500) && !back.is_live(999));
        assert!(back.is_live(1) && back.is_live(998));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Правка видна сразу и без переоткрытия — ради этого всё и делалось.
    #[test]
    fn a_second_reader_sees_the_kill_without_reopening() {
        let dir = tmp("shared");
        let writer = Live::create(&dir, 500).unwrap();
        let reader = Live::open(&dir, 500).unwrap();
        assert!(reader.is_live(42));
        writer.kill(42);
        assert!(!reader.is_live(42), "читатель обязан увидеть гашение немедленно");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Хвост последнего слова не должен считаться живым.
    #[test]
    fn tail_beyond_the_last_document_is_not_counted() {
        let dir = tmp("tail");
        let live = Live::create(&dir, 100).unwrap();
        assert_eq!(live.dead(), 0, "сто документов, два слова, хвост не в счёт");
        assert!(!live.any_dead());
        assert_eq!(Live::open(&dir, 100).unwrap().dead(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Соседние документы в одном слове гасятся независимо.
    ///
    /// Это проверка на то, ради чего стоит `fetch_and`: при чтении-правке-записи
    /// два потока, взявшие одно слово, затирали бы работу друг друга.
    #[test]
    fn neighbours_in_one_word_do_not_erase_each_other() {
        let dir = tmp("word");
        let live = Live::create(&dir, 64).unwrap();
        std::thread::scope(|s| {
            for t in 0..8u32 {
                let live = &live;
                s.spawn(move || {
                    for d in (t..64).step_by(8) {
                        assert!(live.kill(d));
                    }
                });
            }
        });
        assert_eq!(live.dead(), 64, "погашены должны быть все шестьдесят четыре");
        std::fs::remove_dir_all(&dir).ok();
    }
}
