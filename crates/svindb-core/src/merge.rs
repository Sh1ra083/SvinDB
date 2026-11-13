//! Слияние сегментов: несколько в один.

use std::fs::{create_dir_all, File};
use std::io::Write;

use crate::bits::BLOCK;
use crate::dict;
use crate::ids;
use crate::index::{self, Index};

/// Поля слитого сегмента: объединение полей источников по устойчивому номеру.
///
/// ## Почему порядок нельзя выбрать произвольно
///
/// Слияние словарей идёт k-путевым обходом по ключу «поле, слово», и опирается
/// на то, что поток каждого источника **уже отсортирован** по этому ключу.
/// Отсортирован он по местным номерам полей. Значит порядок выхода обязан
/// сохранить относительный порядок полей у каждого источника — иначе поток
/// перестанет быть возрастающим, и обход молча потеряет слова.
struct Plan {
    /// Устойчивые номера полей выхода, в порядке выхода.
    fids: Vec<u32>,
    /// По источнику: местный номер поля выхода. `NONE` — поля в нём нет.
    local: Vec<Vec<usize>>,
    /// По источнику: номер выхода для его местного поля.
    out_of: Vec<Vec<usize>>,
}

/// Поля в источнике нет.
const NONE: usize = usize::MAX;

/// Устойчивые номера полей сегмента в его порядке — вход для [`plan`].
pub fn field_ids(ix: &Index) -> Vec<u32> {
    ix.fields.iter().map(|f| f.fid).collect()
}

/// Подпись схемы сегмента: хеш его устойчивых номеров полей **в порядке
/// объявления**.
///
/// ## Зачем
///
/// [`plan`] обязан знать, есть ли у окна общий порядок полей. Честный ответ
/// даёт только [`field_order`] — топологическая сортировка со списками смежности
/// на `Vec::contains` и `Vec::remove(0)`, то есть квадратичная по числу узлов, —
/// и звалась она на каждом шаге каждого окна. При окне в восемь и двадцати полях
/// это до ста шестидесяти узлов на построение и до восьми построений на сегмент;
/// на десяти тысячах сегментов — порядка двух миллиардов действий **каждые две
/// секунды**, и всё под замком состава.
///
/// Подпись отвечает на тот же вопрос одним сравнением там, где схема у всех
/// сегментов одна, — а это подавляющее большинство индексов.
///
/// ## Почему порядок, а не состав
///
/// Мешает окну не разный набор полей — его слияние умеет объединять, — а
/// **противоречие в порядке**. Поэтому подпись считается от последовательности,
/// и у переставленных полей она обязана быть разной.
///
/// ## Почему хеш, а не сам вектор
///
/// Вектор пришлось бы таскать и сравнивать целиком; подпись — восемь байт,
/// снятых один раз при открытии сегмента. От неё при этом не требуется
/// отсутствия столкновений: несовпадение подписей — окончательный ответ «схемы
/// разные», а совпадение планировщик подтверждает равенством самих векторов.
pub fn schema_sig(fids: &[u32]) -> u64 {
    let mut bytes = Vec::with_capacity(fids.len() * 4);
    for f in fids.iter() {
        bytes.extend_from_slice(&f.to_le_bytes());
    }
    let digest = crate::hash::sha256(&bytes);
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest[..8]);
    u64::from_le_bytes(head)
}

// Сколько раз `field_order` позвали на **этом** потоке.
//
// Только для проверок: ими доказывается, что при одной схеме на весь набор
// планировщик не зовёт сортировку вовсе. Счётчик потоковый, а не общий, затем,
// что проверки идут параллельно и общий считал бы чужие вызовы.
#[cfg(test)]
thread_local! {
    static ORDER_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Общий порядок полей, сохраняющий порядок каждого источника.
///
/// `None` — общего порядка нет: два источника упорядочили одни и те же поля
/// по-разному. Слить их словари тогда нечем, и об этом надо сказать, а не
/// догадываться.
pub fn field_order(orders: &[Vec<u32>]) -> Option<Vec<u32>> {
    #[cfg(test)]
    ORDER_CALLS.with(|c| c.set(c.get() + 1));
    let mut fids: Vec<u32> = Vec::new();
    for o in orders.iter() {
        for f in o.iter() {
            if !fids.contains(f) {
                fids.push(*f);
            }
        }
    }
    let at = |fid: u32| fids.iter().position(|x| *x == fid).expect("узел заведён");
    let n = fids.len();
    let mut after: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut indeg = vec![0usize; n];
    for o in orders.iter() {
        for w in o.windows(2) {
            let (a, b) = (at(w[0]), at(w[1]));
            if !after[a].contains(&b) {
                after[a].push(b);
                indeg[b] += 1;
            }
        }
    }
    let mut order: Vec<usize> = Vec::with_capacity(n);
    let mut ready: Vec<usize> = (0..n).filter(|i| indeg[*i] == 0).collect();
    ready.sort_unstable();
    while let Some(i) = ready.first().copied() {
        ready.remove(0);
        order.push(i);
        let next = after[i].clone();
        for j in next {
            indeg[j] -= 1;
            if indeg[j] == 0 {
                ready.push(j);
                ready.sort_unstable();
            }
        }
    }
    (order.len() == n).then(|| order.iter().map(|&i| fids[i]).collect())
}

impl Plan {
    fn build(sources: &[&Index]) -> Result<Plan, String> {
        let orders: Vec<Vec<u32>> = sources.iter().map(|s| field_ids(s)).collect();
        let Some(fids) = field_order(&orders) else {
            return Err("поля упорядочены в сегментах по-разному: общего порядка нет, и слить их \
                 словари нечем"
                .to_string());
        };

        // Сверка объявлений: под одним номером обязано быть одно поле.
        for s in sources.iter() {
            for f in s.fields.iter() {
                let Some(first) =
                    sources.iter().find_map(|x| x.fields.iter().find(|g| g.fid == f.fid))
                else {
                    continue;
                };
                if first.name != f.name || first.kind != f.kind {
                    return Err(format!(
                        "поле {}: {}:{} против {}:{} — под одним номером два разных поля",
                        f.fid,
                        f.name,
                        f.kind.spec(),
                        first.name,
                        first.kind.spec()
                    ));
                }
            }
        }

        let mut local = Vec::with_capacity(sources.len());
        let mut out_of = Vec::with_capacity(sources.len());
        for s in sources.iter() {
            let mut l = vec![NONE; fids.len()];
            let mut o = vec![NONE; s.fields.len()];
            for (j, f) in s.fields.iter().enumerate() {
                let k = fids.iter().position(|x| *x == f.fid).expect("узел заведён");
                l[k] = j;
                o[j] = k;
            }
            local.push(l);
            out_of.push(o);
        }
        Ok(Plan { fids, local, out_of })
    }

    /// Описание поля выхода: берётся у первого источника, где оно есть.
    fn field<'a>(&self, sources: &[&'a Index], f: usize) -> &'a index::Field {
        for (i, s) in sources.iter().enumerate() {
            let j = self.local[i][f];
            if j != NONE {
                return &s.fields[j];
            }
        }
        unreachable!("поле выхода без единого источника")
    }
}

/// Что вышло из слияния.
pub struct Merged {
    pub docs: usize,
    pub terms: usize,
    pub postings: u64,
    pub bytes: u64,
    /// По источнику: новый номер каждого его документа; `u32::MAX` — погашен.
    ///
    /// Пусто, когда погашенных не было вовсе: тогда номера считаются сдвигом,
    /// см. [`Merged::base`]. Заводить таблицу и там значило бы четыре байта на
    /// документ на ровном месте.
    ///
    /// Наружу она нужна ровно затем, что слияние идёт **без замка состава**:
    /// пока оно работает, удаление может погасить документ в источнике, и
    /// публикации надо знать, какой номер выхода этому документу соответствует.
    /// Разбор — у [`Merged::out_doc`].
    pub remap: Vec<Vec<u32>>,
    /// По источнику: сдвиг его номеров в выходе, когда погашенных не было.
    pub base: Vec<u32>,
    /// Секунды по фазам: нормы, списки, колонки, содержимое, идентификаторы.
    pub phases: Vec<(&'static str, f64)>,
    /// Байты, которые каждая фаза держала в куче на своём пике.
    ///
    /// ## Зачем измерять по фазам, а не целиком
    ///
    /// Оценка расхода слияния складывается из слагаемых, и до сих пор поверить
    /// её можно было только общей суммой: сошлась — хорошо, разошлась — искать
    /// негде. Между тем фазы идут **по очереди**, и в пик входит наибольшая, а
    /// не сумма всех. Не зная, какая именно фаза дороже, константы приходится
    /// подбирать целиком — то есть гадать.
    ///
    /// Считается только в профилировочной сборке и только по просьбе
    /// (`SVINDB_MERGE_PROFILE`): счётчик стоит двух атомарных действий на
    /// каждое выделение, и в боевой сборке этой цены нет.
    pub peaks: Vec<(&'static str, usize)>,
    /// Сколько кусков хранимого слияние **сжало заново**.
    ///
    /// Мера, а не украшение: перенесённые байт в байт сюда не попадают, и на
    /// укрупнении чистых сегментов здесь обязан быть ноль. Величина эта —
    /// единственное, чем отличается перенос от пересборки снаружи: содержимое у
    /// них одинаково по договору, а вот работа разная.
    pub packed: usize,
}

impl Merged {
    /// Каким номером документ `d` источника `i` лёг в выход. `None` — не лёг,
    /// потому что был погашен ещё до слияния.
    pub fn out_doc(&self, i: usize, d: u32) -> Option<u32> {
        if self.remap.is_empty() {
            // Погашенных не было: номера сдвинуты сегмент за сегментом.
            return self.base.get(i).map(|b| b + d);
        }
        match self.remap.get(i)?.get(d as usize).copied()? {
            u32::MAX => None,
            n => Some(n),
        }
    }
}

/// Чем ограничена работа слияния.
#[derive(Clone, Copy, Debug, Default)]
pub struct Limits {
    /// Сколько потоков отдавать сжатию хранимого. `0` — сколько решит машина.
    ///
    /// ## Зачем это вообще ограничивать
    ///
    /// [`crate::store::build`] поднимает `available_parallelism()` потоков, то
    /// есть **все** ядра узла. Пока слияние занимало секунды, это было незаметно;
    /// на пятигигабайтном сегменте оно занимает минуты, и всё это время фоновое
    /// обслуживание отбирает ядра у приёма, который сжимает своё содержимое из
    /// своего потока. Обслуживание не вправе вытеснять работу, ради которой узел
    /// поднят.
    ///
    /// Перенос кусков (см. [`plan_carry`]) компрессор почти не зовёт, и там это
    /// ничего не меняет. Зовёт пересборка: грязные куски и запасной путь, — а
    /// именно она и идёт минутами.
    pub threads: usize,
}

/// Замер памяти по фазам слияния.
///
/// ## Почему по просьбе, а не всегда
///
/// Счётчик выделений стоит двух атомарных действий на каждый вызов
/// распределителя. Это принято измерительным контрактом для коротких
/// профилировочных прогонов и **не** принято для боевого узла. Поэтому замер
/// живёт только в сборке `alloc-stats` и включается переменной среды: сборка
/// без неё не содержит ни счётчика, ни этих строк.
///
/// ## Почему отсечка, а не замыкание
///
/// Фазы слияния — не замыкания, а участки одной длинной функции. Обернуть
/// каждую в замыкание значило бы перекроить её ради измерения; отсечка с явным
/// основанием даёт то же число, не трогая порядок работы.
struct Profile {
    #[cfg(feature = "alloc-stats")]
    on: bool,
    #[cfg(feature = "alloc-stats")]
    base: isize,
    out: Vec<(&'static str, usize)>,
}

impl Profile {
    fn start() -> Profile {
        #[cfg(feature = "alloc-stats")]
        {
            let on = std::env::var_os("SVINDB_MERGE_PROFILE").is_some();
            let base = if on { crate::alloc::mark() } else { 0 };
            Profile { on, base, out: Vec::new() }
        }
        #[cfg(not(feature = "alloc-stats"))]
        Profile { out: Vec::new() }
    }

    #[allow(unused_variables)]
    fn cut(&mut self, name: &'static str) {
        #[cfg(feature = "alloc-stats")]
        if self.on {
            self.out.push((name, crate::alloc::since(self.base)));
            self.base = crate::alloc::mark();
        }
    }

    fn finish(self) -> Vec<(&'static str, usize)> {
        self.out
    }
}

/// Влезает ли смещение внутри сегмента в четыре байта.
///
/// ## Зачем отдельная проверка у укрупнения
///
/// Смещения слова уходят в опоры `terms.idx` и в запись словаря
/// тридцатидвухбитными. Приём это проверяет ассертом ([`crate::indexer`]), а
/// укрупнение — не проверяло, хотя крупные сегменты делает именно оно: приём
/// режет по `--flush`, а сюда приходит сумма нарезанного.
///
/// Молчаливое переполнение здесь обходится дороже всего остального в модуле.
/// Обёрнутое смещение ложится в `terms.idx`; на следующем открытии `walk_terms`
/// видит расхождение и отказывает; отказ теряется в перезагрузке состава — а
/// исходные сегменты к тому времени уже снесены уборкой. Индекс неоткрываем, и
/// поднимать его не из чего.
///
/// Отказ, а не паника: `merge` возвращает `Result`, и вызывающий
/// ([`crate::maintain::once`]) обязан узнать, что укрупнение не вышло, и
/// оставить исходные сегменты в списке.
///
/// Потолок — сам `u32::MAX`: это значение ещё представимо, и приём допускает
/// его тем же сравнением. Берётся он у [`crate::dict::offset_ceiling`], общего
/// на все пять охран: разойдись они, укрупнение отказывало бы там, где приём
/// пропускает, и наоборот. Он же — тот шов, которым проверяется, что эти три
/// вызова и правда стоят на пути записи.
///
/// ## Связь с мерой окна
///
/// Каждый сторожимый здесь поток обязан быть назван и в [`crate::sums::Bytes`]:
/// политика укрупнения меряет окно байтами именно по тем частям, о которые
/// слияние спотыкается. Разойдись эти два перечня — окно набиралось бы по
/// частям, потолок которых ещё не достигнут, а слияние отказывало бы по
/// пропущенной, и так каждый заход. Поток «слова» — это `Bytes::terms`.
fn fits_offset(len: u64, what: &str, out: &str, at: usize) -> std::io::Result<()> {
    let ceiling = crate::dict::offset_ceiling();
    if len <= ceiling {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{out}: {what} сегмента перевалили за четыре гигабайта на {at}-м слове \
             ({len} байт при потолке {ceiling}) — уменьшите целевой размер сегмента (--target)"
        ),
    ))
}

/// Поток сегмента, который пишется **по ходу обхода**, а не собирается в памяти.
///
/// Ложится он отдельным файлом, а куском контейнера становится позже — см.
/// [`pour`]: открытым в контейнере бывает один кусок, а обход кормит четыре
/// потока разом.
///
/// ## Зачем
///
/// `pos.bin` и `terms.bin` растут вместе со словарём и до правки жили в
/// `Vec<u8>` целиком: на сегменте, упирающемся в потолок четырёх гигабайт,
/// позиции — это четыре гигабайта оперативной памяти, и ровно они делали пик
/// слияния линейным по размеру выхода. Ни тот ни другой поток после записи не
/// перечитывается: смещения внутрь них считаются от длины, а длина известна и
/// без содержимого.
///
/// ## Почему длина считается здесь, а не у буфера
///
/// Смещение, которое уходит в `terms.idx` и в запись слова, — это смещение **в
/// файле**. Буфер опустошается, файл нет, поэтому счётчик один и он тут.
/// Считай кто-нибудь длину по буферу — опоры поехали бы ровно на первом
/// сбросе, то есть на любом сегменте крупнее буфера, и никогда на маленьком.
struct Sink {
    sink: std::io::BufWriter<File>,
    /// Сколько байт уже отдано. Это и есть длина файла.
    len: usize,
}

/// Сколько держать в буфере, прежде чем отдать ядру.
const SINK_BUF: usize = 1 << 18;

impl Sink {
    fn create(path: &str) -> std::io::Result<Sink> {
        Ok(Sink { sink: std::io::BufWriter::with_capacity(SINK_BUF, File::create(path)?), len: 0 })
    }

    /// Дописывает кусок и опустошает `buf`: писателю выше он больше не нужен.
    fn take(&mut self, buf: &mut Vec<u8>) -> std::io::Result<()> {
        self.sink.write_all(buf)?;
        self.len += buf.len();
        buf.clear();
        Ok(())
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    /// Дописывает хвост буфера и закрывает файл.
    ///
    /// Без `sync_all`, и это не упущение: файл здесь — перевалочный. Содержимое
    /// его тут же переезжает куском контейнера ([`pour`]), а сам он сносится.
    /// До носителя доводится контейнер, и делает это `Writer::finish` — довести
    /// заодно и черновик значило бы синхронно ждать диск ради байтов, которых
    /// через мгновение не станет.
    fn finish(mut self) -> std::io::Result<()> {
        self.sink.flush()?;
        self.sink.into_inner().map_err(|e| e.into_error())?;
        Ok(())
    }
}

/// Сливает сегменты в новый каталог по их нынешним картам живых.
///
/// Исходные не трогаются: слияние их только читает. Публикация и удаление —
/// дело вызывающего, и в этом порядке.
///
/// Годится там, где состав под замком на всё время работы: разовый запуск,
/// проверки, `svindb merge`. Обслуживанию узла нужен [`merge_as`] — оно идёт
/// без замка, и карта живых под ним меняется.
/// Помещается ли нумерация слитого сегмента в номер документа.
///
/// ## Зачем отдельной охраной, а не проверкой на месте
///
/// Номера выхода считаются в двух местах: сдвиг начала каждого источника
/// (`docs as u32`) и сквозной счётчик живых (`next += 1`). В выпускной сборке
/// проверок переполнения нет — `overflow-checks` выключены, — поэтому оба
/// молча заворачиваются. Последствие не «ошибка», а **порча**: у разных
/// документов совпадают номера, а заголовок при этом проходит проверку, потому
/// что после заворота `docs` оказывается меньше потолка. Сегмент открывается и
/// отвечает на запросы неверно.
///
/// Хуже того, `u32::MAX` служит меткой «погашен» (`remap`), так что документ,
/// которому достался последний номер, стал бы невидимым — молча и навсегда.
/// Поэтому предел на единицу строже разрядности.
///
/// ## Почему это не отвлечённая опасность
///
/// У узла окно закрыто настройкой `target_docs`, но `svindb merge <каталог>
/// <за_раз> <потолок>` берёт оба числа как есть. На индексе в миллиарды строк
/// достаточно один раз назвать потолок побольше.
fn numbering_fits(total: usize) -> Result<(), String> {
    // Последний номер отдан метке «погашен», значит документов не больше, чем
    // `u32::MAX - 1`, а всего номеров — `u32::MAX`.
    const CEILING: usize = u32::MAX as usize;
    if total >= CEILING {
        return Err(format!(
            "слияние не берётся: {total} документов не поместятся в номер \
             документа шириной 32 разряда (потолок {}), а заворот дал бы разным \
             документам один номер молча; уменьшите окно слияния",
            CEILING - 1
        ));
    }
    Ok(())
}

pub fn merge(sources: &[&Index], out: &str) -> std::io::Result<Merged> {
    merge_as(sources, None, out, Limits::default())
}

/// То же с копиями карт живых и ограничением работы.
///
/// `alive` — по копии на источник; `None` — читать карты как есть. Копия нужна
/// тому, кто сливает **без замка состава**: см. [`crate::live::Snapshot`].
pub fn merge_as(
    sources: &[&Index],
    alive: Option<&[crate::live::Snapshot]>,
    out: &str,
    lim: Limits,
) -> std::io::Result<Merged> {
    for source in sources {
        if let Some(field) = source.fields.iter().find(|f| f.legacy_keyword_values) {
            return Err(std::io::Error::new(std::io::ErrorKind::Unsupported, format!(
                "поле {} сегмента {} требует migration keyword values из authoritative source в layout 12",
                field.name, source.dir,
            )));
        }
        if let Some(field) = source.fields.iter().find(|f| f.legacy_multivalue) {
            return Err(std::io::Error::new(std::io::ErrorKind::Unsupported, format!(
                "поле {} сегмента {} требует migration числовых массивов из authoritative source; scalar merge потерял бы значения",
                field.name, source.dir,
            )));
        }
    }
    if let Some(a) = alive {
        if a.len() != sources.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "слиянию дано {} копий карт живых на {} источников — сливать по \
                     чужой карте значило бы выбросить не те документы",
                    a.len(),
                    sources.len()
                ),
            ));
        }
        for (i, s) in sources.iter().enumerate() {
            if a[i].docs() != s.docs {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "{}: копия карты живых на {} документов при {} в сегменте",
                        s.dir,
                        a[i].docs(),
                        s.docs
                    ),
                ));
            }
        }
    }
    if sources.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "сливать нечего: список сегментов пуст",
        ));
    }
    // Нумерация выхода обязана поместиться в номер документа — до того, как
    // будет сделана хоть какая-то работа.
    if let Err(why) = numbering_fits(sources.iter().map(|s| s.docs).sum()) {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, why));
    }
    let plan = Plan::build(sources)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    // Серверный governor получает точки уступки из длинных merge/reclaim
    // циклов. Hook опционален: standalone merge и тесты продолжают работать
    // без серверной очереди.
    let mut maintenance_ticker = crate::work::CompactionTicker::default();

    let nfields = plan.fids.len();
    create_dir_all(out)?;
    let mut phases: Vec<(&'static str, f64)> = Vec::new();
    let mut prof = Profile::start();
    let mut mark = std::time::Instant::now();

    // Сдвиг номеров: сегмент за сегментом, в том порядке, в каком они даны.
    let mut base = Vec::with_capacity(sources.len());
    let mut docs = 0usize;
    for s in sources.iter() {
        base.push(docs as u32);
        docs += s.docs;
    }

    // Мёртвые документы при слиянии **выбрасываются**: их номера не переносятся,
    // а живые перенумеровываются подряд. Это единственное место, где место,
    // занятое удалёнными, возвращается на диск.
    //
    // Живость берётся у копии, если она дана: карта под слиянием меняется, и
    // прочитай две фазы разное — слитый сегмент разошёлся бы сам с собой.
    let live_at = |i: usize, d: u32| match alive {
        Some(a) => a[i].is_live(d),
        None => sources[i].is_live(d),
    };
    let any_dead = match alive {
        Some(a) => a.iter().any(|x| !x.all_live()),
        None => sources.iter().any(|s| !s.all_live()),
    };
    // Новый номер каждого документа исходного сегмента; `u32::MAX` — погашен.
    let mut remap: Vec<Vec<u32>> = Vec::new();
    if any_dead {
        let mut next = 0u32;
        for (i, s) in sources.iter().enumerate() {
            let mut m = Vec::with_capacity(s.docs);
            for d in 0..s.docs as u32 {
                if live_at(i, d) {
                    m.push(next);
                    next += 1;
                } else {
                    m.push(u32::MAX);
                }
                if !maintenance_ticker.tick(4) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "compaction остановлен graceful shutdown",
                    ));
                }
            }
            remap.push(m);
        }
        docs = next as usize;
    }

    // Полосы арности групп. Ординал принадлежит сегменту: перенести его как
    // есть значило бы указать значениями в чужое место, поэтому арности
    // склеиваются по живым документам, а ординалы считаются заново.
    let ngroups = sources
        .iter()
        .flat_map(|s| s.fields.iter().map(|f| f.group as usize + 1))
        .max()
        .unwrap_or(1)
        .max(1);
    let mut arity: Vec<Vec<u32>> = vec![Vec::new(); ngroups];
    #[allow(clippy::needless_range_loop)] // индекс сам участвует в вычислении
    for g in 0..ngroups {
        if g == crate::slots::ROOT as usize {
            continue;
        }
        let mut col = Vec::with_capacity(docs);
        for (i, s) in sources.iter().enumerate() {
            let band = s.slots.get(g);
            for d in 0..s.docs as u32 {
                if any_dead && remap[i][d as usize] == u32::MAX {
                    continue;
                }
                col.push(band.map(|b| b.arity(d)).unwrap_or(0));
                if !maintenance_ticker.tick(4) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "compaction остановлен graceful shutdown",
                    ));
                }
            }
        }
        arity[g] = col;
    }
    // Начал отрезков выхода здесь нет намеренно. Значения кладутся подряд, и
    // порядок задаёт обход, а не поиск по таблице; таблица же — четыре байта на
    // каждый элемент каждой группы, то есть ещё одна величина, линейная по
    // размеру сегмента, и ни один читатель её не спрашивал.
    let mut slot_kinds = vec![crate::slots::Kind::Identity; ngroups];
    let mut slot_units = vec![docs; ngroups];
    // Пути групп переносятся, а не выдумываются. Прежде слияние писало в
    // заголовок пустой третий столбец, и путь пропадал при первом же
    // укрупнении — молча, потому что пустой путь у некорневой группы заголовок
    // принимал. Запрос после этого адресовать группу не мог ничем.
    //
    // Расхождение источников — отказ, а не выбор одного из двух: разные пути у
    // одного номера означают, что сегменты собраны по разным схемам, и слитый
    // отвечал бы на запрос по одной группе данными другой.
    let mut slot_paths = vec![String::new(); ngroups];
    for s in sources.iter() {
        for (g, path) in s.group_paths.iter().enumerate().take(ngroups) {
            if path.is_empty() {
                continue;
            }
            if slot_paths[g].is_empty() {
                slot_paths[g] = path.clone();
            } else if slot_paths[g] != *path {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "{out}: у группы {g} источники расходятся в пути: {:?} и {path:?} — \
                         сегменты собраны по разным схемам, и слить их значило бы ответить \
                         по одной группе данными другой",
                        slot_paths[g]
                    ),
                ));
            }
        }
    }
    // Полосы арности считаются здесь — от них зависит заголовок, — а в контейнер
    // ложатся вместе с прочим крупным, ниже: порядок кусков задан не порядком
    // вычисления. Ровно так же поступает приём.
    let mut bands: Vec<(String, Vec<u8>)> = Vec::new();
    for g in 1..ngroups {
        // Колонка отбирается, а не одалживается: после упаковки она не нужна
        // никому, а иначе пролежала бы до конца слияния — то есть сквозь всю
        // фазу содержимого, где и стоит пик. Четыре байта на документ на группу
        // на мелком сегменте незаметны, на пятидесяти миллионах с двумя
        // группами это четыре сотни мегабайт, лежащих рядом с колонкой ровно
        // тогда, когда памяти меньше всего. На выход отбор не влияет:
        // `encode` читает срез и владения не берёт.
        let col = std::mem::take(&mut arity[g]);
        let built = crate::slots::encode(&col, &format!("{out}/g{g}.arity"))?;
        drop(col);
        if built.kind == crate::slots::Kind::Dense {
            bands.push((format!("g{g}.aidx"), built.idx));
            bands.push((format!("g{g}.arity"), built.band));
        }
        slot_kinds[g] = built.kind;
        slot_units[g] = built.units;
    }

    // Нормы склеиваются подряд по полям; заодно считается новая средняя длина.
    let mut norms: Vec<Vec<u8>> = Vec::with_capacity(nfields);
    let mut avgdl = vec![0f64; nfields];
    // Целая сумма длин и наибольший уровень — см. `norms::FIXED`. Складываются
    // они точно и в любом порядке, поэтому слияние обязано отдать ровно ту же
    // сумму, что дали бы источники по отдельности.
    let mut len_fixed = vec![0u64; nfields];
    let mut lmax = vec![0u8; nfields];
    let mut scoring_docs = vec![None; nfields];
    #[allow(clippy::needless_range_loop)] // индекс сам участвует в вычислении
    for f in 0..nfields {
        let mut column = Vec::with_capacity(docs);
        let mut fixed = 0u64;
        let mut hi = 0u8;
        let mut nonempty = 0u64;
        let absent = u8::from(plan.field(sources, f).kind.keyword());
        for (i, s) in sources.iter().enumerate() {
            let j = plan.local[i][f];
            for d in 0..s.docs as u32 {
                if any_dead && remap[i][d as usize] == u32::MAX {
                    continue;
                }
                // Поля в этом источнике нет — длина нулевая, как у числового.
                //
                // Кроме ключевого: у него уровень постоянная единица (см.
                // приём в `indexer`), и ноль на месте отсутствующего поля
                // сломал бы постоянство ровно там, где оно и нужно, — при
                // слиянии сегментов разного возраста. Наблюдаем этот ноль
                // ниоткуда: постингов у такого документа в этом поле нет.
                let q = if j == NONE { absent } else { s.level(j, d) };
                column.push(q);
                nonempty += u64::from(q > 0);
                fixed = fixed.saturating_add(crate::norms::fixed_len(q));
                hi = hi.max(q);
                if !maintenance_ticker.tick(8) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "compaction остановлен graceful shutdown",
                    ));
                }
            }
        }
        let field = plan.field(sources, f);
        if field.kind == crate::index::Kind::Text && field.group == 0 {
            scoring_docs[f] = Some(nonempty);
        }
        avgdl[f] = crate::norms::avgdl_of(fixed, scoring_docs[f].unwrap_or(docs as u64));
        len_fixed[f] = fixed;
        lmax[f] = hi;
        norms.push(column);
    }

    phases.push(("нормы", mark.elapsed().as_secs_f64()));
    prof.cut("нормы");
    mark = std::time::Instant::now();

    // Куски слитого сегмента ложатся одним файлом, и порядок их тот же, что у
    // приёма: сперва потоковое и крупное, метаданные — сплошным прогоном в конце.
    // `Index::warm` греет ровно метаданные, и в контейнере это одно упреждающее
    // чтение подряд вместо восьми поисков головки по разным файлам. Разбор — у
    // `indexer::write_segment`; расходиться этим двум писателям нельзя, иначе
    // сегмент после укрупнения читался бы иначе, чем до него.
    let mut pack = crate::container::Writer::create(out, index::LAYOUT)?;
    pack.begin("postings.bin")?;
    let mut bytes: Vec<u8> = Vec::with_capacity(1 << 22);
    let mut bp: u64 = 0;
    let (mut b_last, mut b_max, mut b_len) = (Vec::new(), Vec::<f32>::new(), Vec::<u16>::new());
    // Слова, позиции и словарь пишутся потоком, а не собираются в памяти: см.
    // [`Sink`] и [`dict::Writer`]. Обратно в них никто не смотрит, а весят они
    // столько же, сколько сегмент.
    //
    // В контейнер прямо по ходу обхода уходят только списки: открытым бывает
    // **один** кусок, а обход кормит четыре потока разом. Остальные три ложатся
    // отдельными файлами и переливаются в контейнер по готовности — см. [`pour`].
    let mut terms = Sink::create(&format!("{out}/terms.bin"))?;
    let mut pos = Sink::create(&format!("{out}/pos.bin"))?;
    let mut dictw = dict::Writer::create(&format!("{out}/dict.blob"))?;
    // Черновики: `layout::encode` и `write_positions` умеют дописывать только в
    // `Vec`. Отдавать их потоку надо **сразу** — длина потока и есть смещение
    // следующей записи, и задержись черновик, смещения считались бы по числу,
    // которое ещё не стало длиной файла.
    let mut tbuf: Vec<u8> = Vec::with_capacity(64);
    let mut pbytes: Vec<u8> = Vec::with_capacity(1 << 16);
    let mut tidx: Vec<u8> = Vec::new();
    let mut pos_idx: Vec<u32> = Vec::new();
    let mut ppend: Vec<u32> = Vec::new();
    let mut pbuf: Vec<u32> = Vec::new();
    // Сколько слов уже легло в словарь выхода. Прежде это была длина списка
    // **всех** ключей, который слияние держало до самого конца; наружу от того
    // списка уходило только это число.
    let mut nterms = 0usize;
    // Ordinal — адрес в конкретном словаре, а не значение keyword. Таблица
    // строится потоком рядом со словарём и не разрастается в resident HashMap.
    let mut ordinal_writers: Vec<Option<crate::ordmap::Writer>> = sources
        .iter()
        .enumerate()
        .map(|(at, source)| {
            if source.fields.iter().any(|field| {
                field.kind.keyword() && (field.column.is_some() || field.multi_column.is_some())
            }) {
                crate::ordmap::Writer::create(
                    out,
                    at,
                    source.dict.len(),
                    crate::ordmap::BUFFER_BUDGET / sources.len().max(1),
                )
                .map(Some)
            } else {
                Ok(None)
            }
        })
        .collect::<std::io::Result<_>>()?;
    let mut ordinal_inputs = Vec::with_capacity(sources.len());
    let mut keyword_ranges: Vec<Option<(u32, u32)>> = vec![None; nfields];
    let mut postings = 0u64;
    // Наибольшая частота среди слов, оставшихся без позиций.
    //
    // Слияние не может позиции **выдумать**: слово, законно оставшееся без них
    // при `df = 3 000` в каждом из восьми сегментов, придёт сюда с `df = 24 000`
    // и позиций всё равно не получит. Поэтому граница здесь наблюдается, а не
    // назначается, и после слияния она честно оказывается хуже. Видно это в
    // `inspect`, и решать, мириться или пересобрать, — тому, кто смотрит.
    let mut posmin_seen = 0u32;

    // Обход словарей: у каждого сегмента свой, все отсортированы одинаково.
    let mut walks: Vec<dict::DictWalk> = sources.iter().map(|s| s.dict.walk()).collect();
    // Ключ обхода — номер поля **выхода**, а не местный: у сегментов разного
    // возраста местная нумерация своя. Местный номер едет рядом, он нужен,
    // чтобы завести курсор в своём сегменте.
    let key_of = |i: usize, f: u32| -> u32 {
        plan.out_of[i].get(f as usize).copied().unwrap_or(NONE) as u32
    };
    // Голова обхода одного источника: ключ порядка, слово, длина списка и
    // номер поля. Имя нужно затем, что кортеж из четырёх читается плохо.
    type Head = Option<(u32, Vec<u8>, u32, usize)>;
    let mut head: Vec<Head> = walks
        .iter_mut()
        .enumerate()
        .map(|(i, w)| w.next().map(|(f, t, l)| (key_of(i, f), t.to_vec(), l, f as usize)))
        .collect();

    // Наименьший ключ среди голов — очередное слово слитого словаря; кончились
    // головы — кончился и словарь.
    #[allow(clippy::while_let_loop)] // `while let` не выражает `let ... else break` с телом
    loop {
        let Some(key) = head
            .iter()
            .filter_map(|h| h.as_ref().map(|(f, t, _, _)| (*f, t.as_slice())))
            .min()
            .map(|(f, t)| (f, t.to_vec()))
        else {
            break;
        };

        // Потолок четырёх гигабайт проверяется **здесь**, до записи слова.
        // Разбор — у [`fits_offset`].
        //
        // `terms.bin` проверяется вместе со списками и позициями, а не у самой
        // опоры: за время обработки слова он не растёт — запись слова уходит в
        // него в самом конце, — поэтому проверка здесь строго раньше записи
        // опоры, и все три потока меряются в одном месте.
        fits_offset(bp, "список", out, nterms + 1)?;
        fits_offset(pos.len() as u64, "позиции", out, nterms + 1)?;
        fits_offset(terms.len() as u64, "слова", out, nterms + 1)?;
        let post_start = bp;
        let pos_start = pos.len();
        let block_start = b_last.len();
        let mut pend: Vec<u32> = Vec::with_capacity(BLOCK);
        let mut fend: Vec<u32> = Vec::with_capacity(BLOCK);
        let mut best = 0f32;
        let (mut qmax, mut tfmax, mut list_best) = (0u8, 1u32, 0f64);
        let mut start = bp;
        let mut prev = 0u32;
        let mut df = 0u32;
        // Список стал длинным: сто двадцать девятый постинг пришёл, и первый
        // блок можно писать. До этого мгновения класс списка неизвестен, а от
        // него зависит и наличие полосы частот, и наличие метаданных блока.
        let mut long = false;
        let mut first_pos: Option<u32> = None;
        let field = key.0 as usize;
        let fl = Some(plan.field(sources, field));
        // Позиции у поля есть тогда, когда они объявлены и есть у каждого
        // источника, **где это поле вообще есть**: у сегмента без поля брать
        // нечего, и требовать от него позиций бессмысленно.
        let field_pos = fl.map(|f| f.positioned).unwrap_or(false)
            && sources
                .iter()
                .enumerate()
                .all(|(i, s)| plan.local[i][field] == NONE || s.has_positions());
        // Порядок слов можно восстановить из содержимого — значит слово без
        // позиций законно, но граница обязана его учесть.
        let verifiable = fl.map(|f| f.stored && f.kind == index::Kind::Text).unwrap_or(false);
        // Позиции у слитого слова есть тогда, когда они есть у **каждого**
        // исходного: взять недостающие неоткуда, а склеить половину значило бы
        // отдать фразе часть вхождений и промолчать про остальные.
        let has_pos = field_pos
            && sources.iter().enumerate().all(|(i, s)| match &head[i] {
                Some((f, t, list, local)) if (*f, t.as_slice()) == (key.0, key.1.as_slice()) => {
                    s.term(*list as usize, *local).positioned
                }
                _ => true,
            });
        ppend.clear();
        ordinal_inputs.clear();

        for (i, s) in sources.iter().enumerate() {
            let matches = matches!(&head[i], Some((f, t, _, _)) if (*f, t.as_slice()) == (key.0, key.1.as_slice()));
            if !matches {
                continue;
            }
            let (list, local) = {
                let h = head[i].as_ref().expect("голова есть");
                (h.2, h.3)
            };
            if ordinal_writers[i].is_some() {
                ordinal_inputs.push((i, list));
            }
            // Номера этого сегмента идут подряд и все больше предыдущих, потому
            // что сегменты склеиваются по порядку.
            let mut c = s.cursor(list, local, 1.0);
            loop {
                let d = c.cur();
                if d == u32::MAX {
                    break;
                }
                if !maintenance_ticker.tick(32) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "compaction остановлен graceful shutdown",
                    ));
                }
                let doc = if any_dead {
                    let n = remap[i][d as usize];
                    if n == u32::MAX {
                        c.next();
                        continue;
                    }
                    n
                } else {
                    d + base[i]
                };
                // Сброс полного блока откладывается до прихода следующего
                // постинга: пока он не пришёл, список мог кончиться ровно на
                // границе блока и остаться коротким.
                if pend.len() == BLOCK {
                    long = true;
                    crate::indexer::write_run(&mut bytes, &mut bp, &pend, &fend, true);
                    b_len.push((bp - start) as u16);
                    b_last.push(prev);
                    b_max.push(best);
                    pos_idx.push(pos.len() as u32);
                    crate::indexer::write_positions(&mut pbytes, &ppend, has_pos, df, false);
                    pos.take(&mut pbytes)?;
                    pend.clear();
                    fend.clear();
                    best = 0.0;
                }
                if pend.is_empty() {
                    start = bp;
                    ppend.clear();
                }
                pend.push(doc - prev);
                prev = doc;
                df += 1;
                let tf = c.tf();
                fend.push(tf - 1);
                // Позиции переносятся как есть: они относятся к полю документа,
                // а перенумерация документов их не касается.
                if has_pos {
                    c.positions(&mut pbuf);
                    let mut last = 0u32;
                    for (k, &v) in pbuf.iter().enumerate() {
                        if df == 1 && k == 0 {
                            first_pos = Some(v);
                        }
                        ppend.push(if k == 0 { v } else { v.saturating_sub(last) });
                        last = v;
                    }
                    // Частота обещает столько позиций; если источник отдал
                    // меньше — добиваем повтором последней, иначе чтение поехало
                    // бы на всех следующих документах блока.
                    ppend.extend(std::iter::repeat_n(0, (tf as usize).saturating_sub(pbuf.len())));
                }
                let q = norms[field][doc as usize];
                // Разреженный вектор: вклад — сам вес, см. `sparse`. Слияние
                // копирует коды как есть, поэтому и потолок восстанавливается
                // той же таблицей, что при сборке.
                let v64 = if fl.map(|x| x.kind.dot()).unwrap_or(false) {
                    f64::from(crate::sparse::decode(tf.min(255) as u8))
                } else {
                    crate::norms::sat(tf, crate::norms::dl_part(q, avgdl[field]))
                };
                // Вверх, а не к ближайшему: округление вниз перестаёт быть
                // потолком, и отсечение вправе выбросить документ молча.
                let v = crate::norms::up_f32(v64);
                if v > best {
                    best = v;
                }
                if v64 > list_best {
                    list_best = v64;
                    qmax = q;
                    tfmax = tf;
                }
                if bytes.len() > (1 << 22) {
                    pack.append(&bytes)?;
                    bytes.clear();
                }
                c.next();
            }
            head[i] = walks[i].next().map(|(f, t, l)| (key_of(i, f), t.to_vec(), l, f as usize));
        }

        let value_word = plan.field(sources, field).kind.keyword()
            && key.1 != crate::indexer::presence_term(plan.field(sources, field).kind).as_bytes();
        let new_ordinal = if df > 0 && value_word {
            let ordinal = u32::try_from(nterms)
                .map_err(|_| std::io::Error::other("словарь keyword превышает u32"))?;
            let end = ordinal
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("словарь keyword исчерпал ordinal"))?;
            keyword_ranges[field] =
                Some((keyword_ranges[field].map_or(ordinal, |range| range.0), end));
            Some(ordinal)
        } else {
            None
        };
        for &(source, old) in &ordinal_inputs {
            ordinal_writers[source].as_mut().expect("ordinal writer").push(old, new_ordinal)?;
        }

        // Класс списка известен только здесь: длинным он становится не от того,
        // сколько блоков начато, а от того, пришёл ли постинг за границу блока.
        let tf_all_one = !long && fend.iter().all(|&f| f == 0);
        if !pend.is_empty() {
            if df > 1 {
                crate::indexer::write_run(&mut bytes, &mut bp, &pend, &fend, !tf_all_one);
            }
            if long {
                b_len.push((bp - start) as u16);
                b_last.push(prev);
                b_max.push(best);
                pos_idx.push(pos.len() as u32);
            }
            crate::indexer::write_positions(&mut pbytes, &ppend, has_pos, df, tf_all_one);
            pos.take(&mut pbytes)?;
        }
        // Слово, у которого не осталось ни одного живого документа, в словарь не
        // попадает: пустая запись означала бы «список есть, но он пуст», а такое
        // неотличимо от порчи.
        if df > 0 {
            // Опора ставится **здесь**, а не в начале обхода слова: слово, у
            // которого не осталось живых документов, в словарь не попадает, и
            // опора, поставленная ему авансом, сдвинула бы всю группу.
            if nterms.is_multiple_of(crate::layout::ANCHOR) {
                tidx.extend((terms.len() as u32).to_le_bytes());
                tidx.extend((post_start as u32).to_le_bytes());
                tidx.extend((pos_start as u32).to_le_bytes());
                tidx.extend((block_start as u32).to_le_bytes());
            }
            let inline = if df == 1 && tf_all_one && has_pos { first_pos } else { None };
            if !has_pos && verifiable {
                posmin_seen = posmin_seen.max(df);
            }
            crate::layout::encode(
                &mut tbuf,
                &crate::layout::Rec {
                    df,
                    tf_all_one,
                    // Значок «позиции есть» обязан стоять и там, где кусок
                    // позиций пуст, потому что вхождение уехало внутрь записи.
                    // Иначе разбор не прочтёт лежащее следом число и съедет на
                    // всех последующих словах группы.
                    positioned: has_pos && (pos.len() > pos_start || inline.is_some()),
                    doc: if df == 1 { prev } else { 0 },
                    tf: if df == 1 { fend.first().copied().unwrap_or(0) + 1 } else { 0 },
                    inline_pos: inline,
                    qmax,
                    tfmax,
                    post_len: (bp - post_start) as u32,
                    pos_len: (pos.len() - pos_start) as u32,
                },
            );
            terms.take(&mut tbuf)?;
            // Слово уходит в словарь ровно здесь, а не списком в конце: опора
            // словаря считается по тому же счётчику, что и опора `terms.idx`
            // выше, и разъехаться им нельзя.
            dictw.push(key.0, &key.1)?;
            postings += df as u64;
            nterms += 1;
        }
    }
    pack.append(&bytes)?;
    pack.end()?;
    let ordinal_maps: Vec<Option<crate::ordmap::Reader>> = ordinal_writers
        .into_iter()
        .map(|writer| writer.map(crate::ordmap::Writer::finish).transpose())
        .collect::<std::io::Result<_>>()?;

    // Метаданные блоков дожидаются своего места в порядке кусков — прогона в
    // самом конце. Памяти это не стоит ни байта сверх прежнего: и `blocks`, и
    // опоры, и полоса уровней жили до конца слияния и раньше, просто писались
    // по мере готовности.
    let mut blocks: Vec<u8> = Vec::with_capacity(b_last.len() * index::BLOCK_META);
    for i in 0..b_last.len() {
        blocks.extend(b_last[i].to_le_bytes());
        blocks.extend(b_max[i].to_bits().to_le_bytes());
        blocks.extend(b_len[i].to_le_bytes());
    }
    // Пустого куска позиций быть не должно — приём его тоже не пишет. Пустой
    // кусок в контейнере законен и означает «поле без значений», а здесь нечего
    // и обещать: слов с позициями не нашлось ни одного.
    let poslen = pos.len();
    pos.finish()?;
    if poslen > 0 {
        pour(&mut pack, out, "pos.bin")?;
    } else {
        std::fs::remove_file(format!("{out}/pos.bin")).ok();
    }
    if !pos_idx.is_empty() {
        let mut pidx = Vec::with_capacity(pos_idx.len() * 4);
        for o in &pos_idx {
            pidx.extend(o.to_le_bytes());
        }
        pack.put("pos.idx", &pidx)?;
    }
    // Начала позиций своё дело сделали. Их четыре байта на список с позициями —
    // не на документ, но на словаре в миллионы списков это десятки мегабайт,
    // которые иначе дожили бы до конца.
    drop(pos_idx);

    // Слова и словарь дожидаются прогона метаданных: они лежат отдельными
    // файлами и переливаются в контейнер там же, где их кладёт приём.
    terms.finish()?;
    let anchors = dictw.finish()?;

    // Размер фильтра зависит от числа **слитых** слов. Брать сумму слов
    // источников нельзя: у восьми сегментов с одним словарём файл вырос бы
    // восьмикратно и навсегда. Поэтому готовый потоковый словарь проходится
    // ещё раз из page cache после того, как точное `nterms` уже известно.
    // Временная опора нужна `Dict` только на этот проход и в контейнер не
    // публикуется отдельно.
    let dict_index = format!("{out}/dict.idx");
    std::fs::write(&dict_index, &anchors).map_err(|e| {
        std::io::Error::new(e.kind(), format!("{dict_index}: не записана временная опора: {e}"))
    })?;
    let source = crate::container::loose(out);
    let dictionary = dict::Dict::open(&source, nterms, index::LAYOUT)?;
    let mut term_filter = crate::bloom::Builder::new(nterms, crate::bloom::DEFAULT_BITS);
    let mut dictionary_walk = dictionary.walk();
    while let Some((_field, term, _list)) = dictionary_walk.next() {
        term_filter.add_hash(crate::bloom::hash64(term));
    }
    drop(dictionary_walk);
    drop(dictionary);
    let term_filter = term_filter.finish();

    // Полоса — на различное содержимое, а не на поле; у слитого сегмента
    // постоянные уровни те же, что у исходных. Разбор — в
    // [`crate::norms::bands`].
    let (nb, norms_band) = crate::norms::bands(&norms);
    // Нормы дальше не нужны никому: в списки они уже вошли, в полосы — тоже.
    // Байт на поле на документ держится до самого конца слияния только по
    // недосмотру, а следом идут колонки, и пик приходится как раз на них.
    drop(norms);

    phases.push(("списки", mark.elapsed().as_secs_f64()));
    prof.cut("списки");
    mark = std::time::Instant::now();

    // Идентификаторы склеиваются проходом, а не выборкой по номеру, и уходят в
    // файл **тем же проходом**.
    //
    // ## Что здесь было
    //
    // Все идентификаторы сегмента отдельными `String`, потом весь `ids.bin`
    // одним куском, потом фильтр по тому же списку. Три величины, линейные по
    // числу документов, и самая крупная из них — список: заголовок `String` и
    // отдельное выделение под каждую строку стоят больше, чем сами байты. На
    // шестидесяти четырёх миллионах документов это гигабайты, притом что
    // кодировщику дальше одного предыдущего идентификатора смотреть не на что,
    // а фильтру — вообще ни на что.
    //
    // ## Про байты вместо строк
    //
    // Прежде каждый идентификатор проходил через `String::from_utf8_lossy`.
    // На всём, что способен принять приём, это тождество: `_id` приходит
    // строкой. Разница видна только на порченом `ids.bin`, где обход отдаст
    // обрезанную многобайтную последовательность, — и там байты честнее:
    // прежний путь молча подменял их на «замену» и записывал в слитый сегмент
    // идентификатор, которого не было ни в одном источнике. Обратное
    // отображение (`IdMap::rebuilt`) хеширует ровно эти байты, так что фильтр
    // теперь считается тем же, чем потом проверяется.
    let mut idw = ids::Writer::create(&format!("{out}/ids.bin"))?;
    // На диск идёт **только фильтр**. Само отображение собирается по
    // требованию из `ids.bin` — см. `IdMap::rebuilt`.
    let mut mbloom = crate::bloom::Builder::new(docs, crate::bloom::DEFAULT_BITS);
    let mut written = 0usize;
    'ids: for (i, s) in sources.iter().enumerate() {
        let store = ids::Ids::open(&s.src, s.docs);
        let mut w = store.walk();
        let mut at = 0usize;
        while let Some(id) = w.next() {
            if !any_dead || remap[i][at] != u32::MAX {
                // Обрезка по числу документов — вторая половина того, что делал
                // прежний `resize`. Сработать ей не на чем: проход отдаёт ровно
                // `s.docs` записей, столько же их и в `docs`. Стоит она здесь
                // затем, чтобы длина `ids.bin` не могла разойтись с заголовком
                // молча, — на этом стоит и выдача `_id`, и гашение по нему.
                if written == docs {
                    break 'ids;
                }
                idw.push(id)?;
                mbloom.add(id);
                written += 1;
            }
            at += 1;
        }
    }
    // Недостача добивается пустыми — ровно то, что делал прежний `resize`.
    // Случается она только у источника без `ids.bin`: у такого проход отдаёт
    // пустые строки сам, так что и здесь речь про сегмент, у которого их нет
    // вовсе.
    while written < docs {
        idw.push(b"")?;
        mbloom.add(b"");
        written += 1;
    }
    let idanchors = idw.finish()?;
    pour(&mut pack, out, "ids.bin")?;
    pack.put("ids.idx", &idanchors)?;
    pack.put("idmap.bloom", &mbloom.finish())?;
    for (name, band) in bands.drain(..) {
        pack.put(&name, &band)?;
    }

    phases.push(("идентификаторы", mark.elapsed().as_secs_f64()));
    prof.cut("идентификаторы");
    mark = std::time::Instant::now();

    // Плотные векторы: перекодировка байта в байт на общей шкале. Коды идут в
    // файл потоком — держать их в памяти целиком нельзя, см. `vector::merge`.
    for f in 0..nfields {
        let Some(dims) = plan.field(sources, f).kind.dims() else {
            continue;
        };
        let parts: Vec<(Option<&crate::vector::Vectors>, usize)> = sources
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let j = plan.local[i][f];
                (if j == NONE { None } else { s.fields[j].vectors.as_ref() }, s.docs)
            })
            .collect();
        let name = &plan.field(sources, f).stem;
        // Коды текут в отдельный файл, а оттуда — куском контейнера. Прямо в
        // контейнер их не пустить: пока `vector::merge` не вернулся, неизвестно,
        // нашёлся ли хоть один вектор, а начатый кусок обратно не отзывается —
        // в оглавлении остался бы пустой `.q8` без пары, то есть ровно тот
        // мусор, который здесь и убирают.
        let path = format!("{out}/{name}.q8");
        let mut sink = std::io::BufWriter::new(File::create(&path)?);
        let meta = crate::vector::merge(
            &parts,
            dims,
            &|i, d| !any_dead || remap[i][d as usize] != u32::MAX,
            &mut sink,
        )?;
        sink.flush()?;
        sink.into_inner().map_err(|e| e.into_error())?;
        match meta {
            Some(m) => {
                pour(&mut pack, out, &format!("{name}.q8"))?;
                pack.put(&format!("{name}.qm"), &m)?;
            }
            // Ни одного вектора среди исходных — кусков быть не должно, и
            // пустого `.q8` тоже: `Vectors::open` сверяет длины с заголовком, и
            // кусок без пары его не пройдёт, но оставлять мусор незачем.
            None => {
                std::fs::remove_file(&path).ok();
            }
        }
    }

    phases.push(("векторы", mark.elapsed().as_secs_f64()));
    prof.cut("векторы");
    mark = std::time::Instant::now();

    // Заголовок — последним: он и есть признак готовности сегмента.
    //
    // Хранимое по умолчанию **переносится**, а не пересобирается. Чистый кусок
    // уже сжат и от слияния не меняется: номера документов в его теле не лежат
    // вовсе — тело это только «длина, байты» подряд, — а номер первого живёт в
    // засечке. Значит перенумерация это переписать засечку, а тело скопировать.
    // Словарь источника переезжает в `.sdic` дословно и получает свой отрезок в
    // таблице `.sdix`.
    //
    // Кусок, из которого выпали погашенные, пересжимается **тем же** словарём
    // (`EncoderDict::adopt`). Собери его свежим — и он вывалился бы из отрезка
    // своего словаря, а на непрерывности отрезков стоит поиск словаря по номеру
    // куска.
    //
    // Пересборка со свежим словарём осталась запасным путём. Она включается
    // сама, когда грязных кусков больше половины (переносить и пересжимать три
    // четверти дороже, чем собрать заново, и заново возвращает место мёртвых)
    // или когда источник не прошёл сторож против россыпи мелких словарей. Её же
    // включает `SVINDB_STORE_REBUILD` — и это не отладочная лазейка, а
    // дифференциальный оракул: побайтовая сверка «слитый равен собранному с
    // нуля» здесь невозможна по построению, и старый путь сохранён затем, чтобы
    // было с чем сверять. Разбор — `Effort::Fast`, тот же, что на приёме.
    //
    // Замерено на четырёх источниках по 12 000 документов, колонка 155 МБ:
    // фаза «содержимое» 17.047 с против 0.111 с, сжатых кусков 15 687 против 35.
    let mut packed = 0usize;
    for f in 0..nfields {
        // Хватает **одного** сегмента с хранилищем, чтобы собрать его в слитом.
        let out_field = plan.field(sources, f);
        let some_store = sources.iter().enumerate().any(|(i, s)| {
            let j = plan.local[i][f];
            j != NONE && s.fields[j].store.is_some()
        });
        if !out_field.stored {
            continue;
        }
        let name = &out_field.stem;

        // Файлы хранилища на месте, а само оно не открылось. Так выглядит гнутая
        // таблица словарей `.sdix` или порченая запись словаря: `Store::open`
        // отвергает их именно затем, чтобы куски не читались чужим словарём.
        //
        // Пройти мимо значило бы **стереть поле**: и перенос, и пересборка
        // положили бы на место значений пустоту — источник отдаёт `None` на
        // каждый документ — и отчитались бы успехом. Исходные сегменты после
        // публикации сносит уборка, и восстанавливать было бы не из чего.
        for (i, s) in sources.iter().enumerate() {
            let j = plan.local[i][f];
            if j == NONE || s.fields[j].store.is_some() {
                continue;
            }
            let stem = &s.fields[j].stem;
            if ["sbin", "smrk", "sdic"].iter().all(|e| s.src.has(&format!("{stem}.{e}"))) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "{}: хранилище поля {} не открывается, а файлы на месте — \
                         слить его значило бы стереть содержимое поля молча",
                        s.dir, out_field.name
                    ),
                ));
            }
        }

        // Хватает одного источника с хранилищем, чтобы собрать его в слитом; не
        // нашлось ни одного — собирать нечего. Сверка выше идёт **до** этого:
        // «хранилища нет» и «хранилище не открылось» — разные вещи.
        if !some_store {
            continue;
        }

        // Перенос: куски без погашенных ложатся байт в байт, и словарь
        // источника переезжает вместе с ними. Решение принимается на поле и
        // целиком — см. [`plan_carry`].
        if !rebuild_wanted() {
            if let Some(carry) = plan_carry(sources, &plan, f, &remap, any_dead) {
                let made =
                    carry_store(sources, &plan, f, &remap, any_dead, &carry, out, name, docs)?;
                packed += made.packed;
                pour_store(&mut pack, out, name, &made)?;
                continue;
            }
        }

        let remap = &remap;
        // Проход по хранилищу источника мог оборваться на порче. Наружу из
        // толкающего обхода отказ вынести нечем — приёмник обязан вернуть `()`,
        // — поэтому он запоминается и превращается в отказ ниже. Молча дописать
        // на месте нечитаемого пустые значения значило бы потерять половину поля,
        // отчитавшись об успехе.
        let broke = std::cell::Cell::new(false);
        let stopped = std::cell::Cell::new(false);
        // `build_with` принимает `Fn` и вызывает источник несколько раз, поэтому
        // обычный `&mut CompactionTicker` сюда захватить нельзя. Состояние
        // принадлежит одному producer; compression workers его не разделяют.
        let maintenance_ticks = std::cell::RefCell::new(crate::work::CompactionTicker::default());
        let maintenance_tick = |bytes| {
            if !maintenance_ticks.borrow_mut().tick(bytes) {
                stopped.set(true);
            }
        };
        let values = |emit: &mut dyn FnMut(&[u8])| {
            for (i, s) in sources.iter().enumerate() {
                if stopped.get() {
                    return;
                }
                let j = plan.local[i][f];
                // Поля в этом источнике нет вовсе — все его документы отдают
                // пустое значение. Пропустить их нельзя: номера обязаны идти
                // подряд, иначе значения съедут на чужие документы.
                if j == NONE {
                    for d in 0..s.docs as u32 {
                        if stopped.get() {
                            return;
                        }
                        if !(any_dead && remap[i][d as usize] == u32::MAX) {
                            emit(&[]);
                        }
                        maintenance_tick(0);
                    }
                    continue;
                }
                let Some(st) = s.fields[j].store.as_ref() else {
                    // Хранилища у поля нет — но значение может быть. У числового
                    // поля оно лежит в колонке, и `fetch` печатает его обратно;
                    // пустая строка тогда, когда значения и правда нет.
                    //
                    // Без этой ветки слияние сегмента с копией и сегмента без
                    // неё стирало значения второго: он отдавал пустоту на все
                    // свои документы. Поймано тестом на смешанном слиянии.
                    for d in 0..s.docs as u32 {
                        if stopped.get() {
                            return;
                        }
                        if !(any_dead && remap[i][d as usize] == u32::MAX) {
                            let value = s.fetch(d, j).unwrap_or_default();
                            maintenance_tick(value.len() as u64);
                            emit(&value);
                        }
                        maintenance_tick(0);
                    }
                    continue;
                };
                let mut w = st.walk();
                let mut want = 0u32;
                while let Some((doc, val)) = w.next() {
                    if stopped.get() {
                        return;
                    }
                    // Пробел в нумерации — пустые значения: хранилище могло не
                    // покрыть часть документов, и номера обязаны совпасть.
                    while want < doc {
                        if !(any_dead && remap[i][want as usize] == u32::MAX) {
                            emit(&[]);
                        }
                        want += 1;
                        maintenance_tick(0);
                    }
                    maintenance_tick(val.len() as u64);
                    if !(any_dead && remap[i][doc as usize] == u32::MAX) {
                        emit(val);
                    }
                    want = doc + 1;
                    maintenance_tick(0);
                }
                if w.broken() {
                    broke.set(true);
                }
                for d in want..s.docs as u32 {
                    if !(any_dead && remap[i][d as usize] == u32::MAX) {
                        emit(&[]);
                    }
                    maintenance_tick(0);
                }
            }
        };
        // Тот же уровень сжатия, что и на приёме.
        //
        // Один уровень на оба писателя заодно снимает вопрос, почему сегмент,
        // собранный приёмом, и он же после укрупнения сжаты по-разному.
        let sbin = format!("{out}/{name}.sbin");
        // Ширина потоков — не украшение: пересборка содержимого занимает все
        // ядра, а слияние идёт фоном. Разбор — у [`Limits::threads`]. Вывод от
        // числа потоков не зависит побайтово, это проверено у `build_with`.
        let threads = match lim.threads {
            0 => std::thread::available_parallelism().map_or(1, |n| n.get()),
            n => n,
        };
        let built =
            match crate::store::build_with(values, crate::codec::Effort::Fast, threads, &sbin) {
                Ok(built) => built,
                Err(error) if stopped.get() => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        format!("compaction остановлен graceful shutdown: {error}"),
                    ))
                }
                Err(error) => return Err(error),
            };
        if !maintenance_ticks.borrow_mut().flush() {
            stopped.set(true);
        }
        if stopped.get() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                format!("{out}/{name}: compaction остановлен graceful shutdown"),
            ));
        }
        if broke.get() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{out}/{name}: проход по хранилищу поля {} в источнике оборвался на порче — \
                     слить его значило бы дописать на месте нечитаемого пустые значения",
                    out_field.name
                ),
            ));
        }
        if let Some(built) = built {
            packed += built.marks.len() / crate::store::MARK;
            // Пересборка отдаёт куски буферами — их и кладём как есть. Потоком
            // здесь ничего не выигрывается: буферы уже собраны, и заводить ради
            // них временный файл значило бы записать содержимое дважды.
            //
            // Таблицы словарей пересборка не заводит вовсе: словарь у неё один.
            // Прежде здесь сносили `.sdix`, оставшийся от предыдущего слияния в
            // том же каталоге; в контейнер, который собирается с нуля, ничему
            // прежнему попасть неоткуда.
            pack.put(&format!("{name}.sbin"), &built.data)?;
            pack.put(&format!("{name}.smrk"), &built.marks)?;
            pack.put(&format!("{name}.sdic"), &built.dict)?;
            pack.put(&format!("{name}.scrc"), &built.crcs)?;
        }
    }

    phases.push(("содержимое", mark.elapsed().as_secs_f64()));
    prof.cut("содержимое");
    mark = std::time::Instant::now();

    // Метаданные — сплошным прогоном и последними из кусков, ровно как у приёма.
    // `Index::warm` греет именно их: метаданные блоков, слова с опорами, словарь
    // с опорами, полосы уровней и колонки. Лежи они вразбивку между гигабайтами
    // списков и содержимого — прогрев превратился бы в восемь поисков головки.
    pack.put("blocks.bin", &blocks)?;
    pour(&mut pack, out, "terms.bin")?;
    pack.put("terms.idx", &tidx)?;
    pour(&mut pack, out, "dict.blob")?;
    pack.put("dict.idx", &anchors)?;
    pack.put("terms.bloom", &term_filter)?;
    std::fs::remove_file(&dict_index).map_err(|e| {
        std::io::Error::new(e.kind(), format!("{dict_index}: временная опора не удалена: {e}"))
    })?;
    pack.put("norms.bin", &nb)?;

    // Колонки значений пересобираются тем же обходом, что и нормы: документы
    // исходных сегментов по порядку, погашенные пропускаются. Границы блоков и
    // признак разброса считаются заново — после выброса мёртвых они другие.
    let mut spreads: Vec<f64> = vec![1.0; nfields];
    // Раскладка выбирается **заново**: после выброса мёртвых плотность другая,
    // и поле, бывшее заполненным целиком, может им перестать быть. Тот же
    // довод, по которому заново считается признак разброса.
    let mut layouts: Vec<crate::column::Layout> = vec![crate::column::Layout::None; nfields];
    let mut presences: Vec<crate::column::Presence> = vec![crate::column::Presence::Bits; nfields];
    let mut values_of: Vec<usize> = vec![0; nfields];
    // Границы числового поля в слитом сегменте — см. `indexer::write_segment`.
    // Считаются заново по слитым значениям, а не берутся объединением границ
    // источников: у погашенных документов значения выпали, и старые границы
    // были бы шире настоящих. Шире — значит сегмент читался бы там, где мог не
    // читаться; не ошибка, но и не то, ради чего границы заводятся.
    let mut bounds: Vec<Option<(i64, i64)>> = vec![None; nfields];
    let mut multi_counts = vec![(0usize, 0usize); nfields];
    for f in 0..nfields {
        let k = plan.field(sources, f).kind;
        if !k.numeric() && !k.keyword() {
            continue;
        }
        let g = plan.field(sources, f).group as usize;
        let units = slot_units[g];
        let mut vals: Vec<i64> = Vec::with_capacity(units);
        let mut has: Vec<bool> = Vec::with_capacity(units);
        let mut multi = crate::multivalue::Builder::default();
        for (i, s) in sources.iter().enumerate() {
            let j = plan.local[i][f];
            let col = if j == NONE { None } else { s.fields[j].column.as_ref() };
            let band = s.slots.get(g);
            let translate = |value| -> std::io::Result<i64> {
                if !k.keyword() {
                    return Ok(value);
                }
                ordinal_maps[i]
                    .as_ref()
                    .ok_or_else(|| std::io::Error::other("нет ordinal map для keyword source"))?
                    .get(value, keyword_ranges[f])
            };
            for d in 0..s.docs as u32 {
                if any_dead && remap[i][d as usize] == u32::MAX {
                    continue;
                }
                // Корневое поле — один ординал на документ; поле внутри массива
                // — столько, сколько у документа элементов. Значения ложатся
                // подряд, а начала отрезков уже посчитаны выше.
                let (from, n) = match band {
                    Some(b) if g != crate::slots::ROOT as usize => b.range(d),
                    _ => (d, 1),
                };
                for slot in 0..n {
                    if let Some(values) = (j != NONE)
                        .then(|| &s.fields[j])
                        .and_then(|field| field.multi_column.as_ref())
                        .and_then(|column| column.get(from + slot))
                    {
                        let values: Vec<i64> =
                            values.map(&translate).collect::<std::io::Result<_>>()?;
                        multi.add(vals.len(), &values);
                        if !maintenance_ticker.tick((values.len() as u64).saturating_mul(8)) {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Interrupted,
                                "compaction остановлен graceful shutdown",
                            ));
                        }
                    }
                    match col.and_then(|c| c.get(from + slot)) {
                        Some(v) => {
                            vals.push(translate(v)?);
                            has.push(true);
                        }
                        None => {
                            vals.push(0);
                            has.push(false);
                        }
                    }
                    if !maintenance_ticker.tick(8) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "compaction остановлен graceful shutdown",
                        ));
                    }
                }
            }
        }
        let name = &plan.field(sources, f).stem;
        if let Some(bytes) = multi.encode(units)? {
            pack.put(&format!("{name}.nmv"), &bytes)?;
            multi_counts[f] = (multi.entries(), multi.values());
            bounds[f] = multi.bounds();
        }
        let values = has.iter().filter(|&&y| y).count();
        if values == 0 {
            layouts[f] = crate::column::Layout::None;
            continue;
        }
        let present: Vec<(u32, i64)> =
            has.iter().enumerate().filter(|(_, y)| **y).map(|(u, _)| (u as u32, vals[u])).collect();
        values_of[f] = values;
        if k.numeric() {
            let lo = present.iter().map(|(_, v)| *v).min().expect("значения есть");
            let hi = present.iter().map(|(_, v)| *v).max().expect("значения есть");
            bounds[f] = Some(bounds[f].map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))));
        }
        layouts[f] = crate::column::choose(vals.len(), &present);
        if layouts[f] == crate::column::Layout::Sparse {
            let (sdata, smeta) = crate::column::encode_sparse(&present);
            pack.put(&format!("{name}.scol"), &sdata)?;
            pack.put(&format!("{name}.smeta"), &smeta)?;
            spreads[f] = crate::column::spread_of(smeta.chunks(crate::column::SMETA).map(|c| {
                let lo = i64::from_le_bytes(c[0..8].try_into().expect("восемь байт"));
                let hi = i64::from_le_bytes(c[8..16].try_into().expect("восемь байт"));
                (lo, hi)
            }));
            continue;
        }
        if values == vals.len() {
            presences[f] = crate::column::Presence::All;
        }
        let (data, meta) = crate::column::encode(&vals, &has);
        spreads[f] = crate::column::spread_of(meta.chunks(crate::column::META).map(|c| {
            let lo = i64::from_le_bytes(c[0..8].try_into().expect("восемь байт"));
            let hi = i64::from_le_bytes(c[8..16].try_into().expect("восемь байт"));
            (lo, hi)
        }));
        pack.put(&format!("{name}.col"), &data)?;
        pack.put(&format!("{name}.cmeta"), &meta)?;
        if spreads[f] >= crate::column::SPREAD_USELESS {
            let (bkt, bidx) = crate::column::encode_buckets(&vals, &has);
            pack.put(&format!("{name}.bkt"), &bkt)?;
            pack.put(&format!("{name}.bktidx"), &bidx)?;
        }
        if presences[f] == crate::column::Presence::Bits {
            pack.put(&format!("{name}.has"), &crate::column::encode_presence(&has))?;
        }
    }

    drop(ordinal_maps);
    phases.push(("колонки", mark.elapsed().as_secs_f64()));
    prof.cut("колонки");
    mark = std::time::Instant::now();

    // Контейнер закрыт и **на носителе** — `finish` доводит его до диска сам.
    // Порядок дальше нерушим: сначала содержимое, потом карта живых, потом
    // заголовок, и только потом каталог. Заголовок вправе обещать лишь тот
    // контейнер, который уже лёг; иначе после потери питания остался бы слитый
    // сегмент, объявленный готовым и наполовину пустой, — а исходные к тому
    // времени уже снесены уборкой.
    let contained = pack.finish()?;

    let mut head_text = format!(
        "container\t{}\t{contained}\ndocs\t{}\nblocks\t{}\nterms\t{}\nlayout\t{}\nseq\t{}\nposblock\t{}\nposmin\t{}\n",
        crate::container::FILE,
        docs,
        b_last.len(),
        nterms,
        index::LAYOUT,
        // Слитый сегмент содержит всё, что содержали исходные, значит и номер
        // берётся наибольший: меньший означал бы, что часть надо переиграть.
        sources.iter().map(|s| s.seq).max().unwrap_or(0),
        crate::index::POSBLOCK,
        posmin_seen
    );
    // Средняя длина пишется **без потерь**, а не `{:.4}`.
    //
    // Задеты только многоблочные списки: у списка короче блока потолок не
    // хранится, а восстанавливается из пары «уровень, частота» той же таблицей
    // читателя, и потому самосогласован при любом `avgdl`.
    // Группы слитого сегмента. Корневая есть всегда; у прочих переносится и
    // путь массива — без него запрос не может назвать группу.
    for g in 0..ngroups {
        head_text.push_str(&format!(
            "group\t{g}\t{}\t{}\t{}\n",
            slot_paths[g],
            slot_kinds[g].name(),
            slot_units[g]
        ));
    }
    for i in 0..nfields {
        let f = plan.field(sources, i);
        // Устойчивый номер и основа имён переносятся **как есть**: слияние не
        // вправе их переназначить. Придумай оно свои — и слитый сегмент
        // перестал бы отвечать на те же номера, что несегментированный.
        head_text.push_str(&format!(
            "field\t{}\t{}\t{}\t{}\t{:.3e}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            f.name,
            f.boost,
            avgdl[i],
            f.kind.spec(),
            spreads[i],
            u8::from(f.stored),
            u8::from(f.positioned),
            norms_band.get(i).copied().unwrap_or(i as u32),
            f.fid,
            f.stem,
            layouts[i].name(),
            presences[i].name(),
            f.group,
            values_of.get(i).copied().unwrap_or(0),
            // Переносится как есть: слияние не вправе решать, что искать.
            u8::from(f.searched),
            bounds[i].map_or(String::new(), |(lo, _)| lo.to_string()),
            bounds[i].map_or(String::new(), |(_, hi)| hi.to_string()),
            len_fixed[i],
            lmax[i], multi_counts[i].0, multi_counts[i].1,
            scoring_docs[i].map_or(String::new(), |count| count.to_string())
        ));
    }
    write(out, crate::live::FILE, &crate::live::all_live(docs))?;
    phases.push(("содержимое", mark.elapsed().as_secs_f64()));
    prof.cut("содержимое");
    mark = std::time::Instant::now();

    write(out, "head.tsv", head_text.as_bytes())?;
    // Сводка — после заголовка: признаком готовности остаётся он, а сводка на
    // него не влияет и потому может накрыть его самого.
    crate::sums::write(out)?;
    // Каталог — вслед за заголовком: имя заголовка обязано пережить потерю
    // питания, иначе готовый сегмент выглядит незаконченным.
    sync_dir(out)?;

    let bytes_total: u64 = std::fs::read_dir(out)?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();

    if !maintenance_ticker.flush() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "compaction остановлен graceful shutdown",
        ));
    }

    phases.push(("заголовок и суммы", mark.elapsed().as_secs_f64()));
    prof.cut("заголовок и суммы");
    Ok(Merged {
        docs,
        terms: nterms,
        postings,
        bytes: bytes_total,
        // Таблица отдаётся вызывающему, а не выбрасывается: по ней публикация
        // гасит в выходе то, что умерло в источниках за время слияния.
        remap,
        base,
        phases,
        peaks: prof.finish(),
        packed,
    })
}

/// Что делать с очередным куском хранимого при переносе.
///
/// Порядок действий задаёт нумерацию документов выхода, поэтому список строится
/// целиком **до** первого записанного байта: решение «перенос или пересборка»
/// принимается на поле и точно, а не выясняется по ходу.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    /// Открыть отрезок: источник и номер словаря в нём.
    Open(usize, usize),
    /// Перенести кусок байт в байт.
    Carry(usize, usize),
    /// Пересжать кусок **тем же** словарём, выбросив погашенные.
    Recode(usize, usize),
    /// Распаковать куски `[от, до)` и дописать значения текущим словарём.
    Feed(usize, usize, usize),
    /// Пройти хранилище источника целиком и дописать его значения.
    Walk(usize),
    /// Дописать значения источника, у которого хранилища нет вовсе.
    Plain(usize),
    /// Дописать пустые значения на документы, которых хранилище не накрыло.
    Blank(usize, u32, u32),
}

/// План переноса хранимого поля.
struct Carry {
    ops: Vec<Op>,
    /// По источнику: сколько значений в каждом его куске. Пусто — источник не
    /// переносится.
    counts: Vec<Vec<u32>>,
}

/// Отрезок словаря источника — единица, которой распоряжается коалесцирование.
struct Span {
    src: usize,
    dict: usize,
    from: usize,
    to: usize,
    /// Объём кусков **до сжатия**: во столько обойдётся роспуск отрезка.
    volume: u64,
    /// Длина записи словаря в `.sdic`.
    dict_len: u64,
}

/// Пересобирать ли хранимое старым путём — целиком, через [`crate::store::build`].
///
/// ## Зачем этот выключатель существует всегда
///
/// Побайтовая сверка «слитый равен собранному с нуля» для переноса не работает
/// **по построению**: перенесённый кусок сжат словарём источника, а собранный
/// заново — словарём, обученным на слитой колонке. Значит нужен другой критерий,
/// и он один: два пути обязаны давать **одно и то же содержимое** на каждом
/// документе. Дифференциальный оракул возможен ровно до тех пор, пока старый
/// путь никуда не делся, — поэтому он и остаётся за переменной среды, а не
/// удаляется как отработавший.
fn rebuild_wanted() -> bool {
    #[cfg(test)]
    {
        // Проверки идут вперемешку в одном процессе, и переменная среды у них
        // общая: выставь её тест — и соседний, меряющий перенос, увидел бы
        // пересборку. Поэтому у проверок свой переключатель, потоковый.
        if let Some(v) = FORCE_REBUILD.with(|c| c.get()) {
            return v;
        }
    }
    std::env::var_os("SVINDB_STORE_REBUILD").is_some()
}

#[cfg(test)]
thread_local! {
    static FORCE_REBUILD: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Имена в каталоге сегмента, по порядку.
#[cfg(test)]
fn dir_names(dir: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("каталог сегмента")
        .map(|e| e.expect("запись каталога").file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    names
}

/// Всё, чему позволено лежать в каталоге сегмента **снаружи** контейнера.
///
/// Каждое из четырёх имён лежит снаружи намеренно: карта живых правится на живом
/// сегменте, заголовок читается до всякого отображения, сводка пишется после
/// заголовка и накрывает его самого.
///
/// Список общий на две проверки — на пересборку и на перенос, — потому что
/// обещание у них одно. Запиши его дважды, и однажды один из двух путей оставит
/// россыпь, а свой список ему это простит.
#[cfg(test)]
fn outside_names() -> Vec<String> {
    let mut v = vec![
        "head.tsv".to_string(),
        crate::live::FILE.to_string(),
        crate::container::FILE.to_string(),
        crate::sums::FILE.to_string(),
    ];
    v.sort();
    v
}

/// Заставляет слияние идти старым путём на этом потоке — до снятия сторожа.
#[cfg(test)]
struct Rebuilding;

#[cfg(test)]
impl Rebuilding {
    fn on() -> Rebuilding {
        FORCE_REBUILD.with(|c| c.set(Some(true)));
        Rebuilding
    }
}

#[cfg(test)]
impl Drop for Rebuilding {
    fn drop(&mut self) {
        FORCE_REBUILD.with(|c| c.set(None));
    }
}

/// Решает, переносить ли хранимое поля, и раскладывает перенос по действиям.
///
/// `None` — переносить нечего или невыгодно, поле собирается заново.
///
/// ## Как считаются грязные куски
///
/// Кусок `i` накрывает номера `[first(i), first(i+1))`: номера документов внутри
/// куска не хранятся вовсе — тело это «длина|байты» подряд, — а плотность
/// нумерации гарантирована проверкой `check::stores`. Значит достаточно пройти
/// по этим границам таблицу перенумерации — ту самую, что слияние построило
/// раньше, — и **до единой распаковки** известно, в скольких кусках есть
/// погашенные. А когда не погашено ничего, не нужно и это: грязных нет по
/// определению, и карта не читается вовсе.
///
/// Последний кусок — исключение: за ним засечки нет, и число его значений
/// приходится узнать распаковкой. Ровно одной на источник, и только у тех, кто
/// прошёл сторож против россыпи.
///
/// ## Порог плавности
///
/// Грязных больше половины — старый путь: перенести и пересжать три четверти
/// дороже, чем собрать колонку заново, а заново ещё и возвращает место
/// погашенных. Разделение путей обслуживания от этого не страдает: уборка зовёт
/// слияние с **одним** источником при доле мёртвых от `dead_fraction`, и там при
/// ста пятидесяти восьми документах на кусок грязными оказываются практически
/// все куски — пересжатие и есть смысл операции. Укрупнение же сливает восемь
/// обычно чистых, и там грязных нет вовсе.
fn plan_carry(
    sources: &[&Index],
    plan: &Plan,
    f: usize,
    remap: &[Vec<u32>],
    any_dead: bool,
) -> Option<Carry> {
    let live = |i: usize, d: u32| !any_dead || remap[i][d as usize] != u32::MAX;
    let store_of = |i: usize| -> Option<&crate::store::Store> {
        let j = plan.local[i][f];
        (j != NONE).then(|| sources[i].fields[j].store.as_ref()).flatten()
    };

    let (min_blocks, min_dict) = crate::store::carry_floor();
    let n_src = sources.len();
    let mut counts: Vec<Vec<u32>> = vec![Vec::new(); n_src];
    let mut dirty: Vec<Vec<bool>> = vec![Vec::new(); n_src];
    let mut alive: Vec<Vec<u32>> = vec![Vec::new(); n_src];
    let mut cover: Vec<u32> = vec![0; n_src];
    let mut good: Vec<bool> = vec![false; n_src];
    let mut spans: Vec<Span> = Vec::new();
    let mut blocks_all = 0usize;
    let mut blocks_dirty = 0usize;

    for i in 0..n_src {
        let Some(st) = store_of(i) else { continue };
        let n = st.blocks();
        // Сторож против россыпи: мелкий источник переносить дороже, чем
        // распаковать, — его словарь лёг бы в `.sdic` рядом с данными, которые
        // он описывает, и перевесил бы их.
        if n < min_blocks {
            continue;
        }
        // Словарь на полу `DICT_MIN` — это словарь, получивший размер от пола, а
        // не от колонки: так выглядит всякая колонка мельче восьми мегабайт.
        let dicts = st.dicts();
        if (0..dicts).any(|k| st.dict_bytes(k).is_none_or(|b| b <= min_dict)) {
            continue;
        }
        // Сумму распакованного перенос **копирует**, а не считает: пересчёт
        // означал бы распаковку. Нет сумм — нет и переноса; такой источник
        // соберётся заново, и суммы у него появятся.
        if !st.has_crcs() {
            continue;
        }

        let docs_i = sources[i].docs as u32;
        let mut cnt = vec![0u32; n];
        let mut sane = true;
        for (c, slot) in cnt.iter_mut().enumerate() {
            let Some((_, _, _, first)) = st.mark_at(c) else {
                sane = false;
                break;
            };
            let next = if c + 1 < n {
                match st.mark_at(c + 1) {
                    Some((_, _, _, x)) => x,
                    None => {
                        sane = false;
                        break;
                    }
                }
            } else {
                // Единственная распаковка на источник — и только ради того,
                // чтобы узнать, сколько значений в хвостовом куске.
                match st.chunk_at(c) {
                    Some((_, k, _)) => first.saturating_add(k as u32),
                    None => {
                        sane = false;
                        break;
                    }
                }
            };
            if next <= first || next > docs_i {
                sane = false;
                break;
            }
            *slot = next - first;
        }
        if !sane {
            continue;
        }

        let mut dirt = vec![false; n];
        let mut liv = vec![0u32; n];
        let mut at = 0u32;
        for c in 0..n {
            let mut k = 0u32;
            for d in at..at + cnt[c] {
                if live(i, d) {
                    k += 1;
                }
            }
            liv[c] = k;
            dirt[c] = k != cnt[c];
            at += cnt[c];
        }

        let mut runs = Vec::with_capacity(dicts);
        for k in 0..dicts {
            let (Some(from), Some(rec)) = (st.dict_first(k), st.dict_record(k)) else {
                runs.clear();
                break;
            };
            let to = if k + 1 < dicts { st.dict_first(k + 1).unwrap_or(n) } else { n };
            if to <= from || to > n {
                runs.clear();
                break;
            }
            let volume: u64 = (from..to).map(|c| st.mark_at(c).map_or(0, |m| m.2 as u64)).sum();
            runs.push(Span { src: i, dict: k, from, to, volume, dict_len: rec.len() as u64 });
        }
        if runs.len() != dicts {
            continue;
        }

        blocks_all += n;
        blocks_dirty += dirt.iter().filter(|x| **x).count();
        cover[i] = at;
        counts[i] = cnt;
        dirty[i] = dirt;
        alive[i] = liv;
        good[i] = true;
        spans.extend(runs);
    }

    if spans.is_empty() {
        return None;
    }
    // Порог плавности. Сравнение целыми, а не долями: половина от нечётного
    // числа кусков иначе зависела бы от того, как ляжет округление.
    if blocks_dirty * 2 > blocks_all {
        return None;
    }

    let volumes: Vec<u64> = spans.iter().map(|s| s.volume).collect();
    let dict_lens: Vec<u64> = spans.iter().map(|s| s.dict_len).collect();
    let column: u64 = volumes.iter().sum();
    let keep = crate::store::coalesce(&volumes, &dict_lens, column);
    if !keep.iter().any(|k| *k) {
        return None;
    }

    let mut ops: Vec<Op> = Vec::new();
    for i in 0..n_src {
        if !good[i] {
            // Источник целиком уходит в соседний отрезок обычным сжатием.
            ops.push(if store_of(i).is_some() { Op::Walk(i) } else { Op::Plain(i) });
            continue;
        }
        for (at, sp) in spans.iter().enumerate().filter(|(_, s)| s.src == i) {
            if !keep[at] {
                ops.push(Op::Feed(i, sp.from, sp.to));
                continue;
            }
            ops.push(Op::Open(i, sp.dict));
            for c in sp.from..sp.to {
                if alive[i][c] == 0 {
                    // Погашены все: кусок исчезает, и непрерывность отрезка от
                    // этого не страдает — отрезок задан первым куском, а не их
                    // числом.
                    continue;
                }
                ops.push(if dirty[i][c] { Op::Recode(i, c) } else { Op::Carry(i, c) });
            }
        }
        if cover[i] < sources[i].docs as u32 {
            ops.push(Op::Blank(i, cover[i], sources[i].docs as u32));
        }
    }

    // Первый словарь поднимается **вперёд всего**: то, что дописывается до него
    // — значения источника без хранилища или распущенного отрезка, — надо чем-то
    // сжимать, а своего словаря у этих значений нет. Отрезок от переноса вперёд
    // не рвётся: он задан первым куском, и первым куском становится нулевой.
    let at = ops.iter().position(|o| matches!(o, Op::Open(..)))?;
    let head = ops.remove(at);
    ops.insert(0, head);
    Some(Carry { ops, counts })
}

/// Выполняет перенос: куски ложатся в `<out>/<name>.*`.
///
/// Доводов много, и складывать их в структуру незачем: все до одного — то, что
/// уже есть у вызывающего, а лишний тип между планом и его исполнением только
/// добавил бы места, где они разойдутся.
///
/// Наружу отдаётся весь [`crate::store::Made`], а не одно число сжатых кусков:
/// сколько кусков и словарей вышло, знает только сам перенос, а без этого
/// [`pour_store`] не отличит «переносить было нечего» от «файл потерялся».
#[allow(clippy::too_many_arguments)]
fn carry_store(
    sources: &[&Index],
    plan: &Plan,
    f: usize,
    remap: &[Vec<u32>],
    any_dead: bool,
    carry: &Carry,
    out: &str,
    name: &str,
    docs: usize,
) -> std::io::Result<crate::store::Made> {
    let live = |i: usize, d: u32| !any_dead || remap[i][d as usize] != u32::MAX;
    let store_of = |i: usize| -> Option<&crate::store::Store> {
        let j = plan.local[i][f];
        (j != NONE).then(|| sources[i].fields[j].store.as_ref()).flatten()
    };
    let lost = |what: &str| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{out}/{name}: перенос хранимого сорвался — {what}"),
        )
    };

    let mut tr = crate::store::Transfer::create(out, name, crate::codec::Effort::Fast)?;
    for op in carry.ops.iter() {
        match *op {
            Op::Open(i, k) => {
                let st = store_of(i).ok_or_else(|| lost("хранилище источника пропало"))?;
                let rec = st.dict_record(k).ok_or_else(|| lost("запись словаря не читается"))?;
                tr.open_dict(rec)?;
            }
            Op::Carry(i, c) => {
                let st = store_of(i).ok_or_else(|| lost("хранилище источника пропало"))?;
                let body = st.chunk_body(c).ok_or_else(|| lost("тело куска не читается"))?;
                let (_, _, raw, _) =
                    st.mark_at(c).ok_or_else(|| lost("засечка куска не читается"))?;
                let crc = st.chunk_crc(c).ok_or_else(|| lost("суммы куска нет"))?;
                let n = carry.counts[i].get(c).copied().unwrap_or(0);
                tr.carry(body, raw, n, crc)?;
            }
            Op::Recode(i, c) => {
                let st = store_of(i).ok_or_else(|| lost("хранилище источника пропало"))?;
                let (_, _, _, first) =
                    st.mark_at(c).ok_or_else(|| lost("засечка куска не читается"))?;
                let chunk = st.chunk_bytes(c).ok_or_else(|| lost("кусок не распаковался"))?;
                let mut raw = Vec::with_capacity(chunk.len());
                let mut kept = 0u32;
                let mut at = 0usize;
                let mut d = first;
                while at < chunk.len() {
                    let l = chunk
                        .get(at..at + 4)
                        .and_then(|h| h.try_into().ok())
                        .map(u32::from_le_bytes)
                        .ok_or_else(|| lost("кусок оборвался на длине значения"))?
                        as usize;
                    let end = at + 4 + l;
                    if end > chunk.len() {
                        return Err(lost("значение уходит за границу куска"));
                    }
                    if live(i, d) {
                        // Копируется запись целиком, вместе с её длиной: заново
                        // складывать заголовок незачем, а лишний разбор — лишний
                        // способ разойтись с исходной раскладкой.
                        raw.extend_from_slice(&chunk[at..end]);
                        kept += 1;
                    }
                    at = end;
                    d += 1;
                }
                tr.recode(&raw, kept)?;
            }
            Op::Feed(i, from, to) => {
                let st = store_of(i).ok_or_else(|| lost("хранилище источника пропало"))?;
                for c in from..to {
                    let (_, _, _, first) =
                        st.mark_at(c).ok_or_else(|| lost("засечка куска не читается"))?;
                    let chunk = st.chunk_bytes(c).ok_or_else(|| lost("кусок не распаковался"))?;
                    let mut at = 0usize;
                    let mut d = first;
                    while at < chunk.len() {
                        let l = chunk
                            .get(at..at + 4)
                            .and_then(|h| h.try_into().ok())
                            .map(u32::from_le_bytes)
                            .ok_or_else(|| lost("кусок оборвался на длине значения"))?
                            as usize;
                        let end = at + 4 + l;
                        let v =
                            chunk.get(at + 4..end).ok_or_else(|| lost("значение за границей"))?;
                        if live(i, d) {
                            tr.push(v)?;
                        }
                        at = end;
                        d += 1;
                    }
                }
            }
            Op::Walk(i) => {
                let st = store_of(i).ok_or_else(|| lost("хранилище источника пропало"))?;
                let mut w = st.walk();
                let mut want = 0u32;
                while let Some((doc, val)) = w.next() {
                    while want < doc {
                        if live(i, want) {
                            tr.push(&[])?;
                        }
                        want += 1;
                    }
                    if live(i, doc) {
                        tr.push(val)?;
                    }
                    want = doc + 1;
                }
                // «Дальше ничего нет» и «дальше нечитаемо» — разные вещи, и
                // спутать их значит потерять половину поля, отчитавшись об
                // успехе.
                if w.broken() {
                    return Err(lost("проход по хранилищу источника оборвался на порче"));
                }
                for d in want..sources[i].docs as u32 {
                    if live(i, d) {
                        tr.push(&[])?;
                    }
                }
            }
            Op::Plain(i) => {
                let j = plan.local[i][f];
                for d in 0..sources[i].docs as u32 {
                    if !live(i, d) {
                        continue;
                    }
                    if j == NONE {
                        tr.push(&[])?;
                    } else {
                        // Хранилища нет, а значение может быть: у числового поля
                        // оно лежит в колонке, и `fetch` печатает его обратно.
                        tr.push(&sources[i].fetch(d, j).unwrap_or_default())?;
                    }
                }
            }
            Op::Blank(i, from, to) => {
                for d in from..to {
                    if live(i, d) {
                        tr.push(&[])?;
                    }
                }
            }
        }
    }

    let made = tr.finish()?;
    // Единственная сверка, которая ловит съехавшую нумерацию: перенос обязан
    // накрыть ровно столько документов, сколько в слитом сегменте. Разойдись
    // они — значения молча съехали бы на чужие номера, и ни размер файлов, ни
    // их суммы этого не увидели бы.
    if made.docs as usize != docs {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{out}/{name}: перенос накрыл {} документов при {docs} в сегменте", made.docs),
        ));
    }
    Ok(made)
}

/// Пишет файл слитого сегмента и **доводит его до диска**.
///
/// `write_all` возвращается, когда байты приняты ядром, а не когда легли на
/// носитель. Порядок «заголовок последним» без этого ничего не обещает: вытесняет
/// страницы ядро, и порядок вытеснения свой. То есть после потери питания
/// возможен слитый сегмент, на который список уже ссылается, а содержимого нет —
/// и это тем опаснее, что исходные сегменты к тому моменту уже удалены, а журнал
/// обрезан по отметке.
///
/// Приём это делал с самого появления сегментов (`indexer::write`), укрупнение —
/// нет, хотя пишет ровно те же файлы и ровно с теми же последствиями.
fn write(dir: &str, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    let mut f = File::create(format!("{dir}/{name}"))?;
    f.write_all(bytes)?;
    f.sync_all()
}

/// Доводит до диска сам каталог: без этого имя нового файла может не пережить
/// потерю питания, даже когда его содержимое пережило.
fn sync_dir(dir: &str) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Сколько байт брать за раз, переливая файл в кусок контейнера.
///
/// Мегабайт: столько же читает подряд обычный `cp`, и это единственная память,
/// которую перелив вообще занимает.
const POUR_BUF: usize = 1 << 20;

/// Переливает отдельно записанный поток в кусок контейнера и сносит файл.
///
/// ## Почему потоки не пишутся в контейнер сразу
///
/// Открытым в контейнере бывает **один** кусок: кусок лежит непрерывно, и
/// оглавление знает про него одну пару «смещение, длина». Обход слов кормит
/// четыре потока разом — списки, слова, позиции и словарь, — и собрать хоть один
/// из них в памяти нельзя: на сегменте у потолка четырёх гигабайт это гигабайты
/// кучи, то есть ровно та величина, ради устранения которой потоки и заведены
/// (см. [`Sink`]). Значит прямо в контейнер идёт самый крупный — списки, — а
/// остальные ложатся файлами и переезжают сюда, каждый на своё место в порядке
/// кусков.
///
/// Цена — одна лишняя запись и одно лишнее чтение этих файлов. Приём её не
/// платит: у него потоковый кусок один. Взамен памяти слияние не тратит ни
/// байта сверх [`POUR_BUF`], а это свойство отдельно завоёвано и дороже места на
/// носителе.
///
/// Отказ, если файла нет: перелив зовут ровно там, где поток только что закрыт,
/// и пропавший файл означает, что кусок сегмента потерян. Тихо пропустить его
/// значило бы выдать сегмент без слов или без словаря за готовый.
///
/// Возвращается длина уложенного куска: у вызывающего бывает своё знание о том,
/// сколько в куске обязано быть, и «кусок пуст» — не тот же исход, что «куска
/// нет».
fn pour(pack: &mut crate::container::Writer, out: &str, name: &str) -> std::io::Result<u64> {
    let path = format!("{out}/{name}");
    let mut f =
        File::open(&path).map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
    pack.begin(name)?;
    let mut buf = vec![0u8; POUR_BUF];
    loop {
        if !crate::work::compaction_checkpoint(0) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "compaction остановлен graceful shutdown",
            ));
        }
        let n = match std::io::Read::read(&mut f, &mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            // Прерванное сигналом чтение — не конец файла и не порча: повторяем.
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(std::io::Error::new(e.kind(), format!("{path}: {e}"))),
        };
        let Some(part) = buf.get(..n) else {
            return Err(std::io::Error::other(format!(
                "{path}: чтение отдало {n} байт при буфере {POUR_BUF}"
            )));
        };
        // Здесь одна итерация уже равна одному мегабайту: уступаем сразу,
        // перед записью, а не после уже выполненного конфликтующего I/O.
        if !crate::work::compaction_checkpoint(n as u64) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "compaction остановлен graceful shutdown",
            ));
        }
        pack.append(part)?;
    }
    let len = pack.end()?;
    drop(f);
    std::fs::remove_file(&path)
        .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
    Ok(len)
}

/// Переносит в контейнер файлы хранимого поля, оставшиеся от [`crate::store::Transfer`].
///
/// Перенос содержимого пишет тело потоком в файл — тем же он его и писал до
/// контейнера, — а засечки, словари и суммы отдаёт буферами. Тело переливается,
/// остальное кладётся как есть.
///
/// ## Какие файлы обязаны быть, решает не каталог, а сам перенос
///
/// Спрашивать об этом файловую систему нельзя. `Path::exists()` отвечает «нет»
/// и тогда, когда файла и правда нет, и тогда, когда `stat` не удался, — а
/// прежний ранний `Ok(())` по такому ответу объявлял поле перенесённым, оставляя
/// `.smrk`, `.sdic` и `.scrc` россыпью в каталоге. Заголовок при этом обещает
/// упакованный сегмент, читатель ищет куски в оглавлении и не находит: поле
/// молча отдаёт пустоту на каждый документ, а исходные сегменты к тому времени
/// снесены уборкой.
///
/// Поэтому берётся [`crate::store::Made`] — счёт того, что перенос **сам**
/// уложил. Ни одного куска (`blocks == 0`) — значений не было вовсе, и
/// `Transfer::finish` снёс всё, что успел завести; хранилища у поля не будет, и
/// это тот же исход, каким на пустое поле отвечает пересборка (`build` отдаёт
/// `None`). Куски есть — тело, засечки, словарь и суммы обязаны лежать на месте,
/// и всякий другой ответ здесь отказ.
///
/// Таблица словарей `.sdix` бывает не всегда: при единственном словаре перенос
/// её не пишет, и куска ей взяться неоткуда. Спрашивается она ровно тогда, когда
/// словарей больше одного, — иначе её пропажу не отличить от законного
/// отсутствия.
fn pour_store(
    pack: &mut crate::container::Writer,
    out: &str,
    name: &str,
    made: &crate::store::Made,
) -> std::io::Result<()> {
    if made.blocks == 0 {
        return Ok(());
    }
    let body = format!("{name}.sbin");
    let len = pour(pack, out, &body)?;
    // Пустое тело при уложенных кусках — не «нечего переносить», а порванный
    // перенос: кусков без значений не бывает. Уложи такой кусок молча — и поле
    // отвечало бы пустотой, ничем не отличаясь от поля, которого нет.
    if len == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{out}/{body}: тело пусто при {} перенесённых кусках", made.blocks),
        ));
    }
    let rest: &[&str] =
        if made.dicts > 1 { &["smrk", "sdic", "scrc", "sdix"] } else { &["smrk", "sdic", "scrc"] };
    for ext in rest {
        let path = format!("{out}/{name}.{ext}");
        let bytes = std::fs::read(&path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "{path}: перенос закрыл этот кусок при {} кусках и {} словарях, \
                     а забрать его не вышло — {e}",
                    made.blocks, made.dicts
                ),
            )
        })?;
        // «Куска нет» и «кусок пуст» — разные исходы, и второй здесь такой же
        // отказ: засечки и словарь при уложенных кусках пустыми не бывают, а
        // пустой кусок в контейнере читатель принял бы за законный.
        if bytes.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{path}: кусок пуст при {} перенесённых кусках", made.blocks),
            ));
        }
        pack.put(&format!("{name}.{ext}"), &bytes)?;
        std::fs::remove_file(&path)
            .map_err(|e| std::io::Error::new(e.kind(), format!("{path}: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::Indexer;
    use crate::search::{search, search_exhaustive};
    use crate::segments::{QuerySpec, Segments, SpecWord, Term};

    fn corpus(name: &str, per_seg: u32) -> String {
        let dir = std::env::temp_dir().join(format!("svindb-merge-{name}"));
        let dir = dir.to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix =
            Indexer::new(&dir, vec!["title".into(), "body".into()], vec![2.0, 1.0], per_seg);
        for i in 0..2000u32 {
            let title = format!("док{} загол{}", i % 97, i % 13);
            let extra = " хвост".repeat((i % 47) as usize);
            let body = format!("общее слово{} форма{} тело{}{}", i % 7, i % 211, i % 53, extra);
            ix.add(&format!("d{i:05}"), &[&title, &body]);
        }
        ix.flush();
        dir
    }

    fn spec(field: usize, terms: &[&str], k: usize) -> QuerySpec {
        QuerySpec {
            words: vec![SpecWord {
                given: None,
                fid: field,
                terms: terms.iter().map(|t| Term::parse(t)).collect(),
            }],
            k,
            ..QuerySpec::default()
        }
    }

    /// Все номера списка, по порядку.
    fn docs_of(ix: &Index, list: u32) -> Vec<u32> {
        let mut c = ix.cursor(list, 0, 1.0);
        let mut out = Vec::new();
        loop {
            let d = c.cur();
            if d == u32::MAX {
                break;
            }
            out.push(d);
            c.next();
        }
        out
    }

    /// Слитый сегмент содержит ровно то же, что исходные вместе.
    ///
    /// Сверяется **состав**, а не ранжирование, и это не упрощение. У слитого
    /// сегмента своя статистика: одна средняя длина поля на всё и одна частота
    /// слова вместо десяти отдельных. Скоры от этого меняются законно — то же
    /// свойство, что записано тестом `idf_is_per_segment`. А вот множество
    /// «слово → номера документов» меняться не имеет права ни на один номер.
    #[test]
    fn merged_holds_exactly_the_same_lists() {
        let dir = corpus("same", 250);
        let before = Segments::load(&dir).unwrap();
        assert!(before.list.len() >= 8, "нужно несколько сегментов");

        let out = format!("{dir}/mrg0000");
        let sources: Vec<&Index> = before.list.iter().map(|a| a.as_ref()).collect();
        let res = merge(&sources, &out).unwrap();
        assert_eq!(res.docs, before.docs());

        let merged = index::load(&out).unwrap();
        assert_eq!(merged.docs, before.docs());

        // Собираем эталон: слово → номера в общем пространстве.
        let mut want: std::collections::BTreeMap<(u32, String), Vec<u64>> = Default::default();
        for (i, s) in before.list.iter().enumerate() {
            let mut w = s.dict.walk();
            while let Some((f, t, list)) = w.next() {
                let key = (f, String::from_utf8_lossy(t).to_string());
                let docs = docs_of(s, list);
                want.entry(key)
                    .or_default()
                    .extend(docs.iter().map(|d| *d as u64 + before.base[i]));
            }
        }
        assert_eq!(merged.nterms, want.len(), "число слов в словаре");

        let mut seen = 0usize;
        let mut w = merged.dict.walk();
        while let Some((f, t, list)) = w.next() {
            let key = (f, String::from_utf8_lossy(t).to_string());
            let expect = want.get(&key).unwrap_or_else(|| panic!("лишнее слово {key:?}"));
            assert_eq!(
                merged.t_df(list as usize) as usize,
                expect.len(),
                "число документов у {key:?}"
            );
            let got: Vec<u64> = docs_of(&merged, list).iter().map(|&d| d as u64).collect();
            assert_eq!(&got, expect, "номера у {key:?}");
            seen += 1;
        }
        assert_eq!(seen, want.len(), "пройдены все слова");
        assert_eq!(res.postings, want.values().map(|v| v.len() as u64).sum::<u64>());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// После слияния ни один вклад не выше сохранённого потолка.
    ///
    /// Потолок ниже вклада — это не неточность, а потеря документов: отсечение
    /// MaxScore вправе выбросить список, чей потолок не дотягивает до порога.
    /// Причина была в том, что слияние писало среднюю длину поля в заголовок
    /// округлённой до четырёх знаков, а потолки считало по неокруглённой:
    /// писатель и читатель считали по разным числам. В приёме эту же поломку
    /// нашли и починили, в слияние правку не перенесли.
    ///
    /// Ловится это только на **многоблочных** списках: у списка короче блока
    /// потолок не хранится, а восстанавливается из пары «уровень, частота» той
    /// же таблицей читателя — и потому самосогласован при любой средней длине.
    /// Поэтому здесь корпус, где слово встречается у каждого второго документа,
    /// а длины разбросаны: и то и другое обязательно, иначе проверка проходит
    /// вхолостую.
    #[test]
    fn no_posting_outscores_its_ceiling_after_a_merge() {
        let dir = std::env::temp_dir().join("svindb-merge-ceiling").to_string_lossy().to_string();
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::new(&dir, vec!["t".into()], vec![1.0], 4000);
        for i in 0..12_000u32 {
            // Каждый девятьсот девяносто седьмой — длинный. Так средняя длина
            // получается дробной с длинным хвостом, а он-то и обрубается.
            let long = if i % 997 == 0 { " хвост".repeat(1000) } else { String::new() };
            ix.add(&format!("d{i:05}"), &[&format!("общее сл{}{}", i % 40, long)]);
        }
        ix.flush();

        let before = Segments::load(&dir).unwrap();
        let out = format!("{dir}/mrg0000");
        let sources: Vec<&Index> = before.list.iter().map(|a| a.as_ref()).collect();
        merge(&sources, &out).unwrap();
        drop(before);
        let merged = index::load(&out).unwrap();

        let mut over = 0usize;
        let mut worst = 0f64;
        let mut checked = 0usize;
        let mut w = merged.dict.walk();
        while let Some((f, _, list)) = w.next() {
            let t = merged.term(list as usize, f as usize);
            if t.df as usize <= crate::bits::BLOCK {
                continue;
            }
            let mut c = crate::index::Cursor::new(&merged, list, f as usize, 1.0, None);
            while c.cur() != u32::MAX {
                checked += 1;
                // Сверяется потолок БЛОКА и только он. Потолок списка брать
                // нельзя: он не хранится, а восстанавливается читателем из пары
                // «уровень, частота» его же таблицей — то есть самосогласован
                // при любой средней длине и испорченным быть не может.
                let (got, cap) = (c.score(), c.block_bound());
                if got > cap * (1.0 + 1e-6) {
                    over += 1;
                    worst = worst.max(got / cap - 1.0);
                }
                c.next();
            }
        }
        assert!(checked > 10_000, "проверено всего {checked} постингов — корпус не тот");
        assert_eq!(over, 0, "{over} постингов выше потолка, худшее превышение {worst:.3e}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Поиск по слитому сегменту сходится с полным перебором по нему же.
    #[test]
    fn merged_search_matches_exhaustive() {
        let dir = corpus("search", 250);
        let before = Segments::load(&dir).unwrap();
        let out = format!("{dir}/mrg0000");
        let sources: Vec<&Index> = before.list.iter().map(|a| a.as_ref()).collect();
        merge(&sources, &out).unwrap();
        let merged = index::load(&out).unwrap();

        for q in [
            spec(1, &["общее"], 10),
            spec(1, &["слово3"], 10),
            spec(1, &["форма17"], 5),
            spec(1, &["форма*"], 10),
            spec(1, &["тело*"], 20),
            spec(0, &["док*"], 10),
        ] {
            let req = q.resolve(&merged);
            let (fast, _) = search(&merged, &req, 0.0);
            let slow = search_exhaustive(&merged, &req);
            assert_eq!(fast.len(), slow.len());
            for (a, b) in fast.iter().zip(slow.iter()) {
                assert_eq!(a.doc, b.doc, "документ");
                assert!((a.score - b.score).abs() < 1e-12, "скор");
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Идентификаторы переезжают в слитый сегмент без перестановок.
    #[test]
    fn ids_survive_the_merge() {
        let dir = corpus("ids", 250);
        let before = Segments::load(&dir).unwrap();
        let out = format!("{dir}/mrg0000");
        let sources: Vec<&Index> = before.list.iter().map(|a| a.as_ref()).collect();
        merge(&sources, &out).unwrap();

        let merged = index::load(&out).unwrap();
        // Куски берутся у самого сегмента, а не по имени файла: слитый сегмент
        // теперь контейнер, и `ids.bin` отдельным файлом в каталоге не лежит.
        let store = ids::Ids::open(&merged.src, merged.docs);
        for doc in [0usize, 1, 249, 250, 999, merged.docs - 1] {
            assert_eq!(store.get(doc).as_deref(), Some(format!("d{doc:05}").as_str()));
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Слово сегмента и идентификаторы документов, которые за ним стоят.
    ///
    /// Идентификаторы, а не номера: у слитого сегмента нумерация своя, и
    /// сравнивать номера значило бы сравнивать не то. Порядок сохраняется —
    /// источники склеиваются подряд, и внутри каждого списки возрастают, — так
    /// что перестановка внутри списка проверку не пройдёт.
    fn term_ids(ix: &Index) -> Vec<((u32, String), Vec<String>)> {
        let store = ids::Ids::open(&ix.src, ix.docs);
        let mut out: Vec<((u32, String), Vec<String>)> = Vec::new();
        let mut w = ix.dict.walk();
        while let Some((f, t, list)) = w.next() {
            let ids: Vec<String> = docs_of(ix, list)
                .iter()
                .map(|d| store.get(*d as usize).expect("идентификатор документа"))
                .collect();
            out.push(((f, String::from_utf8_lossy(t).to_string()), ids));
        }
        out
    }

    /// Слитый сегмент — **один файл**, и отвечает он тем же, чем отвечали источники.
    ///
    /// ## Зачем это отдельной проверкой
    ///
    /// Смысл контейнера один: область отображения у ядра посчитана, и потолок
    /// на их число — потолок числа сегментов. Приём кладёт куски одним файлом,
    /// и оставь укрупнение россыпь — весь выигрыш пропадал бы ровно там, где он
    /// нужнее всего: после укрупнения сегментов меньше, а файлов у них снова
    /// два десятка на каждый. Поэтому проверяется не «читается», а «читается
    /// **и** россыпи не осталось».
    #[test]
    fn a_merged_segment_is_one_file_and_answers_the_same() {
        let dir = corpus("packed", 700);
        let before = Segments::load(&dir).expect("индекс открылся");
        assert!(before.list.len() >= 2, "нужно хотя бы два сегмента");

        // Эталон снимается **до** слияния: словари источников подряд.
        let mut want: Vec<((u32, String), Vec<String>)> = Vec::new();
        for s in before.list.iter() {
            for (key, ids) in term_ids(s.as_ref()) {
                match want.iter_mut().find(|(k, _)| *k == key) {
                    Some((_, v)) => v.extend(ids),
                    None => want.push((key, ids)),
                }
            }
        }
        want.sort_by(|a, b| a.0.cmp(&b.0));
        // Обе стороны головного сравнения снимает один и тот же `term_ids`.
        // Опустей обход словаря — и сравнение сошлось бы, доказав только то, что
        // пустоту получили дважды. Поэтому эталон проверяется на непустоту
        // отдельно, и не по числу слов, а по номерам за ними: словарь из слов с
        // пустыми списками сравнение прошёл бы так же тихо.
        assert!(
            want.iter().any(|(_, ids)| !ids.is_empty()),
            "эталон пуст: слитый сегмент сравнивать не с чем"
        );
        // Содержимое хранимого поля у источников: по номеру документа выхода.
        let mut body_want: Vec<Option<Vec<u8>>> = Vec::new();
        for s in before.list.iter() {
            let f = s.fields.iter().position(|f| f.name == "body").expect("поле body");
            for d in 0..s.docs as u32 {
                body_want.push(s.fetch(d, f));
            }
        }

        let out = format!("{dir}/mrg0000");
        let sources: Vec<&Index> = before.list.iter().map(|a| a.as_ref()).collect();
        merge(&sources, &out).expect("слияние");

        // Контейнер есть, и заголовок обещает его настоящую длину.
        let head = index::read_head(&out).expect("заголовок слитого");
        let promised = head.container.expect("слитый сегмент вышел россыпью");
        let path = format!("{out}/{}", crate::container::FILE);
        assert_eq!(
            promised,
            std::fs::metadata(&path).expect("контейнер на месте").len(),
            "заголовок обещает не ту длину контейнера"
        );

        // Россыпи не осталось ни куска: в каталоге только то, чему позволено
        // лежать снаружи контейнера (см. [`outside_names`]).
        assert_eq!(
            dir_names(&out),
            outside_names(),
            "в каталоге слитого сегмента осталась россыпь"
        );

        // Куски названы теми же именами, какими их кладёт приём.
        let table = crate::container::Table::read(&path).expect("оглавление");
        for must in [
            "postings.bin",
            "blocks.bin",
            "terms.bin",
            "terms.idx",
            "dict.blob",
            "dict.idx",
            "norms.bin",
            "ids.bin",
            "ids.idx",
            "idmap.bloom",
            "body.sbin",
            "body.smrk",
            "body.sdic",
        ] {
            assert!(table.find(must).is_some(), "куска {must} нет в контейнере");
        }
        // И порядок тот же: метаданные прогрева лежат в хвосте, за списками и
        // содержимым. Сверяется смещением, а не порядком записей оглавления, —
        // оглавление отсортировано по имени и про порядок укладки не говорит.
        let at = |name: &str| table.find(name).unwrap_or_else(|| panic!("{name}")).off;
        assert!(at("postings.bin") < at("body.sbin"), "содержимое легло раньше списков");
        for meta in ["blocks.bin", "terms.bin", "dict.blob", "norms.bin"] {
            assert!(
                at(meta) > at("body.sbin"),
                "кусок {meta} лёг раньше содержимого — прогрев перестал быть одним чтением"
            );
        }

        // И отвечает он ровно тем же: слово в слово, документ в документ.
        let merged = index::load(&out).expect("слитый открылся");
        assert_eq!(merged.docs, before.docs(), "документы потерялись");
        assert_eq!(term_ids(&merged), want, "слитый отвечает не тем, чем источники");

        let f = merged.fields.iter().position(|f| f.name == "body").expect("поле body");
        let got: Vec<Option<Vec<u8>>> =
            (0..merged.docs as u32).map(|d| merged.fetch(d, f)).collect();
        assert_eq!(got, body_want, "содержимое поля разошлось с источниками");

        std::fs::remove_dir_all(&dir).ok();
    }
}

// ---------------------------------------------------------------------------
// Политика: что и когда сливать
// ---------------------------------------------------------------------------

/// Когда сливать и по сколько — у **плоского** планировщика.
///
/// Плоский он потому, что ярусов не знает вовсе: единственная мера окна —
/// число документов, и потолок у неё один на весь индекс. Что этого мало, видно
/// числами, и они у [`Tier`]. Здесь политика сохранена целиком и включается
/// настройкой (`merge_policy = flat`): оператору нужен способ сказать «не
/// сейчас» ярусному укрупнению, не выключая заодно уборку.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Policy {
    /// Сколько сегментов сливать за один раз.
    ///
    /// Больше — реже слияния и меньше сегментов в итоге, но каждое слияние
    /// дольше и дороже по памяти страниц. Восемь — середина: столько же берёт
    /// Lucene по умолчанию, и на нашей нарезке это два-три захода до потолка.
    pub at_once: usize,
    /// Потолок слитого сегмента в документах.
    pub max_docs: usize,
}

impl Default for Policy {
    fn default() -> Self {
        Policy { at_once: 8, max_docs: 50_000_000 }
    }
}

/// Плоский выбор окна: полуинтервал в порядке списка, мера — документы.
///
/// Планировщик по умолчанию теперь другой — [`plan_tiered`], и мерит он байты.
/// Этот остался целиком, потому что он же и вход разового `svindb merge`, где
/// ярусы не нужны: там сливают до упора и под замком.
///
/// Сливаются **соседние** сегменты, а не произвольные. Порядок в списке — это
/// порядок приёма, то есть и порядок возраста, а возраст на нашей нарезке
/// совпадает с размером. Соседство заодно сохраняет смысл нумерации: слитый
/// встаёт на место своих исходных, а не куда придётся.
///
/// Выбирается окно с **наибольшим числом сегментов**, укладывающееся в потолок;
/// при равенстве — с наименьшим числом документов, то есть самое дешёвое.
///
/// `orders` — устойчивые номера полей каждого сегмента в его порядке. Окно
/// расширяется, только пока у набранных сегментов существует **общий порядок**
/// полей: без него [`merge`] отказывает, а отказ на каждом заходе означал бы
/// вставшее обслуживание. Прецедент записан рядом: пустой сегмент однажды уже
/// валил все 424 захода подряд.
///
/// Разный **состав** полей окну не мешает — слияние умеет их объединять.
/// Мешает противоречие в **порядке**, а оно возможно, только если одни и те же
/// поля объявили в двух сегментах наоборот.
///
/// Подписей у вызывающего может не быть — тогда они считаются здесь. У того, кто
/// зовёт планировщик каждые две секунды, они есть готовыми в открытом сегменте
/// ([`crate::index::Index::schema_sig`]), и ему нужен [`plan_flat`].
pub fn plan(sizes: &[usize], orders: &[Vec<u32>], p: Policy) -> Option<(usize, usize)> {
    let sigs: Vec<u64> = orders.iter().map(|o| schema_sig(o)).collect();
    plan_flat(sizes, orders, &sigs, p)
}

/// То же, но с готовыми подписями схем — по одной на сегмент.
///
/// ## Что даёт подпись
///
/// Прежде на каждом шаге каждого окна звался [`field_order`]: полное построение
/// общего порядка ради одного бита «он существует». Сортировка квадратична по
/// узлам, узлов в окне до `at_once * F`, а построений — до `at_once` на сегмент:
/// на десяти тысячах сегментов, окне в восемь и двадцати полях это порядка двух
/// миллиардов действий за заход, то есть каждые две секунды и под замком состава.
///
/// Пока схема у всех сегментов окна **одна**, общий порядок известен без всякой
/// сортировки: это она сама. Сравнение подписей отвечает на «одна ли» за одно
/// действие, и [`field_order`] остаётся только для окон, где схемы правда
/// разошлись, — то есть в здоровом индексе не зовётся ни разу.
///
/// Взамен на сегмент приходится сравнение схем на каждый шаг окна (`O(F)`) и
/// одна проверка её самосогласованности (`O(F^2)`): на тех же числах — пять с
/// небольшим миллионов действий вместо двух миллиардов.
///
/// ## Почему совпадение подписей ещё подтверждается
///
/// Подпись — хеш, и требовать от него отсутствия столкновений значит поставить
/// правильность выбора в зависимость от вероятности. Несовпадение подписей —
/// окончательный ответ «схемы разные», а совпадение подтверждается равенством
/// самих векторов: сравнение `O(F)` там, где иначе строился бы весь порядок, и
/// ответ при этом точный, а не вероятный.
///
/// Выбор от этого не меняется ни в одном случае: [`plan_flat`] обязан давать
/// ровно то же окно, что дал бы прежний обход с [`field_order`] на каждом шаге.
/// Это не смена политики, а снятие квадратичности.
///
/// `sigs` короче `orders` — недостающие считаются неизвестными, и схемы
/// сравниваются самими векторами. Молчаливого «сойдёт» тут нет.
pub fn plan_flat(
    sizes: &[usize],
    orders: &[Vec<u32>],
    sigs: &[u64],
    p: Policy,
) -> Option<(usize, usize)> {
    let mut best: Option<(usize, usize, usize)> = None; // (сколько, всего док, начало)
    for from in 0..sizes.len() {
        let mut total = 0usize;
        let mut count = 0usize;
        // Одна ли схема у всего набранного окна. Пока да — общий порядок есть
        // по построению, и звать сортировку не за чем.
        let mut alike = true;
        // Ответ [`distinct`] про схему начала окна. Лениво и один раз на окно:
        // нужен он только быстрому пути, а начало окна по нему не меняется.
        let mut alone: Option<bool> = None;
        for (at, size) in sizes[from..].iter().enumerate().take(p.at_once) {
            // Общего порядка полей больше нет — окно кончилось. Сегменты за
            // границей сольются между собой своим окном.
            if !orders.is_empty() {
                let last = orders.len() - 1;
                let (lo, hi) = (from.min(last), (from + at).min(last));
                // Всё до `hi` уже сверено с `lo`, поэтому сравнивается только
                // вновь взятый сегмент, а не окно целиком.
                if alike && at > 0 {
                    alike = same_schema(orders, sigs, lo, hi);
                }
                let fits = if alike {
                    *alone.get_or_insert_with(|| distinct(&orders[lo]))
                } else {
                    field_order(&orders[lo..=hi]).is_some()
                };
                if !fits {
                    break;
                }
            }
            if total.saturating_add(*size) > p.max_docs {
                break;
            }
            total += size;
            count += 1;
        }
        if count < 2 {
            continue;
        }
        let better = match best {
            None => true,
            Some((c, t, _)) => count > c || (count == c && total < t),
        };
        if better {
            best = Some((count, total, from));
        }
    }
    best.map(|(count, _, from)| (from, from + count))
}

/// Одна ли схема у сегментов `a` и `b`.
///
/// Подпись отсекает несовпадение за одно сравнение; совпадение подтверждается
/// самими векторами, потому что от хеша не требуется отсутствия столкновений и
/// выбор окна не должен от него зависеть. Подписи нет — сравниваются векторы,
/// ответ тот же, только дороже.
fn same_schema(orders: &[Vec<u32>], sigs: &[u64], a: usize, b: usize) -> bool {
    if let (Some(x), Some(y)) = (sigs.get(a), sigs.get(b)) {
        if x != y {
            return false;
        }
    }
    orders[a] == orders[b]
}

/// Годится ли схема сама с собой.
///
/// ## Почему это отдельный вопрос
///
/// Окно из одинаковых схем совместимо не всегда: повторённый устойчивый номер
/// внутри одной схемы даёт путь из номера в него же, то есть цикл, и
/// [`field_order`] отказывает уже на **одном** таком источнике. Прежний обход
/// звал сортировку и на окне из одного, поэтому такой сегмент в слияние не
/// попадал; быстрый путь обязан отвечать так же, иначе оптимизация тихо
/// поменяла бы политику.
///
/// Заголовок сегмента такого не допускает — `index::load` требует уникальности
/// номеров полей, — но планировщик чистая функция, и разбираться, кто его
/// позвал, ему нечем.
fn distinct(fids: &[u32]) -> bool {
    fids.iter().enumerate().all(|(i, f)| !fids[..i].contains(f))
}

// ---------------------------------------------------------------------------
// Ярусная политика: потолок сегмента в байтах
// ---------------------------------------------------------------------------

/// Что политика знает о сегменте. Всё берётся у уже открытого [`Index`].
///
/// ## Почему числа именно эти
///
/// Ярусу нужен **размер**, и он один — `bytes`, длина всего каталога. Остальные
/// шесть длин нужны не ярусу, а потолку четырёх гигабайт: смещения внутри
/// сегмента у нас тридцатидвухбитные, и упирается в них не «сегмент вообще», а
/// первым — какая-то одна из этих частей. Перечень тот же, что у
/// [`crate::sums::Bytes`] и у [`fits_offset`]: там отказ, здесь мера, по которой
/// до отказа не доводят.
///
/// `docs` и `dead` — вторая мера окна (прежний `target_docs`) и делитель живого
/// размера. `sig` и `order` — схема: без общего порядка полей слияние
/// отказывает, и окно обязано это знать заранее.
///
/// ## Почему порядок полей едет целиком, а не одной подписью
///
/// Подпись — хеш, и совпадение её ещё ничего не доказывает; несовпадение же не
/// значит «сливать нельзя»: схема, у которой **дописали** поле, с прежней
/// прекрасно сливается. И то и другое решается только самими векторами, и
/// плоский планировщик их уже возит ([`same_schema`], [`field_order`]). Вектор
/// на сегмент — те же двадцать чисел, что заход берёт у `field_ids` и сейчас,
/// так что дороже не стало.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Seg {
    /// Весь каталог сегмента. Это и есть его размер для яруса.
    pub bytes: u64,
    /// `postings.bin` — списки документов.
    pub postings: u64,
    /// `pos.bin` — позиции слов.
    pub positions: u64,
    /// `terms.bin` — записи слов.
    pub terms: u64,
    /// Наибольший `<поле>.sbin` среди хранимых полей.
    pub stored_max: u64,
    /// `ids.bin` — идентификаторы документов.
    pub ids: u64,
    /// `dict.blob` — тела слов словаря.
    pub dictionary: u64,
    /// Сколько документов в сегменте, живых и погашенных вместе.
    pub docs: usize,
    /// Сколько групп (массивов) сверх корневой.
    pub groups: u64,
    /// Единиц самой широкой группы: сколько элементов массива всего.
    ///
    /// У схемы без массивов это то же число, что и документов, — и именно
    /// поэтому его когда-то не завели отдельно. Разойтись они могут только на
    /// поле внутри массива, а там расходятся сразу в десятки раз: сборка
    /// колонки заводит буферы на **каждый элемент**, а не на документ.
    pub units: u64,
    /// Наибольший typed multivalue stream поля. Его raw buffers живут рядом
    /// со scalar column и не ограничиваются числом документов/слотов.
    pub multi_values: u64,
    /// Сколько из них погашено.
    pub dead: usize,
    /// Подпись схемы: быстрый ответ «схемы разные».
    pub sig: u64,
    /// Устойчивые номера полей в порядке объявления.
    pub order: Vec<u32>,
}

impl Seg {
    /// Снимает всё нужное с открытого сегмента.
    pub fn of(ix: &Index) -> Seg {
        Seg {
            // Самая широкая группа, а не сумма по группам: колонки собираются
            // по одному полю за раз, и буферы предыдущего освобождаются до
            // начала следующего. В пик входит наибольшее, а не всё сразу.
            groups: ix.slots.len().saturating_sub(1) as u64,
            units: ix
                .slots
                .iter()
                .map(|s| s.units() as u64)
                .max()
                .unwrap_or(ix.docs as u64)
                .max(ix.docs as u64),
            multi_values: ix
                .fields
                .iter()
                .map(|field| field.multi_values as u64)
                .max()
                .unwrap_or(0),
            bytes: ix.bytes.total,
            postings: ix.bytes.postings,
            positions: ix.bytes.positions,
            terms: ix.bytes.terms,
            stored_max: ix.bytes.stored_max,
            ids: ix.bytes.ids,
            dictionary: ix.bytes.dictionary,
            docs: ix.docs,
            dead: ix.dead(),
            sig: ix.schema_sig,
            order: field_ids(ix),
        }
    }

    /// Сколько документов сегмента живы.
    pub fn live_docs(&self) -> usize {
        self.docs.saturating_sub(self.dead)
    }

    /// Всё, что не названо отдельными частями: содержимое, колонки, нормы.
    ///
    /// Отдельной величины «сколько занято хранимым» у сегмента нет —
    /// `sums::Bytes` называет наибольший `.sbin`, а не сумму, — и это остаток от
    /// вычитания. В него попадают заодно колонки и нормы; они мельче
    /// содержимого, и завышение здесь безвредно, а занижение — нет: по этому
    /// числу считается память слияния.
    pub fn stored_rest(&self) -> u64 {
        let named = self
            .postings
            .saturating_add(self.positions)
            .saturating_add(self.terms)
            .saturating_add(self.ids)
            .saturating_add(self.dictionary);
        self.bytes.saturating_sub(named)
    }

    /// Размер по **живому**: байты, помноженные на долю живых документов.
    ///
    /// ## Зачем не длина каталога
    ///
    /// Ровно так меряет `TieredMergePolicy::size`, и польза здесь не в точности,
    /// а в **достижимости**. Сегмент, из которого погасили девять десятых,
    /// занимает на диске столько же, сколько занимал, и по длине каталога
    /// остаётся в верхнем ярусе — то есть не сливается ни с кем и мёртвое из
    /// него не уходит никогда. По живому он падает в тот ярус, которому и
    /// соответствует, и возвращается в оборот сам, без отдельного правила про
    /// грязные сегменты.
    ///
    /// Оценка это, а не измерение: сжатые списки от выброса мёртвых сокращаются
    /// не строго пропорционально. Для яруса приблизительности хватает — он и
    /// сам про порядок величины, — а вот потолку четырёх гигабайт не хватает, и
    /// туда идут длины как есть. См. [`Window::fits`].
    pub fn live(&self) -> u64 {
        if self.dead == 0 || self.docs == 0 {
            return self.bytes;
        }
        // В u128, потому что пять гигабайт на пятьдесят миллионов документов —
        // это 2.7e17, и до потолка u64 тут ближе, чем хотелось бы полагаться.
        ((self.bytes as u128 * self.live_docs() as u128) / self.docs as u128) as u64
    }

    /// Размер с полом яруса: мельче пола сегменты не различаются.
    ///
    /// Пол нужен ровно затем, чтобы россыпь крошечных сегментов не считалась
    /// «сбалансированным окном» и не растаскивалась по разным ярусам: ниже пола
    /// разница в размерах перестаёт что-либо значить, а перекос окна на ней
    /// считался бы всерьёз.
    pub fn floored(&self, t: &Tier) -> u64 {
        self.live().max(t.floor_bytes)
    }

    /// Пик пары этого сегмента с таким же по форме соседом.
    fn twin_peak(&self) -> u64 {
        peak_of(Shape {
            src_docs: (self.docs as u64).saturating_mul(2),
            any_dead: self.dead > 0,
            groups: self.groups,
            out_docs: (self.live_docs() as u64).saturating_mul(2),
            units: self.units.saturating_mul(2),
            multi_values: self.multi_values.saturating_mul(2),
            stored: self.stored_rest().saturating_mul(2),
            // Заголовок открытого сегмента уже проверил уникальность
            // номеров полей; пересобирать множество на каждом заходе
            // обслуживания означало бы O(segments * fields) аллокаций.
            fields: self.order.len() as u64,
        })
    }

    /// Заморожен ли сегмент — то есть выведен ли он из укрупнения насовсем.
    ///
    /// ## Почему половина потолка, а не потолок
    ///
    /// Это правило Lucene, но потолков у окна теперь три: байты, документы и
    /// память. Следствие у них одно: достижимый размер сегмента — около
    /// половины соответствующего потолка. Сегмент, чья пара с таким же уже не
    /// влезает, нельзя оставлять обычным участником яруса.
    ///
    /// Ошибка здесь измерена на настоящем 1 ГиБ cgroup. Сегмент в 350 тысяч
    /// документов уже не мог слиться ни с одним равным: пара просила 383 МиБ
    /// при бюджете 240 МиБ. Но `frozen` смотрел только на пятигигабайтный
    /// потолок файла. Поэтому каждый новый 50-тысячный сегмент присоединялся к
    /// цепочке 100k → 150k → … → 350k, после чего заводилась следующая цепочка.
    /// Все документы переписывались многократно, write amplification вырос до
    /// 7.386, а достигшие потолка сегменты всё равно считались долгом.
    ///
    /// Память считается той же формулой, которой окно проверяется перед
    /// запуском. Для одинакового соседа число полей и групп не удваивается, а
    /// документы, единицы и содержимое удваиваются. Если даже такая пара не
    /// проходит, сегмент становится отдельным замороженным членом состава и не
    /// заставляет ярус сливать следующую мелочь после каждого сброса.
    ///
    /// Обратное правило — «морозим на самом потолке» — стоило бы полного
    /// перебора окон с этим сегментом на каждом заходе и не дало бы ни одного
    /// слияния сверх нынешних.
    pub fn frozen(&self, t: &Tier) -> bool {
        let memory_frozen = if t.max_peak == 0 { false } else { self.twin_peak() > t.max_peak };
        self.live() >= t.max_merged / 2
            || self.live_docs().saturating_mul(2) >= t.max_docs
            || memory_frozen
    }
}

/// Ярусные пределы: чем ограничено укрупнение и когда индекс считается
/// перегруженным.
///
/// ## Зачем ярусы вообще
///
/// Замерено на этой машине, не оценка: сегмент стоит запросу 3.2 мкс на двух
/// словах, 8.2 мкс на маске и 57.9 мкс на сортировке по числу — **до** всякой
/// полезной работы. При плоском потолке в миллион документов десять миллиардов
/// документов дают десять тысяч сегментов, то есть 32 и 82 миллисекунды
/// накладных на запрос; сто миллиардов — сто тысяч сегментов и 320 миллисекунд.
/// Вдобавок сегмент занимает 22 области отображения, а умолчание
/// `vm.max_map_count` в Linux — 65 530: это жёсткая стена примерно на трёх
/// миллиардах документов. Цель же — десятки и сотни миллиардов в одном индексе
/// на одной машине, и упирается она не в объём, а в **число** сегментов.
///
/// Число документов мерой быть не может: в живом корпусе хвост тяжелее середины
/// в несколько раз, и одинаковые по документам сегменты отличаются по байтам на
/// порядок. Отсюда байты.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Tier {
    /// Сколько сегментов сливать за один заход.
    pub at_once: usize,
    /// Сколько сегментов позволено одному ярусу.
    ///
    /// Ниже [`at_once`](Tier::at_once) не опускается: ярус, которому позволено
    /// меньше, чем берёт одно слияние, требовал бы слияния сразу после слияния —
    /// то есть той самой бури, против которой и заведён
    /// [`allowed_seg_count`].
    pub per_tier: usize,
    /// Пол яруса: ниже него размеры не различаются.
    pub floor_bytes: u64,
    /// Потолок слитого сегмента в байтах.
    ///
    /// Достижимый размер — около **половины** этого числа, см. [`Seg::frozen`].
    pub max_merged: u64,
    /// Второй потолок — в живых документах. Прежний `target_docs`.
    ///
    /// Байты не отменяют его: перенумерация внутри сегмента тридцатидвухбитная,
    /// и запас по документам стоит держать явным, а не выводить из плотности
    /// корпуса, которая у каждого своя.
    pub max_docs: usize,
    /// Насколько сильно оценка окна тянется к возвращаемому месту.
    ///
    /// Двойка — как `reclaimDeletesWeight` у Lucene: окно из наполовину мёртвых
    /// сегментов оказывается вчетверо привлекательнее такого же чистого.
    pub reclaim_weight: f64,
    /// Насколько дороже несмежное окно. Разбор — у [`score`].
    pub spread_penalty: f64,
    /// Сколько памяти позволено занять слиянию этого окна.
    ///
    /// Третий потолок рядом с байтами и документами, и по той же причине, что
    /// они: окно, который узел не сможет собрать, не должен быть выбран. Разница
    /// в том, что байты и документы стерегут **формат**, а этот — саму машину:
    /// слияние, не влезшее в память, не отказывает, а гибнет от ядра посреди
    /// работы.
    ///
    /// Не влезло — окно просто короче. Сегментов в индексе станет больше, и
    /// каждый запрос заплатит за них примерно по микросекунде; это дешевле
    /// остановки узла и много дешевле его смерти.
    ///
    /// Ноль — предела нет. Так собирается [`Tier::default`]: пределы политики
    /// приходят из настроек, а память — от узла, и подставлять сюда догадку
    /// значило бы решать за него.
    pub max_peak: u64,
}

impl Default for Tier {
    fn default() -> Self {
        Tier {
            at_once: 8,
            // Десять — умолчание Lucene (`segsPerTier`). Больше — сегментов в
            // индексе больше, и каждый запрос платит за них; меньше — слияния
            // идут чаще, чем окупаются.
            per_tier: 10,
            // Два мегабайта — тоже умолчание Lucene. На нашей плотности (73
            // байта на документ, замерено) это порядка тридцати тысяч
            // документов: ниже этого разница в размерах сегментов ничего не
            // значит.
            floor_bytes: 2 * 1024 * 1024,
            // Пять гигабайт — умолчание Lucene, и оно же примерно наш предел по
            // другой причине: достижимый размер вдвое меньше, то есть 2.5 ГБ, а
            // это около 34 миллионов документов на нашей плотности — с запасом
            // под потолком четырёх гигабайт на каждую часть сегмента.
            max_merged: 5 * 1024 * 1024 * 1024,
            max_docs: 50_000_000,
            reclaim_weight: 2.0,
            // Одна десятая: окно, пропустившее по десять чужих сегментов на
            // каждого своего, обязано быть вдвое лучше по перекосу, чтобы
            // победить смежное. Запрета на несмежность нет — есть цена.
            spread_penalty: 0.1,
            max_peak: 0,
        }
    }
}

/// Какой политикой укрупнять индекс.
///
/// Настройка, а не догадка: у оператора обязан быть способ сказать ярусному
/// укрупнению «не сейчас», не выключая заодно уборку, — а `--no-maintain`
/// выключает обе.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MergePolicy {
    /// Ярусная: [`plan_tiered`].
    Tiered(Tier),
    /// Та же корректная ярусная модель, но с широким окном мелких сегментов.
    ///
    /// Профиль предназначен для индексов, где короткие update/delete пачки
    /// появляются быстрее обычного фонового прохода. Отдельный вариант нужен,
    /// чтобы `/_settings` честно возвращал выбранный профиль, а не слово
    /// `tiered` после первого же round-trip.
    UpdateHeavy(Tier),
    /// Плоская, прежняя: [`plan_flat`]. Свои пределы несёт с собой — по той же
    /// причине, по которой их несёт ярусная: слово настройки их не знает, а
    /// подставить умолчание вместо настроенного значило бы молча слить весь
    /// индекс в один сегмент.
    Flat(Policy),
}

impl Default for MergePolicy {
    fn default() -> Self {
        MergePolicy::Tiered(Tier::default())
    }
}

/// Чем укрупнять — без пределов, только выбор.
///
/// ## Почему отдельно от [`MergePolicy`]
///
/// `MergePolicy` носит пределы, и это верно там, где политику применяют. Но в
/// файле настроек пределы живут **соседними ключами** (`max_merged_mb`,
/// `per_tier`, `target_docs`), и держать их ещё и внутри выбора значило бы
/// хранить одно число в двух местах. Круг «прочитать настройки — записать
/// обратно» на этом и не замыкался: наружу уходило слово, обратно возвращались
/// умолчания, и прочитанное перестало равняться записанному.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum MergeKind {
    #[default]
    Tiered,
    UpdateHeavy,
    Flat,
}

impl MergeKind {
    /// Слово настройки. Обратное к [`MergeKind::parse`].
    pub fn word(&self) -> &'static str {
        match self {
            MergeKind::Tiered => "tiered",
            MergeKind::UpdateHeavy => "update_heavy",
            MergeKind::Flat => "flat",
        }
    }

    /// Разбирает слово. `None` — слово не наше.
    ///
    /// Разбор здесь, а не у читателя настроек, затем, что читателей двое — файл
    /// настроек и переменная среды, — и слова у них обязаны быть одни и те же.
    pub fn parse(word: &str) -> Option<MergeKind> {
        match word.trim() {
            "tiered" => Some(MergeKind::Tiered),
            "update_heavy" | "update-heavy" => Some(MergeKind::UpdateHeavy),
            "flat" => Some(MergeKind::Flat),
            _ => None,
        }
    }
}

impl MergePolicy {
    /// Разбирает слово настройки. `None` — слово не наше.
    ///
    /// Разбор здесь, а не у того, кто читает настройки, затем, что читателей
    /// двое — файл настроек и переменная среды, — и слова у них обязаны быть
    /// одни и те же. Пределы ярусов слово не несёт: они приходят от настроек
    /// обслуживания, а `tiered` значит «ярусами с этими пределами».
    /// Какой это вид, без пределов.
    pub fn kind(&self) -> MergeKind {
        match self {
            MergePolicy::Tiered(_) => MergeKind::Tiered,
            MergePolicy::UpdateHeavy(_) => MergeKind::UpdateHeavy,
            MergePolicy::Flat(_) => MergeKind::Flat,
        }
    }

    pub fn parse(word: &str) -> Option<MergePolicy> {
        match word.trim() {
            "tiered" => Some(MergePolicy::Tiered(Tier::default())),
            "update_heavy" | "update-heavy" => Some(MergePolicy::UpdateHeavy(Tier {
                at_once: 32,
                per_tier: 32,
                floor_bytes: 16 * 1024 * 1024,
                reclaim_weight: 3.0,
                ..Tier::default()
            })),
            "flat" => Some(MergePolicy::Flat(Policy { at_once: 8, max_docs: 1_000_000 })),
            _ => None,
        }
    }
}

/// Какое окно слить — той политикой, которую попросили.
///
/// Один вход на обе, и это не украшение. Ярусная отдаёт **набор номеров**, а не
/// отрезок: окно у неё группируется по размеру, а размеры в списке лежат
/// вперемешку. Плоская отдаёт отрезок. Сведи их у вызывающего — и он выбирал бы
/// политику дважды, в двух ветвях, которые однажды разойдутся.
///
/// Номера всегда возрастают: публикация вырезает источники по ним и ставит
/// слитый на место самого раннего, чтобы список остался возрастным по возрасту,
/// а `list[0]` — самым старым сегментом. От него планировщик запроса берёт
/// состав полей, и подмена первого на молодой изменила бы ответ о похороненном
/// поле.
pub fn plan_by(segs: &[Seg], policy: &MergePolicy) -> Option<Vec<usize>> {
    plan_by_why(segs, policy, &mut NoPlan::TooFew)
}

/// Причина, по которой окно укрупнения не сложилось.
///
/// ## Зачем перечисление, а когда-то хватало `None`
///
/// «Окна нет» снаружи выглядело одинаково у двух состояний, которые лечатся
/// прямо противоположно. Первое: состав в пределах допустимого — всё хорошо,
/// сливать нечего, и через минуту будет то же самое. Второе: долг есть, но
/// работа не берётся — по памяти, по потолку смещений, по несводимым схемам.
/// Второе само не проходит **никогда**, а выглядело как первое.
///
/// Порядок вариантов — порядок тяжести: из причин, встреченных при переборе
/// окон, наружу уходит наибольшая. Несовместимость схем пройдёт сама, как
/// только соседи сольются; нехватка памяти не пройдёт.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum NoPlan {
    /// Сегментов меньше двух — сливать физически нечего.
    #[default]
    TooFew,
    /// Состав в пределах допустимого: долга нет.
    WithinAllowed,
    /// Ни одна пара не сводится к общему порядку полей.
    SchemaSplit,
    /// Окно упирается в потолок размера или числа документов.
    OverSize,
    /// Все свободные сегменты выведены из укрупнения насовсем.
    AllFrozen,
    /// Окно не влезает в потолок смещений внутри сегмента.
    OverOffset,
    /// Окно не влезает в отведённую слиянию память.
    OverPeak { need: u64, have: u64 },
}

pub fn plan_by_why(segs: &[Seg], policy: &MergePolicy, why: &mut NoPlan) -> Option<Vec<usize>> {
    match policy {
        MergePolicy::Tiered(t) | MergePolicy::UpdateHeavy(t) => plan_tiered_why(segs, t, why),
        MergePolicy::Flat(p) => {
            // Плоской нужны только размеры в документах и схемы: байты она не
            // знает вовсе, потому и заменяется.
            let sizes: Vec<usize> = segs.iter().map(|s| s.docs).collect();
            let orders: Vec<Vec<u32>> = segs.iter().map(|s| s.order.clone()).collect();
            let sigs: Vec<u64> = segs.iter().map(|s| s.sig).collect();
            let out = plan_flat(&sizes, &orders, &sigs, *p);
            if out.is_none() {
                // У плоской политики потолков памяти нет вовсе: она знает
                // только число документов и схемы. Значит и причин у неё две.
                *why = if segs.len() < 2 { NoPlan::TooFew } else { NoPlan::WithinAllowed };
            }
            out.map(|(from, to)| (from..to).collect())
        }
    }
}

/// Сколько сегментов индексу вообще позволено иметь.
///
/// ## Это главный тормоз против бури слияний
///
/// Без явной границы «индекс не перегружен» политика сливает всегда: окно из
/// двух сегментов находится в любом наборе, и заход за заходом уходил бы на
/// слияния, которые ничего не улучшают. Граница отвечает на это одним числом:
/// пока сегментов не больше позволенного, укрупнять **нечего**, и заход
/// достаётся уборке.
///
/// Считается как у `TieredMergePolicy`: индекс мысленно раскладывается по
/// ярусам от самого мелкого сегмента вверх, каждому ярусу позволено `per_tier`
/// сегментов, ярус выше вмещает вдвое-впятеро больше — во столько раз, во
/// сколько сегментов берёт одно слияние.
///
/// ## Два отличия от Lucene, оба намеренные
///
/// Первое: `total` складывается из размеров **с полом**, а у Lucene — из
/// настоящих, при том что уровень он тоже берёт с полом. Смешивать их незачем:
/// ярус ниже пола один, и считать его надо целиком в одних единицах. Плата —
/// россыпи мельче пола позволено немного больше сегментов, чем у Lucene;
/// выигрыш — число не зависит от того, насколько мелка мелочь.
///
/// Второе: замороженные сегменты не участвуют в раскладке, но каждый добавляет
/// к позволенному единицу. Иначе индекс, честно доросший до дюжины предельных
/// сегментов, считался бы перегруженным навсегда — а слить их всё равно нечем.
///
/// ## Почему ярусы считаются сегментами, а не делением байт на уровень
///
/// Прежде уровень брался как размер **самого мелкого** сегмента, а из общей
/// суммы байт на каждом ярусе вычиталось `per_tier * уровень`. Пока сегменты
/// одного порядка, это то же самое. Но стоило появиться россыпи мелочи — и
/// уровень падал до неё, а сумма оставалась большой, потому что в неё входят
/// байты крупных. Цикл выдавал ярус за ярусом по полному `per_tier`, вычитая
/// каждый раз сущую мелочь, и позволенное раздувалось втрое.
///
/// Числа не выдуманы, они воспроизводят наблюдённое. Одиннадцать сегментов по
/// 144.5 МБ дают позволенных 11 — это состав сразу после чистой заливки. Добавь
/// к ним тринадцать сегментов по три мегабайта, то есть два с половиной
/// процента индекса, — и позволенных становится 28. Индекс из двадцати четырёх
/// сегментов объявляется «в пределах допустимого», укрупнение не идёт, и
/// состав так и остаётся вдвое дробнее посевного. Запрос платит за это на
/// каждом сегменте, и само это не рассасывается никогда.
///
/// Теперь ярус — это **отрезок размеров**, а не отрезок байт: сегменты
/// раскладываются по отрезкам `[низ, низ * at_once)` от самого мелкого вверх, и
/// каждому отрезку позволено не больше `per_tier` **сегментов, которые в нём
/// правда лежат**. Мелочь остаётся в своём ярусе и на позволенное крупным не
/// влияет.
pub fn allowed_seg_count(segs: &[Seg], t: &Tier) -> f64 {
    let per_tier = t.per_tier.max(t.at_once).max(2);
    let at_once = (t.at_once.max(2) as f64).max(2.0);
    let mut allowed = 0f64;
    let mut sizes: Vec<f64> = Vec::with_capacity(segs.len());
    for s in segs.iter() {
        if s.frozen(t) {
            allowed += 1.0;
            continue;
        }
        // Пол яруса может быть и нулём — его выставляет вызывающий. Ноль тут
        // значил бы ярус нулевой ширины, поэтому размер не ниже байта.
        sizes.push((s.floored(t) as f64).max(1.0));
    }
    if sizes.is_empty() {
        return allowed;
    }
    sizes.sort_by(|a, b| a.total_cmp(b));
    let ceiling = (t.max_merged.max(1) as f64).max(sizes[sizes.len() - 1]);
    let mut at = 0usize;
    let mut low = sizes[0];
    while at < sizes.len() {
        // Пустой ярус не тратит оборота: низ прыгает сразу к ближайшему
        // сегменту. Без этого россыпь у пола и одиночный гигант заставляли бы
        // крутить десятки пустых ярусов.
        if sizes[at] >= low * at_once {
            low = sizes[at];
        }
        let top = low * at_once;
        let mut n = 0usize;
        while at < sizes.len() && (sizes[at] < top || low >= ceiling) {
            n += 1;
            at += 1;
        }
        allowed += n.min(per_tier) as f64;
        if low >= ceiling {
            break;
        }
        low = top;
    }
    allowed
}

#[cfg(test)]
mod allowed_tests {
    use super::*;

    fn seg(bytes: u64) -> Seg {
        Seg { bytes, docs: (bytes / 128).max(1) as usize, ..Seg::default() }
    }

    fn tier() -> Tier {
        Tier { at_once: 8, per_tier: 10, floor_bytes: 2 << 20, ..Tier::default() }
    }

    /// Россыпь мелочи не поднимает позволенный состав втрое.
    ///
    /// Это и есть арифметика наблюдённого: одиннадцать сегментов после чистой
    /// заливки и двадцать с лишним после churn — на одном и том же объёме. Пока
    /// ярус считался делением общей суммы байт на размер самого мелкого
    /// сегмента, тринадцать сегментов по три мегабайта — два с половиной
    /// процента индекса — раздували позволенное с одиннадцати до двадцати
    /// восьми, и укрупнение честно молчало: состав ведь «в пределах».
    #[test]
    fn a_dusting_of_small_segments_does_not_triple_the_allowed_count() {
        let t = tier();
        let big: Vec<Seg> = (0..11).map(|_| seg(144_500_000)).collect();
        let clean = allowed_seg_count(&big, &t);
        assert!(
            (10.0..=11.0).contains(&clean),
            "одиннадцать равных сегментов дают позволенных {clean}"
        );

        let mut dusted = big.clone();
        dusted.extend((0..13).map(|_| seg(3_000_000)));
        let after = allowed_seg_count(&dusted, &t);
        assert!(
            after < clean + 11.0,
            "мелочь весом 2.5% индекса подняла позволенное с {clean} до {after}"
        );
        assert!(
            (dusted.len() as f64) > after,
            "состав из {} сегментов обязан считаться перегруженным, а позволено {after}",
            dusted.len()
        );
    }

    /// Каждому ярусу позволено не больше `per_tier`, и мелочь не занимает чужого.
    #[test]
    fn each_rung_is_capped_by_the_segments_that_really_lie_in_it() {
        let t = tier();
        // Ярус — отрезок [низ, низ * at_once). При at_once = 8 и поле 2 МиБ это
        // [2, 16), [16, 128), [128, 1024) мегабайт. Размеры ниже выбраны по
        // этой раскладке, а не на глаз.
        let ten: Vec<Seg> = (0..10).map(|_| seg(32 << 20)).collect();
        assert_eq!(allowed_seg_count(&ten, &t), 10.0, "один ярус на пределе");
        // Второй ярус — мелочь у пола. Своё, а не за счёт крупного.
        let mut two = ten.clone();
        two.extend((0..10).map(|_| seg(2 << 20)));
        assert_eq!(allowed_seg_count(&two, &t), 20.0, "два полных яруса дают двадцать");
        // Сорок сегментов в одном ярусе — всё равно десять позволенных.
        let mut three = two.clone();
        three.extend((0..40).map(|_| seg(700 << 20)));
        assert_eq!(
            allowed_seg_count(&three, &t),
            30.0,
            "сорок сегментов одного яруса дали больше десяти"
        );
    }

    /// Замороженные не участвуют в раскладке, но каждый добавляет единицу.
    #[test]
    fn frozen_segments_are_counted_one_by_one() {
        let t = tier();
        let frozen = seg(t.max_merged);
        assert!(frozen.frozen(&t), "сегмент на потолке обязан быть заморожен");
        let only: Vec<Seg> = (0..4).map(|_| seg(t.max_merged)).collect();
        assert_eq!(allowed_seg_count(&only, &t), 4.0, "четыре замороженных — четыре позволенных");
    }

    #[test]
    fn documents_and_memory_are_real_freezing_boundaries_too() {
        let sample = Seg {
            bytes: 20 << 20,
            docs: 100_000,
            units: 100_000,
            order: vec![0, 1],
            ..Seg::default()
        };
        let pair_peak = sample.twin_peak();
        let mut t =
            Tier { max_merged: u64::MAX, max_docs: usize::MAX, max_peak: pair_peak, ..tier() };
        assert!(!sample.frozen(&t), "пара ровно на потолке памяти ещё достижима");
        t.max_peak = pair_peak - 1;
        assert!(sample.frozen(&t), "недостижимая по памяти пара не заморожена");

        t.max_peak = 0;
        t.max_docs = sample.live_docs() * 2;
        assert!(sample.frozen(&t), "половина потолка документов не заморожена");
    }

    #[test]
    fn memory_frozen_segments_do_not_force_incremental_rewrites_of_the_next_tier() {
        let t = Tier {
            at_once: 8,
            per_tier: 10,
            floor_bytes: 64 << 20,
            max_merged: u64::MAX,
            max_docs: usize::MAX,
            max_peak: 64 << 20,
            ..Tier::default()
        };
        let grown = Seg {
            bytes: 20 << 20,
            docs: 100_000,
            units: 100_000,
            order: vec![0],
            ..Seg::default()
        };
        let small =
            Seg { bytes: 1 << 20, docs: 5_000, units: 5_000, order: vec![0], ..Seg::default() };
        assert!(grown.frozen(&t), "крупный участник должен упереться в память");
        assert!(!small.frozen(&t), "мелкий участник обязан оставаться в обороте");

        let mut all = vec![grown; 4];
        all.extend(vec![small; 10]);
        assert_eq!(
            allowed_seg_count(&all, &t),
            14.0,
            "замороженные отняли оборот у следующего яруса и вызовут merge после каждого flush"
        );
    }

    #[test]
    fn a_settled_frozen_composition_is_not_reported_as_blocked() {
        let t = Tier { max_merged: u64::MAX, max_docs: 100_000, max_peak: 0, ..tier() };
        let frozen =
            Seg { bytes: 8 << 20, docs: 60_000, units: 60_000, order: vec![0], ..Seg::default() };
        assert!(frozen.frozen(&t));
        let all = vec![frozen; 12];
        let mut why = NoPlan::TooFew;
        assert!(plan_tiered_why(&all, &t, &mut why).is_none());
        assert_eq!(why, NoPlan::WithinAllowed, "нулевой долг назван блокировкой all_frozen");
    }
}

/// Набранное окно: суммы по всем потолкам сразу.
///
/// Отдельной записью, а не шестью переменными в цикле, потому что проверка
/// «влезает ли» обязана быть **одна**: потолков восемь, и забытый в одном месте
/// — это отказ слияния на девятом часу работы.
#[cfg(test)]
mod peak_estimate_tests {
    use super::*;

    /// Оценка обязана быть **сверху** от настоящего пика — иначе она бесполезна.
    ///
    /// Сверяется с тем же замером, на котором стоит `PEAK_TIMES_STORED`: берётся
    /// настоящий корпус с хранимым, меряется пик, и оценка обязана его накрыть.
    /// Разойдись они — узел, принявший решение по оценке, окажется убит по факту.
    #[test]
    fn the_estimate_covers_the_real_peak() {
        let root = std::env::temp_dir().join(format!("svindb-peak-est-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        let a = peak_tests::seg_stored(&root, "a", 0, 12_000);
        let b = peak_tests::seg_stored(&root, "b", 12_000, 12_000);
        let (ia, ib) = (index::load(&a).expect("a"), index::load(&b).expect("b"));
        let want = peak_estimate(&[&ia, &ib]);

        let out = format!("{root}/out");
        let peak = crate::alloc::peak(|| {
            merge(&[&ia, &ib], &out).expect("слияние");
        });
        assert!(
            (peak as u64) <= want,
            "настоящий пик {peak} Б обогнал оценку {want} Б — узел, поверивший оценке, \
             был бы убит ядром посреди работы"
        );
        // И не вздорно завышена: запас больше сорока раз означал бы, что узел
        // отказывается от работы, которую сделал бы играючи.
        assert!(
            want < peak as u64 * 40,
            "оценка {want} Б при настоящем пике {peak} Б — запас в {} раз",
            want / peak.max(1) as u64
        );
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Постоянный расход слияния, не зависящий от корпуса.
///
/// Буфер списков на четыре мегабайта, потоки приёмника по четверти мегабайта,
/// `pour` на мегабайт и мелочь вокруг. То же число, от которого выбраны размеры
/// в `peak_tests`.
pub const PEAK_FLOOR: u64 = 6 << 20;

/// Байт на документ, **не** зависящие от числа полей.
///
/// Карта перенумерации (четыре байта на исходный документ), фильтр Блума
/// (полтора), карта живых (одна восьмая), отображение идентификаторов и
/// временные буферы обхода. Разбор `inspect` называет только резидентные
/// структуры (фильтр 1.5, живые 0.125), а слияние держит сверх них ещё и свои
/// промежуточные — поэтому величина выведена не из разбора, а из проверки
/// `the_estimate_covers_the_real_peak`: на узкой схеме именно она упирается в
/// это слагаемое, и меньше четырнадцати оценка перестаёт быть верхней границей.
const PEAK_PER_DOC_BASE: u64 = 14;

/// Байт на документ **на каждое поле**.
///
/// Полоса норм — ровно байт на документ на полосу. Прежде этого слагаемого не
/// было вовсе: стояла общая константа в двенадцать байт, выведенная на схеме из
/// **двух** полей. На схеме из восемнадцати `inspect` показывает двадцать байт
/// на документ одних только норм, то есть оценка была занижена почти вдвое
/// именно там, где слияние самое дорогое.
///
/// ## Почему два, а не один
///
/// Полоса норм существует в **двух** копиях одновременно: собранная и её
/// пересборка под новую нумерацию. Обе живы в одном окне, а вторая доживает
/// до самой фазы содержимого и складывается с её пиком. Один байт на поле на
/// документ считал только одну из них.
///
/// Цена ошибки здесь несимметрична: завышение отказывает работе, которая
/// поместилась бы, — и это лечится разделением выхода (`Kind::ReclaimSplit`).
/// Занижение не лечится ничем: узел берётся за работу и снимается ядром
/// посреди неё, вместе со всеми соединениями.
const PEAK_PER_DOC_FIELD: u64 = 2;

/// Во сколько раз пик обгоняет **содержимое**.
///
/// Оценка обязана быть **сверху**: недооценка означает узел, убитый ядром
/// посреди слияния, а переоценка — отказ, у которого есть причина и число. Но
/// переоценка не бесплатна, и это выяснилось дорого.
///
/// Прежде здесь стояло десять — полтора запаса к замеру 5.2–6.6 раза при восьми
/// потоках сжатия. Замер устарел: с тех пор обучающее сырьё стало отпускаться
/// сразу за обучением, а очередь сжатия — ограничиваться байтами. Соседняя
/// проверка `the_merge_peak_with_stored_content_grows_no_faster_than_the_segment`
/// с тех пор сообщает 2.13 и держит порог **четыре**, а константа осталась
/// вдвое с половиной больше того, что разрешает её собственная проверка.
///
/// Цена этого расхождения — не лишний запас, а неработающая уборка. Замороженный
/// сегмент в 2.5 ГиБ требовал по старой формуле около двадцати гигабайт при
/// доступных на стенде семи и семи десятых, то есть уборка не запускалась
/// **никогда**, и мёртвое оставалось в нём навсегда.
///
/// Новое значение выведено измерением, а не желанием. Пятьсот тысяч документов
/// разнородного корпуса, восемнадцать полей, тридцать потоков сжатия: оценка по
/// старой формуле 535.6 МиБ, измеренный пик процесса (`/usr/bin/time -v`,
/// то есть RSS вместе с отображёнными страницами источников — заведомо
/// **сверху**) 213.8 МиБ. Отсюда множитель, при котором формула совпала бы с
/// этой верхней границей, равен 3.9; порог соседней проверки — четыре. Берётся
/// пять: около трети запаса к обоим.
const PEAK_TIMES_STORED: u64 = 6;

/// Сколько байт стоит слиянию одна **единица** самой широкой группы.
///
/// ## Откуда число
///
/// Из кода, а не из подгонки. Сборка колонки держит одновременно (merge.rs,
/// сборка колонок): значение `i64` — восемь байт на единицу; признак наличия
/// `bool` — один; пара «где, что» для разрежённой раскладки `(u32, i64)` —
/// шестнадцать. Итого двадцать пять. Сверху — буферы самого кодировщика
/// колонки (данные, метаданные блоков, битовая карта присутствия), которые
/// добавляют около семи. Тридцать два — округление вверх до степени двойки.
///
/// ## Почему по самой широкой группе, а не по сумме
///
/// Поля обходятся по одному, и буферы предыдущего освобождаются до начала
/// следующего. В пик входит наибольшее, а не всё сразу.
///
/// ## Чего не хватало без этого члена
///
/// Ровно этого. На схеме с группой оценка оказывалась **ниже** измеренного
/// пика уже при арности единица: 7.63 МБ против 7.80 МБ. Оценка ниже пика —
/// это не неточность, это узел, который взялся за работу и был снят ядром
/// посреди неё вместе со всеми соединениями.
const PEAK_PER_UNIT: u64 = 32;

/// Raw values, offsets, рост capacity и одновременно живущий encoded output.
/// Счёт относится к значениям массивов, а не к числу содержащих их документов.
const PEAK_PER_MULTI_VALUE: u64 = 96;

/// Сколько из [`PEAK_PER_DOC_BASE`] приходится на карту перенумерации.
///
/// Ячейка `u32` на каждый исходный документ (merge.rs, заведение `remap`).
/// Отделено от остального, потому что это единственная часть расхода, которой
/// у чистого укрупнения нет: без погашенных номера выходят сдвигом, и таблицы
/// не заводится вовсе.
const PEAK_PER_DOC_REMAP: u64 = 4;

/// Сколько стоит документ выхода на каждую **группу** сверх корневой.
///
/// Полосы арности всех групп строятся разом и живут до конца фазы списков, в
/// отличие от колонок, которые обходятся по одному полю за раз. Четыре байта —
/// ординал `u32` на документ на группу.
const PEAK_PER_DOC_GROUP: u64 = 4;

/// Во что обойдётся слияние этих источников по памяти. Оценка **сверху**.
///
/// ## Зачем оценка, а не учёт по факту
///
/// Тот же довод, что у [`crate::budget`] для поиска: учитывать по факту значит
/// останавливаться посреди работы, когда часть памяти уже взята, а часть выхода
/// уже написана. Оценка до начала даёт отказ, у которого есть причина и число, и
/// не отнимает памяти у соседей.
///
/// ## Откуда берётся содержимое
///
/// Отдельной величины «сколько занято хранимым» у сегмента нет: `sums::Bytes`
/// называет наибольший `.sbin`, а не сумму. Поэтому содержимое считается
/// остатком — всё, что не постинги, не позиции, не слова, не идентификаторы и не
/// словарь. В остаток попадают заодно колонки, нормы и карта живых; они мельче
/// содержимого, и завышение здесь безвредно, а занижение — нет.
///
/// Считается по **источникам**, а не по выходу: выход не больше их суммы, а
/// знать его заранее нельзя.
pub fn peak_estimate(sources: &[&Index]) -> u64 {
    let mut src_docs = 0u64;
    let mut out_docs = 0u64;
    let mut units = 0u64;
    let mut multi_values = 0u64;
    let mut stored = 0u64;
    let mut any_dead = false;
    let mut groups = 0u64;
    let mut fields: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for s in sources.iter() {
        // Два счётчика, а не один, и различие несущее. Карта перенумерации
        // заводит ячейку на **каждый исходный** документ, живой или нет. Полосы
        // норм строятся по **выходу**, то есть только по живым. Считать оба
        // одним числом значит завысить одно или занизить другое.
        src_docs = src_docs.saturating_add(s.docs as u64);
        out_docs = out_docs.saturating_add((s.docs - s.dead()) as u64);
        // Сумма наибольших групп источников — верхняя граница для наибольшей
        // группы выхода: наибольшее из сумм не больше суммы наибольших.
        let shape = Seg::of(s);
        units = units.saturating_add(shape.units);
        multi_values = multi_values.saturating_add(shape.multi_values);
        any_dead |= s.dead() > 0;
        // Групп у выхода столько же, сколько у самого широкого источника:
        // слияние сводит схемы к общему порядку, а не складывает их.
        groups = groups.max(s.slots.len().saturating_sub(1) as u64);
        fields.extend(field_ids(s));
        let b = &s.bytes;
        let named = b
            .postings
            .saturating_add(b.positions)
            .saturating_add(b.terms)
            .saturating_add(b.ids)
            .saturating_add(b.dictionary);
        stored = stored.saturating_add(b.total.saturating_sub(named));
    }
    peak_of(Shape {
        src_docs,
        any_dead,
        groups,
        out_docs,
        units,
        multi_values,
        stored,
        fields: fields.len() as u64,
    })
}

/// Сама формула. Одна на два места: оценку по сегментам и меру окна политики.
///
/// Разойдись они — политика набирала бы окно по одному правилу, а годность его
/// проверялась бы по другому, и объяснить разницу было бы нечем.
/// Во что обойдётся **часть** уборки одного источника на `per` живых документов.
///
/// ## Чем отличается от общей оценки
///
/// Делятся вместе с выходом только те слагаемые, что считаются по выходу:
/// живые документы, корневые единицы, содержимое. Переменная арность не
/// делится по среднему: один документ может содержать все слоты группы.
/// Карта перенумерации не делится
/// вовсе — она заводится на все документы источника при каждом проходе,
/// потому что проход обязан знать про каждый исходный документ, попал он в эту
/// часть или нет.
///
/// Поэтому у дробления есть предел: неделимая часть остаётся как есть, и когда
/// она сама не влезает, дробить бесполезно.
pub fn reclaim_part_peak(src: &Index, per: u64) -> u64 {
    let seg = Seg::of(src);
    let live = (src.docs - src.dead()) as u64;
    let per = per.min(live);
    let share = |v: u64| -> u64 {
        if live == 0 {
            0
        } else {
            v.saturating_mul(per).div_ceil(live)
        }
    };
    peak_of(Shape {
        src_docs: src.docs as u64,
        // Уборка идёт ровно ради погашенных: без них её и не бывает.
        any_dead: true,
        groups: src.slots.len().saturating_sub(1) as u64,
        out_docs: per,
        // Identity имеет ровно один слот на документ. Для Dense metadata
        // содержит лишь сумму, а не максимум арности документа. Поэтому
        // единственная доказанная граница без чтения содержимого — вся группа.
        // Это не оценка по среднему и не разрешение выйти за memory reserve.
        units: if per == 0 {
            0
        } else {
            src.slots
                .iter()
                .map(|slots| match slots {
                    crate::slots::Slots::Identity { .. } => per,
                    crate::slots::Slots::Dense { units, .. } => *units as u64,
                })
                .max()
                .unwrap_or(per)
        },
        // Массивы могут быть сосредоточены в одном документе. Делить V
        // пропорционально числу документов без per-document bound нельзя.
        multi_values: seg.multi_values,
        stored: share(seg.stored_rest()),
        fields: seg.order.len() as u64,
    })
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Shape {
    /// Документы всех источников, **включая погашенные**: по ним заводится
    /// карта перенумерации, и живых для неё мало.
    pub src_docs: u64,
    /// Есть ли среди источников хоть один погашенный документ.
    ///
    /// Карта перенумерации заводится **только** при них: без мёртвых номера
    /// выходят сдвигом, и таблица не нужна вовсе. Считать её всегда значило бы
    /// отказывать чистому укрупнению по расходу, которого у него нет.
    pub any_dead: bool,
    /// Сколько групп (массивов) у выхода сверх корневой.
    ///
    /// Полосы арности живут одновременно у **всех** групп, в отличие от колонок,
    /// которые обходятся по одному полю за раз.
    pub groups: u64,
    /// Живые документы, то есть размер выхода.
    pub out_docs: u64,
    /// Единиц самой широкой группы на выходе.
    pub units: u64,
    pub multi_values: u64,
    /// Содержимое, колонки и нормы источников: всё, что не постинги, не
    /// позиции, не слова, не идентификаторы и не словарь.
    pub stored: u64,
    /// Сколько разных полей встретилось у источников.
    pub fields: u64,
}

pub fn peak_of(s: Shape) -> u64 {
    peak_parts(s).total()
}

/// Из чего складывается оценка. Наружу — чтобы отказ можно было объяснить.
///
/// Одним числом отказ говорит «не влезает», и на этом разговор кончается.
/// Разбивкой он говорит, **чем** не влезает: широкой схемой, массивом,
/// содержимым или просто числом документов. Первые три лечатся настройкой,
/// последнее — только памятью.
#[derive(Clone, Copy, Debug, Default)]
pub struct Peak {
    /// Постоянный расход: буферы ввода-вывода, словарь кодировщика.
    pub floor: u64,
    /// Карта перенумерации: ячейка на каждый исходный документ. Только при мёртвых.
    pub remap: u64,
    /// Полосы норм и арности: по документу выхода на поле и на группу.
    pub bands: u64,
    /// Сборка колонок: по единице самой широкой группы.
    pub columns: u64,
    /// Содержимое: сжатие и перенос хранимых полей.
    pub stored: u64,
}

impl Peak {
    pub fn total(&self) -> u64 {
        self.floor
            .saturating_add(self.remap)
            .saturating_add(self.bands)
            .saturating_add(self.columns)
            .saturating_add(self.stored)
    }
}

/// Какое слагаемое оценки самое крупное. Для отказа, который надо объяснить.
pub fn worst_part(sources: &[&Index]) -> &'static str {
    let mut src_docs = 0u64;
    let mut out_docs = 0u64;
    let mut units = 0u64;
    let mut multi_values = 0u64;
    let mut stored = 0u64;
    let mut any_dead = false;
    let mut groups = 0u64;
    let mut fields: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for s in sources.iter() {
        let seg = Seg::of(s);
        src_docs = src_docs.saturating_add(s.docs as u64);
        out_docs = out_docs.saturating_add((s.docs - s.dead()) as u64);
        units = units.saturating_add(seg.units);
        multi_values = multi_values.saturating_add(seg.multi_values);
        stored = stored.saturating_add(seg.stored_rest());
        any_dead |= s.dead() > 0;
        groups = groups.max(seg.groups);
        fields.extend(field_ids(s));
    }
    let p = peak_parts(Shape {
        src_docs,
        any_dead,
        groups,
        out_docs,
        units,
        multi_values,
        stored,
        fields: fields.len() as u64,
    });
    let named = [
        ("stored", p.stored),
        ("columns", p.columns),
        ("bands", p.bands),
        ("remap", p.remap),
        ("floor", p.floor),
    ];
    named.iter().max_by_key(|(_, v)| *v).map_or("floor", |(n, _)| *n)
}

/// Та же оценка, но по слагаемым.
pub fn peak_parts(s: Shape) -> Peak {
    Peak {
        floor: PEAK_FLOOR,
        // Карта перенумерации — единственное слагаемое, которого у чистого
        // укрупнения нет вовсе: без мёртвых номера выходят сдвигом.
        remap: if s.any_dead {
            s.src_docs.saturating_mul(PEAK_PER_DOC_BASE)
        } else {
            s.src_docs.saturating_mul(PEAK_PER_DOC_BASE - PEAK_PER_DOC_REMAP)
        },
        bands: s
            .out_docs
            .saturating_mul(s.fields.saturating_mul(PEAK_PER_DOC_FIELD))
            .saturating_add(s.out_docs.saturating_mul(s.groups.saturating_mul(PEAK_PER_DOC_GROUP))),
        columns: s
            .units
            .saturating_mul(PEAK_PER_UNIT)
            .saturating_add(s.multi_values.saturating_mul(PEAK_PER_MULTI_VALUE)),
        stored: s.stored.saturating_mul(PEAK_TIMES_STORED),
    }
}

#[derive(Clone, Debug, Default)]
struct Window {
    live: u64,
    bytes: u64,
    floored: u64,
    docs: usize,
    postings: u64,
    positions: u64,
    terms: u64,
    stored_max: u64,
    ids: u64,
    dictionary: u64,
    /// Всё, что не постинги, не позиции, не слова, не идентификаторы и не
    /// словарь: содержимое, колонки, нормы. По нему считается память слияния.
    stored_rest: u64,
    /// Документы всех источников окна, **включая погашенные**: по ним заводится
    /// карта перенумерации, и живых для неё мало.
    src_docs: usize,
    /// Объединение номеров полей окна. Полосы норм стоят байт на документ на
    /// поле, поэтому широкая схема дороже узкой при том же числе документов.
    fields: std::collections::BTreeSet<u32>,
    /// Сумма наибольших групп источников окна.
    units: u64,
    multi_values: u64,
    /// Есть ли среди набранных хоть один погашенный документ.
    any_dead: bool,
    /// Сколько групп (массивов) у самого широкого источника окна.
    groups: u64,
}

impl Window {
    /// Влезет ли сегмент в окно, не перевалив ни за один потолок.
    ///
    /// ## Почему части считаются длинами как есть, а ярус — живым размером
    ///
    /// Потолок четырёх гигабайт — не предпочтение, а отказ: смещение внутри
    /// сегмента тридцатидвухбитное, и слияние, добравшееся до него, бросает
    /// работу целиком. Оценка тут обязана быть **не оптимистичнее** правды, а
    /// живой размер оптимистичен: запись слова и его место в словаре переживают
    /// гашение всех документов, кроме одного, и из потока не уходят. Поэтому
    /// части складываются как лежат на диске — это верхняя граница выхода, —
    /// а ярус и оценка окна меряются живым, где приблизительность безвредна.
    ///
    /// ## Почему это отличие от Lucene и почему оно вынужденное
    ///
    /// У Lucene смещения внутри сегмента длинные, и потолка такого нет вовсе.
    /// У нас он есть и сторожа на нём уже стоят ([`fits_offset`],
    /// [`crate::ids::fits_anchor`], [`crate::dict::fits_anchor`]). Политика
    /// обязана до них не доводить, а не полагаться на них: слияние, отказавшее
    /// на девятом часу, — это девять часов в мусор и ни одного слитого байта.
    /// Во что обойдётся по памяти окно, если добавить к нему этот сегмент.
    ///
    /// Считается тем же кодом, что и оценка перед началом работы
    /// ([`peak_estimate`]): разойдись они, политика выбирала бы окно по одному
    /// правилу, а отказывало бы оно по другому.
    fn peak_with(&self, s: &Seg) -> u64 {
        peak_of(Shape {
            src_docs: self.src_docs.saturating_add(s.docs) as u64,
            any_dead: self.any_dead || s.dead > 0,
            groups: self.groups.max(s.groups),
            out_docs: self.docs.saturating_add(s.live_docs()) as u64,
            units: self.units.saturating_add(s.units),
            multi_values: self.multi_values.saturating_add(s.multi_values),
            stored: self.stored_rest.saturating_add(s.stored_rest()),
            fields: self.fields.union(&s.order.iter().copied().collect()).count() as u64,
        })
    }

    /// То же, но называет **первый** потолок, за который окно вышло.
    ///
    /// Раньше сюда сходились восемь условий одной цепочкой `&&`, и наружу
    /// уходило одно слово «нет». Снаружи это выглядело как «работы нет» — то же
    /// самое, что у здорового узла, которому нечего сливать. Разница решающая:
    /// «нечего» проходит само, «не влезает» не проходит никогда.
    fn fits_why(&self, s: &Seg, t: &Tier) -> Result<(), NoPlan> {
        let part = crate::dict::offset_ceiling();
        // Память первой: это единственный потолок, за которым не отказ, а
        // смерть от ядра посреди работы.
        if t.max_peak > 0 {
            let need = self.peak_with(s);
            if need > t.max_peak {
                return Err(NoPlan::OverPeak { need, have: t.max_peak });
            }
        }
        if self.live.saturating_add(s.live()) > t.max_merged
            || self.docs.saturating_add(s.live_docs()) > t.max_docs
        {
            return Err(NoPlan::OverSize);
        }
        // Смещение внутри сегмента тридцатидвухбитное, и слияние, добравшееся
        // до потолка, бросает работу целиком. Политика обязана до него не
        // доводить, а не полагаться на сторожей внутри.
        if self.postings.saturating_add(s.postings) > part
            || self.positions.saturating_add(s.positions) > part
            || self.terms.saturating_add(s.terms) > part
            || self.stored_max.saturating_add(s.stored_max) > part
            || self.ids.saturating_add(s.ids) > part
            || self.dictionary.saturating_add(s.dictionary) > part
        {
            return Err(NoPlan::OverOffset);
        }
        Ok(())
    }

    fn add(&mut self, s: &Seg, t: &Tier) {
        self.live = self.live.saturating_add(s.live());
        self.bytes = self.bytes.saturating_add(s.bytes);
        self.floored = self.floored.saturating_add(s.floored(t));
        self.docs = self.docs.saturating_add(s.live_docs());
        self.postings = self.postings.saturating_add(s.postings);
        self.positions = self.positions.saturating_add(s.positions);
        self.terms = self.terms.saturating_add(s.terms);
        self.stored_max = self.stored_max.saturating_add(s.stored_max);
        self.ids = self.ids.saturating_add(s.ids);
        self.dictionary = self.dictionary.saturating_add(s.dictionary);
        self.stored_rest = self.stored_rest.saturating_add(s.stored_rest());
        self.src_docs = self.src_docs.saturating_add(s.docs);
        self.units = self.units.saturating_add(s.units);
        self.multi_values = self.multi_values.saturating_add(s.multi_values);
        self.any_dead |= s.dead > 0;
        self.groups = self.groups.max(s.groups);
        self.fields.extend(s.order.iter().copied());
    }
}

/// Оценка окна: **меньше — лучше**.
///
/// Форма `TieredMergePolicy::score` с одним нашим множителем.
///
/// * `skew` — перекос: доля самого крупного члена в окне. Ровный набор из
///   восьми даёт 1/8, пара «гигант и кроха» — почти единицу. Перекошенные
///   слияния — это `O(N^2)` переписанных байт за время жизни индекса, потому
///   что гигант переписывается заново на каждом слиянии.
/// * `pow(сумма живого, 0.05)` — лёгкое предпочтение мелким слияниям. Степень
///   нарочно крошечная: сделай её заметной, и политика уйдёт в частые
///   бессмысленные слияния мелочи.
/// * `pow(живая доля, reclaim_weight)` — сильное предпочтение окнам, которые
///   возвращают место. Живая доля здесь — это живой размер, делённый на
///   занятый: у чистого окна единица, у наполовину мёртвого — половина, а в
///   квадрате уже четверть.
/// * `1 + spread_penalty * разброс` — **наше** добавление, у Lucene его нет.
///   Смежность окна нам не безразлична: несмежное слияние перенумеровывает
///   документы между своими членами, а номер уходит клиенту в `Hit.doc` и
///   участвует в курсоре страницы ([`crate::search`]). Запрещать несмежность
///   нельзя — ярус тем и хорош, что собирает похожие по размеру, где бы они ни
///   лежали, — поэтому у неё цена, а не запрет. Разброс считается как число
///   чужих сегментов, перепрыгнутых окном, на каждого своего.
///
/// Отличие от Lucene, названное отдельно: у него перекос окна, упёршегося в
/// потолок, объявляется наилучшим (`skew = 1/maxMergeAtOnce`), чтобы крупные
/// сегменты всё же сливались. Нам это не нужно: кандидаты все мельче половины
/// потолка ([`Seg::frozen`]), значит любая пара под потолок влезает, и окно,
/// упёршееся в него, — это окно, которому не хватило места на **восьмого**
/// члена, а не на второго. Такое честнее оценить как есть.
fn score(segs: &[Seg], picked: &[usize], w: &Window, t: &Tier) -> f64 {
    let biggest = picked.iter().map(|&i| segs[i].floored(t)).max().unwrap_or(0);
    let skew = biggest as f64 / w.floored.max(1) as f64;
    // Единица снизу: логарифмическая по сути степень от нуля дала бы ноль, то
    // есть пустое окно оказалось бы лучшим из возможных.
    let size = (w.live.max(1) as f64).powf(0.05);
    let alive = if w.bytes == 0 { 1.0 } else { w.live as f64 / w.bytes as f64 };
    let lo = picked.iter().copied().min().unwrap_or(0);
    let hi = picked.iter().copied().max().unwrap_or(0);
    let n = picked.len().max(1);
    // Смежное окно даёт ноль: у него hi - lo ровно n-1.
    let spread = (hi - lo - (n - 1)) as f64 / n as f64;
    skew * size * alive.powf(t.reclaim_weight) * (1.0 + t.spread_penalty * spread)
}

/// Какие сегменты слить следующими: номера в порядке возрастания.
///
/// Форма `TieredMergePolicy::findMerges`. Возвращает **одно** окно, а не список
/// слияний: заход обслуживания делает одну работу и приходит снова через две
/// секунды, а держать очередь слияний значило бы вести состав, которого у
/// планировщика нет — он чистая функция.
///
/// Порядок работы:
///
/// 1. Замороженные ([`Seg::frozen`]) откладываются: сливать их не с чем.
/// 2. Считается [`allowed_seg_count`]. Сегментов не больше позволенного —
///    ответ `None`, и это не «нечего делать», а «индекс не перегружен».
/// 3. Кандидаты сортируются по размеру с полом, от крупных к мелким, и от
///    каждого начала набирается окно до `at_once` членов, пока держатся **все**
///    потолки разом ([`Window::fits`]) и у окна есть общий порядок полей.
/// 4. Лучшее по [`score`] окно и есть ответ.
///
/// ## Почему не соседи
///
/// Смежность [`merge`] не требуется: `base` считается из порядка переданного
/// среза. Ярус же собирает похожие по размеру, а они лежат где придётся —
/// особенно после того, как грязный сегмент вернулся в оборот по живому
/// размеру. Цена несмежности названа в [`score`], а порядок списка сохраняет
/// публикация: слитый встаёт **на место самого раннего** своего члена, и список
/// остаётся возрастным по возрасту.
///
/// ## Почему сегмент, не влезший в окно, пропускается, а не обрывает набор
///
/// Так делает Lucene, и довод тот же: набор идёт от крупных к мелким, и
/// крупный, не влезший под потолок, не запрещает влезть следующему за ним
/// мелкому. Обрыв на первом же неподходящем оставил бы половину ярусов без
/// слияний вовсе.
pub fn plan_tiered(segs: &[Seg], t: &Tier) -> Option<Vec<usize>> {
    plan_tiered_why(segs, t, &mut NoPlan::TooFew)
}

/// То же, но называет причину, по которой окно не сложилось.
pub fn plan_tiered_why(segs: &[Seg], t: &Tier, why: &mut NoPlan) -> Option<Vec<usize>> {
    if segs.len() < 2 {
        *why = NoPlan::TooFew;
        return None;
    }
    // Сначала долг, потом способность работать. Каждый замороженный сегмент
    // добавляет себе ровно одно разрешённое место в `allowed_seg_count`.
    // Поэтому состав только из замороженных сегментов здоров и не имеет долга.
    // Прежний обратный порядок называл такой установившийся индекс
    // `all_frozen`, хотя `pending_segments == 0`: оператор видел блокировку,
    // которой не существовало, а runner сохранял её в зелёном settled.
    if segs.len() as f64 <= allowed_seg_count(segs, t) {
        *why = NoPlan::WithinAllowed;
        return None;
    }
    let at_once = t.at_once.max(2);
    let mut cand: Vec<usize> = (0..segs.len()).filter(|&i| !segs[i].frozen(t)).collect();
    if cand.len() < 2 {
        // Все свободные сегменты выведены из укрупнения насовсем. Это не
        // «нечего делать»: долг, если он есть, не разберётся уже ничем, кроме
        // уборки.
        *why = NoPlan::AllFrozen;
        return None;
    }
    // По размеру с полом, от крупных к мелким; при равенстве — по возрасту.
    // Равенство здесь обычное дело (весь ярус ниже пола), и без второго ключа
    // выбор зависел бы от того, в каком порядке сортировка переставила равных.
    cand.sort_by(|&a, &b| segs[b].floored(t).cmp(&segs[a].floored(t)).then(a.cmp(&b)));

    let mut best: Option<(f64, Vec<usize>)> = None;
    let mut worst = NoPlan::WithinAllowed;
    for start in 0..cand.len() {
        let mut picked: Vec<usize> = Vec::with_capacity(at_once);
        let mut w = Window::default();
        for &i in cand[start..].iter() {
            if picked.len() == at_once {
                break;
            }
            let s = &segs[i];
            // Схема: первый член обязан быть годен сам с собой (повторённый
            // номер поля не даёт общего порядка даже одному сегменту), дальше
            // — совместим с уже набранными.
            let ok =
                if picked.is_empty() { distinct(&s.order) } else { schema_fits(segs, &picked, i) };
            if !ok {
                worst = worst.max(NoPlan::SchemaSplit);
                continue;
            }
            if let Err(e) = w.fits_why(s, t) {
                // Из причин, встреченных по дороге, наружу уходит самая
                // тяжёлая: несовместимость схем пройдёт сама после слияния
                // соседей, а нехватка памяти — нет.
                worst = worst.max(e);
                continue;
            }
            w.add(s, t);
            picked.push(i);
        }
        if picked.len() < 2 {
            continue;
        }
        let sc = score(segs, &picked, &w, t);
        if best.as_ref().is_none_or(|(b, _)| sc < *b) {
            best = Some((sc, picked));
        }
    }
    if best.is_none() {
        *why = worst;
    }
    best.map(|(_, mut picked)| {
        // По возрасту: слияние склеивает документы в порядке переданного среза,
        // и порядок этот обязан быть тем же, в каком сегменты лежат в списке, —
        // иначе номера в слитом пошли бы вразрез с возрастом.
        picked.sort_unstable();
        picked
    })
}

/// Уживётся ли сегмент `add` с уже набранными в одном слиянии.
///
/// Быстрый путь тот же, что у [`plan_flat`]: совпали подписи — сравниваются
/// сами векторы, и на этом всё, потому что одинаковая схема сама себе общий
/// порядок. Разошлись — строится общий порядок целиком, и это законно: схема,
/// у которой дописали поле, с прежней сливается.
/// Сводятся ли схемы двух сегментов к общему порядку полей.
///
/// Наружу — для последней ступени лестницы деградации: пара, у которой общего
/// порядка нет, не слилась бы, и предлагать её значило бы потратить заход на
/// заведомый отказ.
pub fn pair_fits_schema(segs: &[Seg], a: usize, b: usize) -> bool {
    distinct(&segs[a].order) && schema_fits(segs, &[a], b)
}

fn schema_fits(segs: &[Seg], picked: &[usize], add: usize) -> bool {
    let head = &segs[picked[0]];
    let s = &segs[add];
    // Все набранные уже сверены с первым, поэтому сравнивается только новичок.
    if head.sig == s.sig && head.order == s.order {
        return true;
    }
    let mut orders: Vec<Vec<u32>> = picked.iter().map(|&i| segs[i].order.clone()).collect();
    orders.push(s.order.clone());
    field_order(&orders).is_some()
}

#[cfg(test)]
mod signature_tests {
    use super::*;

    /// Подпись различает переставленные поля, а не только их набор.
    ///
    /// ## Что ломалось бы без этого
    ///
    /// Подпись — единственное, по чему планировщик решает, что схемы одинаковы и
    /// строить общий порядок не нужно. Сведись она к набору полей — и два
    /// сегмента, объявившие одни и те же поля в обратном порядке, прошли бы за
    /// одинаковые. Окно взяло бы их вместе, а слияние на них отказывает: общего
    /// порядка нет и словари сливать нечем. Отказ повторялся бы на каждом
    /// заходе, то есть обслуживание встало бы насовсем.
    #[test]
    fn the_schema_signature_tells_reordered_fields_apart() {
        assert_eq!(schema_sig(&[0, 1, 2]), schema_sig(&[0, 1, 2]), "подпись не устойчива");
        assert_ne!(schema_sig(&[0, 1, 2]), schema_sig(&[0, 2, 1]), "перестановка не различена");
        assert_ne!(schema_sig(&[1, 0]), schema_sig(&[0, 1]), "перестановка пары не различена");
        // Дописанное поле — тоже другая схема: у неё другой порядок выхода.
        assert_ne!(schema_sig(&[0, 1]), schema_sig(&[0, 1, 2]), "состав не различён");
        assert_ne!(schema_sig(&[]), schema_sig(&[0]), "пустая схема совпала с однопольной");
        // Подпись не сводится к сумме или исключающему ИЛИ: у них перестановка
        // и повтор дают то же число.
        assert_ne!(schema_sig(&[1, 2]), schema_sig(&[2, 1]));
        assert_ne!(schema_sig(&[3, 3]), schema_sig(&[]));
        // Разные номера — разные подписи. Иначе сегменты с непересекающимися
        // схемами считались бы одинаковыми.
        let mut all: Vec<u64> = (0..256u32).map(|i| schema_sig(&[i])).collect();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), 256, "подписи однопольных схем столкнулись");
    }
}

#[cfg(test)]
mod policy_tests {
    use super::*;

    /// Одна схема у всех: проверяется поведение окна, а не порядок полей.
    fn same(n: usize) -> Vec<Vec<u32>> {
        vec![vec![0u32, 1]; n]
    }

    /// Разный состав полей окну не мешает, а противоречие в порядке — мешает.
    ///
    /// Слияние умеет объединять поля, но только если у сегментов есть общий
    /// порядок. Без него оно отказывает, а отказ на каждом заходе означал бы
    /// вставшее обслуживание.
    #[test]
    fn a_window_stops_where_no_common_field_order_exists() {
        let p = Policy { at_once: 8, max_docs: 100 };
        // Дописанное поле окну не помеха: порядок [0,1] и [0,1,2] совместим.
        let grown = vec![vec![0u32, 1], vec![0, 1], vec![0, 1, 2], vec![0, 1, 2]];
        assert_eq!(plan(&[2, 2, 2, 2], &grown, p), Some((0, 4)), "дописанное поле разорвало окно");

        // А переставленные поля — помеха: общего порядка нет.
        let flipped = vec![vec![0u32, 1], vec![0, 1], vec![1, 0], vec![1, 0]];
        let (from, to) = plan(&[2, 2, 2, 2], &flipped, p).expect("окно нашлось");
        assert_eq!(to - from, 2, "окно {from}..{to} перешло через противоречие");

        // Порядок противоречит на каждом шаге — сливать нечего.
        assert_eq!(plan(&[2, 2], &[vec![0, 1], vec![1, 0]], p), None, "совместимых соседей нет");
    }

    #[test]
    fn merges_the_widest_window_that_fits() {
        let p = Policy { at_once: 4, max_docs: 10 };
        // Четыре подряд по два укладываются целиком.
        assert_eq!(plan(&[2, 2, 2, 2, 9], &same(64), p), Some((0, 4)));
        // Потолок режет окно: девятка ни с кем не сливается.
        assert_eq!(plan(&[9, 2, 2, 2, 2], &same(64), p), Some((1, 5)));
    }

    #[test]
    fn nothing_to_merge() {
        let p = Policy { at_once: 4, max_docs: 10 };
        assert_eq!(plan(&[], &same(64), p), None, "пустой индекс");
        assert_eq!(plan(&[5], &same(64), p), None, "один сегмент");
        assert_eq!(plan(&[9, 9, 9], &same(64), p), None, "любая пара выходит за потолок");
    }

    /// Граница четырёх гигабайт: `u32::MAX` ещё влезает, на байт больше — нет.
    ///
    /// Настоящее переполнение в тесте не воспроизвести — оно требует четырёх
    /// гигабайт на диске, — поэтому проверяется само правило и его край.
    /// Край здесь несущий: потолок это **сам** `u32::MAX`, потому что такое
    /// смещение ещё представимо четырьмя байтами, и приём допускает его тем же
    /// сравнением. Сдвинь границу на единицу — и укрупнение начнёт отказывать
    /// сегментам, которые приём принимает.
    #[test]
    fn the_four_gigabyte_ceiling_is_inclusive() {
        assert!(fits_offset(0, "список", "/тмп/mrg0000", 1).is_ok());
        assert!(
            fits_offset(u32::MAX as u64, "список", "/тмп/mrg0000", 1).is_ok(),
            "потолок обязан быть самим u32::MAX — приём допускает его"
        );

        let e = fits_offset(u32::MAX as u64 + 1, "позиции", "/тмп/mrg0000", 7)
            .expect_err("на байт за потолком обязан быть отказ");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        // Отказ обязан называть каталог, вид файла, слово и выход: без этого
        // оператор не поймёт, что именно уменьшать.
        assert!(msg.contains("mrg0000"), "отказ не назвал сегмент: {msg}");
        assert!(msg.contains("позиции"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("7-м слове"), "отказ не назвал слово: {msg}");
        assert!(msg.contains("--target"), "отказ не подсказал выход: {msg}");
    }

    /// Предикат отказывает каждому из трёх потоков и называет его по имени.
    ///
    /// ## Почему имя проверки такое узкое
    ///
    /// Прежде она называлась «потолок меряется у каждого потока слова» и
    /// утверждала этим то, чего не проверяла: что все три вызова стоят в
    /// `merge`. Здесь зовётся один только предикат, и сними хоть все три вызова
    /// с пути записи — проверка останется зелёной. Проводку закрывает
    /// `a_merge_refuses_the_moment_a_stream_crosses_the_ceiling`, и имя обязано
    /// говорить, где что проверено, иначе набор врёт о собственном покрытии.
    ///
    /// ## Что ломалось бы без потока слов
    ///
    /// В опору `terms.idx` уходят четыре тридцатидвухбитных числа, и смещение
    /// внутри `terms.bin` — одно из них. Прежде проверялись только два: список
    /// и позиции. Завёрнутое смещение слова ложилось в опору, и на следующем
    /// открытии `walk_terms` объявлял сегмент негодным — а исходные к тому
    /// времени уже снесены уборкой, и поднимать индекс не из чего. Это
    /// единственный из пяти обрывов, который хоть кем-то ловился, и ловился
    /// слишком поздно.
    #[test]
    fn the_predicate_names_each_of_the_three_streams_it_refuses() {
        for what in ["список", "позиции", "слова"] {
            assert!(
                fits_offset(u32::MAX as u64, what, "/тмп/mrg0000", 1).is_ok(),
                "{what}: сам u32::MAX обязан проходить"
            );
            let e = fits_offset(u32::MAX as u64 + 1, what, "/тмп/mrg0000", 3)
                .err()
                .unwrap_or_else(|| panic!("{what}: за потолком обязан быть отказ"));
            assert!(e.to_string().contains(what), "{what}: отказ не назвал поток: {e}");
        }
    }

    /// При равном числе сегментов выбирается самое дешёвое окно.
    #[test]
    fn prefers_the_cheaper_window() {
        let p = Policy { at_once: 2, max_docs: 100 };
        assert_eq!(plan(&[50, 40, 1, 1], &same(64), p), Some((2, 4)));
    }

    /// Повторное применение сходится: сегментов становится всё меньше.
    #[test]
    fn converges() {
        let p = Policy { at_once: 8, max_docs: 50 };
        let mut sizes = vec![2usize; 40];
        let mut rounds = 0;
        while let Some((from, to)) = plan(&sizes, &same(64), p) {
            let total: usize = sizes[from..to].iter().sum();
            sizes.splice(from..to, [total]);
            rounds += 1;
            assert!(rounds < 100, "политика не сходится");
        }
        assert!(sizes.len() <= 6, "осталось сегментов: {}", sizes.len());
        assert!(sizes.iter().all(|&s| s <= 50), "потолок соблюдён");
    }
}

#[cfg(test)]
mod plan_equivalence_tests {
    use super::*;

    /// Прежний планировщик: построение общего порядка на каждом шаге каждого
    /// окна. Оракул — снят с кода дословно, до порядка проверок включительно.
    fn plan_by_sorting(sizes: &[usize], orders: &[Vec<u32>], p: Policy) -> Option<(usize, usize)> {
        let mut best: Option<(usize, usize, usize)> = None;
        for from in 0..sizes.len() {
            let mut total = 0usize;
            let mut count = 0usize;
            for (at, size) in sizes[from..].iter().enumerate().take(p.at_once) {
                if !orders.is_empty()
                    && field_order(&orders[from..=(from + at).min(orders.len() - 1)]).is_none()
                {
                    break;
                }
                if total.saturating_add(*size) > p.max_docs {
                    break;
                }
                total += size;
                count += 1;
            }
            if count < 2 {
                continue;
            }
            let better = match best {
                None => true,
                Some((c, t, _)) => count > c || (count == c && total < t),
            };
            if better {
                best = Some((count, total, from));
            }
        }
        best.map(|(count, _, from)| (from, from + count))
    }

    /// Линейный конгруэнтный генератор: наборы обязаны быть одни и те же у всех.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 11
        }
        fn upto(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// Схемы для набора: совместимые, противоречивые и вырожденная.
    fn pool() -> Vec<Vec<u32>> {
        vec![
            vec![0, 1, 2],
            vec![0, 1, 2, 3], // дописанное поле: порядку не противоречит
            vec![0, 2],       // пропущенное поле: тоже не противоречит
            vec![0, 2, 1],    // переставленные: противоречие
            vec![1, 0],       // противоречие в паре
            vec![7, 8, 9],    // непересекающаяся схема
            vec![0, 1, 0],    // вырожденная: номер повторён
            vec![],           // сегмент без полей
        ]
    }

    /// Новый выбор окна совпадает со старым на любых наборах.
    ///
    /// ## Что ломалось бы без него
    ///
    /// Подпись схемы заведена ради скорости, а не ради новой политики. Стоит
    /// быстрому пути ответить «схемы одни и те же» там, где прежний обход
    /// отвечал «общего порядка нет», — и окно возьмёт сегменты, на которых
    /// слияние отказывает: обслуживание встанет и будет вставать каждые две
    /// секунды. Ошибись он в другую сторону — окна начнут рваться на ровном
    /// месте, и индекс перестанет укрупняться, не сказав ни слова.
    ///
    /// Поэтому сверка идёт с **дословно перенесённым прежним обходом** и точным
    /// равенством ответа, на тысяче наборов, где схемы намеренно перемешаны:
    /// совместимые, противоречивые, непересекающиеся, вырожденная и пустая.
    #[test]
    fn the_signed_planner_picks_exactly_what_the_old_scan_picked() {
        let schemas = pool();
        let mut rng = Rng(0x5EED_1234_5678_9ABC);
        let (mut mixed, mut chosen, mut refused) = (0usize, 0usize, 0usize);
        for case in 0..1000 {
            let n = 1 + rng.upto(12);
            let sizes: Vec<usize> = (0..n).map(|_| 1 + rng.upto(20)).collect();
            let orders: Vec<Vec<u32>> =
                (0..n).map(|_| schemas[rng.upto(schemas.len())].clone()).collect();
            let sigs: Vec<u64> = orders.iter().map(|o| schema_sig(o)).collect();
            let p = Policy { at_once: 2 + rng.upto(7), max_docs: 1 + rng.upto(60) };

            let want = plan_by_sorting(&sizes, &orders, p);
            let got = plan_flat(&sizes, &orders, &sigs, p);
            assert_eq!(
                got, want,
                "набор {case}: окна разошлись\n размеры {sizes:?}\n схемы {orders:?}\n политика {p:?}"
            );
            // И обёртка, считающая подписи сама, обязана давать то же.
            assert_eq!(plan(&sizes, &orders, p), want, "набор {case}: обёртка разошлась");

            if orders.windows(2).any(|w| w[0] != w[1]) {
                mixed += 1;
            }
            if want.is_some() {
                chosen += 1;
            } else {
                refused += 1;
            }
        }
        // Проверка не должна быть пустой: наборы обязаны быть разными и по
        // схемам, и по исходу.
        assert!(mixed > 700, "схемы почти не перемешивались: {mixed} наборов");
        assert!(chosen > 200, "окно почти никогда не находилось: {chosen} наборов");
        assert!(refused > 20, "отказ почти не встречался: {refused} наборов");
    }

    /// Вырожденная схема отсекается и быстрым путём тоже.
    ///
    /// Повторённый номер поля не даёт общего порядка даже одному сегменту с
    /// самим собой. Прежний обход это ловил построением порядка; быстрый путь
    /// обязан ловить тем же исходом, иначе такой сегмент попадёт в окно и
    /// слияние на нём откажет.
    #[test]
    fn a_schema_with_a_repeated_field_number_never_enters_a_window() {
        let p = Policy { at_once: 8, max_docs: 1000 };
        let orders = vec![vec![4u32, 4], vec![4, 4], vec![4, 4]];
        let sigs: Vec<u64> = orders.iter().map(|o| schema_sig(o)).collect();
        assert!(field_order(&orders[..1]).is_none(), "оракул перестал отвергать повтор");
        assert_eq!(plan_flat(&[2, 2, 2], &orders, &sigs, p), None, "вырожденная схема слита");
        assert_eq!(
            plan_flat(&[2, 2, 2], &orders, &sigs, p),
            plan_by_sorting(&[2, 2, 2], &orders, p)
        );
        assert!(distinct(&[4, 5]), "различные номера объявлены повтором");
        assert!(!distinct(&[4, 5, 4]), "повтор не замечен");
    }

    /// Столкновение подписей не меняет выбор: совпадение подтверждается векторами.
    ///
    /// Подписи считает не планировщик, а тот, кто его зовёт, и требовать от
    /// хеша отсутствия столкновений значило бы поставить правильность выбора в
    /// зависимость от вероятности. Здесь столкновение подделано нарочно: всем
    /// сегментам выдана одна подпись при заведомо разных схемах.
    #[test]
    fn equal_signatures_on_different_schemas_do_not_widen_the_window() {
        let p = Policy { at_once: 8, max_docs: 1000 };
        let orders = vec![vec![0u32, 1], vec![0, 1], vec![1, 0], vec![1, 0]];
        let lying = vec![7u64; orders.len()];
        let want = plan_by_sorting(&[2, 2, 2, 2], &orders, p);
        assert_eq!(want, Some((0, 2)), "оракул выбрал не то окно");
        assert_eq!(
            plan_flat(&[2, 2, 2, 2], &orders, &lying, p),
            want,
            "подделанная подпись расширила окно"
        );
    }

    /// Подписей меньше, чем сегментов, — ответ тот же, только дороже.
    #[test]
    fn missing_signatures_fall_back_to_comparing_the_orders() {
        let p = Policy { at_once: 8, max_docs: 1000 };
        let orders = vec![vec![0u32, 1], vec![0, 1], vec![1, 0], vec![1, 0]];
        let want = plan_by_sorting(&[2, 2, 2, 2], &orders, p);
        assert_eq!(plan_flat(&[2, 2, 2, 2], &orders, &[], p), want, "без подписей выбор поехал");
        let head: Vec<u64> = orders[..2].iter().map(|o| schema_sig(o)).collect();
        assert_eq!(
            plan_flat(&[2, 2, 2, 2], &orders, &head, p),
            want,
            "неполные подписи сбили выбор"
        );
    }

    fn calls() -> usize {
        ORDER_CALLS.with(|c| c.get())
    }

    /// При одной схеме на весь набор построение порядка не зовётся ни разу.
    ///
    /// ## Что ломалось бы без него
    ///
    /// Ради этого подпись и заведена. Прежде `field_order` звался на каждом
    /// шаге каждого окна: при десяти тысячах сегментов, окне в восемь и двадцати
    /// полях это сотни миллионов действий каждые две секунды, целиком под
    /// замком состава, — и всё ради ответа, который известен заранее, потому что
    /// схема у всех одна. Проверять это временем нельзя, поэтому считаются сами
    /// вызовы; счётчик потоковый, чтобы соседние проверки его не путали.
    ///
    /// Обратный случай проверяется тут же: на противоречии в порядке вызовы
    /// обязаны быть. Иначе тест доказывал бы только то, что счётчик сломан.
    #[test]
    fn one_schema_across_the_set_costs_no_topological_sort() {
        let p = Policy { at_once: 8, max_docs: 100_000 };
        let n = 200usize;
        let sizes = vec![3usize; n];
        let orders = vec![vec![7u32, 3, 9]; n];
        let sigs: Vec<u64> = orders.iter().map(|o| schema_sig(o)).collect();

        let before = calls();
        assert_eq!(plan_flat(&sizes, &orders, &sigs, p), Some((0, 8)), "окно выбрано не то");
        assert_eq!(
            calls() - before,
            0,
            "при одной схеме порядок строился {} раз",
            calls() - before
        );

        // Один сегмент с переставленными полями — и сортировка обязана позваться.
        let mut mixed = orders.clone();
        mixed[3] = vec![3, 7, 9];
        let sigs: Vec<u64> = mixed.iter().map(|o| schema_sig(o)).collect();
        let before = calls();
        let got = plan_flat(&sizes, &mixed, &sigs, p);
        assert!(calls() > before, "на противоречии порядок не строился вовсе");
        assert_eq!(got, plan_by_sorting(&sizes, &mixed, p), "на противоречии выбор разошёлся");
    }
}

#[cfg(test)]
mod plan_overflow_tests {
    use super::*;

    /// Одна схема у всех: проверяется потолок окна, а не порядок полей.
    fn same(n: usize) -> Vec<Vec<u32>> {
        vec![vec![0u32, 1]; n]
    }

    /// Доросший сегмент не должен попадать в слияние ни при каком соседстве.
    #[test]
    fn full_segments_are_never_drawn_into_a_merge() {
        let p = Policy { at_once: 8, max_docs: 2_000_000 };
        // Пять полноразмерных: сливать нечего.
        assert_eq!(plan(&[2_000_000; 5], &same(64), p), None);
        // Полноразмерные вперемешку с мелочью: берётся только мелочь и только
        // подряд идущая.
        let sizes = [2_000_000, 300_000, 400_000, 2_000_000, 100_000];
        assert_eq!(plan(&sizes, &same(64), p), Some((1, 3)));
        // Даже огромный сосед не должен ничего испортить.
        let huge = [usize::MAX / 2, 300_000, 400_000];
        assert_eq!(plan(&huge, &same(64), p), Some((1, 3)));
    }

    /// Потолок соблюдается: сумма слитого не превышает его никогда.
    #[test]
    fn a_merge_never_exceeds_the_ceiling() {
        let p = Policy { at_once: 8, max_docs: 1_000_000 };
        let sizes = [400_000, 400_000, 400_000, 400_000];
        let (from, to) = plan(&sizes, &same(64), p).expect("две штуки влезают");
        let total: usize = sizes[from..to].iter().sum();
        assert!(total <= p.max_docs, "слито {total} при потолке {}", p.max_docs);
        assert_eq!(to - from, 2);
    }
}

#[cfg(test)]
mod column_tests {
    use super::*;
    use crate::index::Kind;
    use crate::manifest;

    /// Значения переживают слияние, а погашенные документы — нет.
    ///
    /// Проверка не про то, что файлы записались: она про то, что значение
    /// осталось при **своём** документе после перенумерации. Сдвиг номеров при
    /// выбросе мёртвых — ровно то место, где колонка и постинги могут разъехаться
    /// молча, и ни один тест на правильность выдачи этого не увидит.
    #[test]
    fn values_follow_their_documents_through_a_merge() {
        let dir = std::env::temp_dir().join("svindb-merge-columns");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();

        let mut ix = crate::indexer::Indexer::typed(
            &d,
            vec!["title".into(), "ts".into()],
            vec![1.0, 1.0],
            vec![Kind::Text, Kind::Date],
            100,
        );
        for i in 0..250 {
            // У каждого третьего метки нет вовсе — она обязана остаться пустой.
            let ts =
                if i % 3 == 0 { String::new() } else { format!("{}", 1_700_000_000_000i64 + i) };
            ix.add(&format!("док-{i}"), &[&format!("слово{}", i % 5), &ts]);
        }
        ix.flush();

        let names = manifest::segments(&d);
        assert_eq!(names.len(), 3, "три сегмента по сто");
        manifest::write(&d, &names.iter().map(|p| manifest::name_of(p)).collect::<Vec<_>>())
            .unwrap();

        // Гасим каждый пятый документ первого сегмента.
        let ids: Vec<String> =
            (0..100).filter(|i| i % 5 == 0).map(|i| format!("док-{i}")).collect();
        let killed = crate::maintain::delete(&d, &ids).unwrap().killed;
        assert_eq!(killed, ids.len());

        let loaded: Vec<Index> = names.iter().map(|n| index::load(n).unwrap()).collect();
        let refs: Vec<&Index> = loaded.iter().collect();
        let out = format!("{d}/mrg0000");
        let res = merge(&refs, &out).unwrap();
        drop(loaded);

        let merged = index::load(&out).unwrap();
        assert_eq!(merged.docs, res.docs);
        let col = merged.fields[1].column.as_ref().expect("колонка на месте");

        // Живые документы по порядку — и значение каждого обязано совпасть.
        let mut at = 0u32;
        for i in 0..250i64 {
            if i < 100 && i % 5 == 0 {
                continue; // погашен, в слитом его нет
            }
            let want = if i % 3 == 0 { None } else { Some(1_700_000_000_000i64 + i) };
            assert_eq!(col.get(at), want, "документ {i} стал номером {at}");
            at += 1;
        }
        assert_eq!(at as usize, merged.docs, "живых столько же, сколько ждали");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Содержимое переживает слияние: живые сохраняют своё, погашенные исчезают.
    ///
    /// Проверяется на **каждом** документе, а не на выборке. Ошибка в обходе
    /// сдвинула бы содержимое на один и вернула бы соседа — а сосед выглядит
    /// правдоподобно, и заметить подмену можно только полным перебором.
    #[test]
    fn content_follows_its_documents_through_a_merge() {
        let dir = std::env::temp_dir().join("svindb-merge-store");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();

        // Второе поле объявлено нехранимым: оно обязано искаться и не храниться.
        let mut ix = crate::indexer::Indexer::typed_stored(
            &d,
            vec!["title".into(), "secret".into()],
            vec![1.0, 1.0],
            vec![Kind::Text, Kind::Text],
            vec![true, false],
            100,
        );
        let body = |i: i64| format!("документ {i} со словом {} и длинным хвостом текста", i % 7);
        for i in 0..250i64 {
            ix.add(&format!("док-{i}"), &[&body(i), &format!("тайна{i}")]);
        }
        ix.flush();

        let names = manifest::segments(&d);
        manifest::write(&d, &names.iter().map(|p| manifest::name_of(p)).collect::<Vec<_>>())
            .unwrap();
        let ids: Vec<String> =
            (0..100).filter(|i| i % 5 == 0).map(|i| format!("док-{i}")).collect();
        assert_eq!(crate::maintain::delete(&d, &ids).unwrap().killed, ids.len());

        let loaded: Vec<Index> = names.iter().map(|n| index::load(n).unwrap()).collect();
        // До слияния содержимое тоже обязано читаться.
        assert_eq!(loaded[0].fetch(1, 0).as_deref(), Some(body(1).as_bytes()));
        assert!(loaded[0].fetch(1, 1).is_none(), "нехранимое поле отдало содержимое");

        let refs: Vec<&Index> = loaded.iter().collect();
        let out = format!("{d}/mrg0000");
        merge(&refs, &out).unwrap();
        drop(loaded);

        let merged = index::load(&out).unwrap();
        let mut at = 0u32;
        for i in 0..250i64 {
            if i < 100 && i % 5 == 0 {
                continue;
            }
            assert_eq!(
                merged.fetch(at, 0).as_deref(),
                Some(body(i).as_bytes()),
                "документ {i} стал номером {at} и вернул не своё"
            );
            at += 1;
        }
        assert_eq!(at as usize, merged.docs);
        assert!(merged.fetch(0, 1).is_none(), "нехранимое поле пережило слияние");
        assert!(
            !std::path::Path::new(&format!("{out}/secret.sbin")).exists(),
            "нехранимое поле заняло место на диске"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Позиции переживают слияние: у живых те же, что были, погашенных нет.
    ///
    /// Проверяется на каждом вхождении каждого слова. Сдвиг на одно вхождение
    /// вернул бы позиции соседнего документа — величины правдоподобные, и
    /// заметить подмену иначе нельзя.
    #[test]
    fn positions_follow_their_documents_through_a_merge() {
        let dir = std::env::temp_dir().join("svindb-merge-pos");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();

        let text = |i: i64| {
            let n = 2 + (i % 11);
            (0..n).map(|k| format!("сл{}", (i + k) % 9)).collect::<Vec<_>>().join(" ")
        };
        let mut ix = crate::indexer::Indexer::new(&d, vec!["body".into()], vec![1.0], 100);
        for i in 0..250i64 {
            ix.add(&format!("док-{i}"), &[&text(i)]);
        }
        ix.flush();

        let names = manifest::segments(&d);
        manifest::write(&d, &names.iter().map(|p| manifest::name_of(p)).collect::<Vec<_>>())
            .unwrap();
        let ids: Vec<String> =
            (0..100).filter(|i| i % 5 == 0).map(|i| format!("док-{i}")).collect();
        assert_eq!(crate::maintain::delete(&d, &ids).unwrap().killed, ids.len());

        let loaded: Vec<Index> = names.iter().map(|n| index::load(n).unwrap()).collect();
        let refs: Vec<&Index> = loaded.iter().collect();
        let out = format!("{d}/mrg0000");
        merge(&refs, &out).unwrap();
        drop(loaded);

        let merged = index::load(&out).unwrap();
        assert!(merged.has_positions(), "слияние потеряло позиции");

        // Чего ждём после перенумерации.
        let mut want: std::collections::HashMap<String, Vec<(u32, Vec<u32>)>> =
            std::collections::HashMap::new();
        let mut at = 0u32;
        for i in 0..250i64 {
            if i < 100 && i % 5 == 0 {
                continue;
            }
            let t = text(i);
            let mut by: std::collections::HashMap<&str, Vec<u32>> =
                std::collections::HashMap::new();
            for (p, w) in t.split_whitespace().enumerate() {
                by.entry(w).or_default().push(p as u32);
            }
            for (w, ps) in by {
                want.entry(w.to_string()).or_default().push((at, ps));
            }
            at += 1;
        }
        assert_eq!(at as usize, merged.docs);

        let mut got = Vec::new();
        let mut checked = 0usize;
        for (word, docs) in want.iter() {
            let list = merged.dict.lookup(0, word).expect("слово в словаре");
            let mut c = merged.cursor(list, 0, 1.0);
            for (doc, ps) in docs {
                assert_eq!(c.cur(), *doc, "слово {word}: список разошёлся");
                c.positions(&mut got);
                assert_eq!(&got, ps, "слово {word}, документ {doc}: позиции");
                checked += 1;
                c.next();
            }
            assert_eq!(c.cur(), u32::MAX, "слово {word}: лишние документы");
        }
        assert!(checked > 800, "проверено всего {checked}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Слияние сегментов, собранных разными версиями схемы.
#[cfg(test)]
mod evolution_tests {
    use super::*;
    use crate::index::Kind;
    use crate::indexer::Indexer;

    /// Собирает сегмент своей схемой и отдаёт путь к нему.
    fn seg(root: &str, name: &str, fields: &[(&str, Kind, u32)], rows: &[Vec<String>]) -> String {
        let dir = format!("{root}/{name}-build");
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            fields.iter().map(|(n, _, _)| n.to_string()).collect(),
            vec![1.0; fields.len()],
            fields.iter().map(|(_, k, _)| *k).collect(),
            1_000_000,
        );
        ix.set_fids(fields.iter().map(|(_, _, f)| *f).collect());
        for (i, row) in rows.iter().enumerate() {
            let v: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
            ix.add(&format!("{name}{i}"), &v);
        }
        ix.flush_now();
        let out = format!("{root}/{name}");
        std::fs::rename(format!("{dir}/seg0000"), &out).expect("сегмент перенесён");
        std::fs::remove_dir_all(&dir).ok();
        out
    }

    /// Поле, добавленное позже, переживает укрупнение и не смешивается с чужим.
    #[test]
    fn a_field_added_later_survives_a_merge() {
        let root = std::env::temp_dir().join("svindb-merge-evo");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        let old: Vec<Vec<String>> =
            (0..200).map(|i| vec![format!("общий документ {i}"), (i * 10).to_string()]).collect();
        let a = seg(&root, "a", &[("title", Kind::Text, 0), ("price", Kind::Int32, 1)], &old);
        // Новая: title, price, extra. Номер `price` тот же, позиция та же,
        // а `extra` дописан в конец.
        let new: Vec<Vec<String>> = (0..200)
            .map(|i| {
                vec![format!("общий документ {i}"), (i * 10).to_string(), (1000 + i).to_string()]
            })
            .collect();
        let b = seg(
            &root,
            "b",
            &[("title", Kind::Text, 0), ("price", Kind::Int32, 1), ("extra", Kind::Int32, 2)],
            &new,
        );

        let (ia, ib) = (index::load(&a).unwrap(), index::load(&b).unwrap());
        let out = format!("{root}/mrg");
        let res = merge(&[&ia, &ib], &out).expect("слияние разных схем");
        assert_eq!(res.docs, 400);

        let m = index::load(&out).expect("слитый сегмент открылся");
        assert_eq!(m.fields.len(), 3, "состав выхода — объединение");
        let names: Vec<&str> = m.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["title", "price", "extra"], "порядок сохранил порядок источников");
        let fids: Vec<u32> = m.fields.iter().map(|f| f.fid).collect();
        assert_eq!(fids, [0, 1, 2], "устойчивые номера перенесены как есть");

        // Слово из общего поля обязано найтись у документов обоих источников.
        let list = m.dict.lookup(0, "документ").expect("слово в словаре");
        let mut seen = 0usize;
        let mut c = m.cursor(list, 0, 1.0);
        while c.cur() != u32::MAX {
            seen += 1;
            c.next();
        }
        assert_eq!(seen, 400, "слово общего поля потеряло документы");

        // Цена есть у всех четырёхсот и совпадает со своим документом.
        let price = m.fields[1].column.as_ref().expect("колонка цены");
        for d in 0..400u32 {
            assert_eq!(price.get(d), Some((d as i64 % 200) * 10), "цена документа {d}");
        }

        let extra = m.fields[2].column.as_ref().expect("колонка нового поля");
        for d in 0..200u32 {
            assert_eq!(extra.get(d), None, "у старого документа {d} взялось новое поле");
        }
        for d in 200..400u32 {
            assert_eq!(extra.get(d), Some(1000 + d as i64 - 200), "новое поле документа {d}");
        }

        // Содержимое отдаётся по своим документам, а не съезжает.
        let text = String::from_utf8(m.fetch(199, 0).expect("содержимое")).unwrap();
        assert_eq!(text, "общий документ 199");
        let text = String::from_utf8(m.fetch(399, 0).expect("содержимое")).unwrap();
        assert_eq!(text, "общий документ 199", "содержимое съехало на чужой документ");

        let rep = crate::check::check(&out, crate::check::Depth::Full);
        let bad: Vec<String> = rep
            .findings
            .iter()
            .map(|f| format!("{} {}: {} / {}", f.scope, f.check, f.text, f.means))
            .collect();
        assert_eq!(rep.broken(), 0, "{bad:?}");
        std::fs::remove_dir_all(&root).ok();
    }

    /// Поля, упорядоченные в сегментах по-разному, слить нечем — и это отказ.
    #[test]
    fn fields_ordered_differently_are_refused_loudly() {
        let root = std::env::temp_dir().join("svindb-merge-cycle");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        let rows: Vec<Vec<String>> =
            (0..8).map(|i| vec![format!("слово {i}"), i.to_string()]).collect();
        let a = seg(&root, "a", &[("x", Kind::Text, 0), ("y", Kind::Int32, 1)], &rows);
        // Те же поля, тот же устойчивый номер — но объявлены наоборот.
        let flip: Vec<Vec<String>> =
            (0..8).map(|i| vec![i.to_string(), format!("слово {i}")]).collect();
        let b = seg(&root, "b", &[("y", Kind::Int32, 1), ("x", Kind::Text, 0)], &flip);

        let (ia, ib) = (index::load(&a).unwrap(), index::load(&b).unwrap());
        let msg = match merge(&[&ia, &ib], &format!("{root}/mrg")) {
            Ok(_) => panic!("слияние приняло поля, упорядоченные по-разному"),
            Err(e) => e.to_string(),
        };
        assert!(msg.contains("общего порядка нет"), "невнятный отказ: {msg}");
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Пик памяти слияния: он не обязан расти вместе с сегментом.
/// Матрица профиля пика: какая фаза сколько держит на разных схемах.
///
/// ## Зачем это тест, а не отдельная программа
///
/// Константы формулы обязаны браться из замера, а замер обязан жить рядом с
/// тем, что он меряет: программа сбоку устаревает молча, тест — падает. Здесь
/// перебираются схемы, доля мёртвых и число потоков сжатия, и на каждой ячейке
/// проверяется единственное, что от формулы требуется: она **не ниже**
/// измеренного пика.
///
/// Отдельно печатается разбивка по фазам — по ней и правятся константы. Чтобы
/// увидеть её, нужна профилировочная сборка и переменная среды; в обычной
/// сборке разбивка пуста, а проверка стороны неравенства работает всё равно.
#[cfg(test)]
mod profile_matrix {
    use super::*;
    use crate::index::Kind;
    use crate::indexer::{Cell, Indexer};

    fn build(
        root: &str,
        tag: &str,
        from: usize,
        n: usize,
        fields: usize,
        arity: usize,
        stored: bool,
    ) -> String {
        let dir = format!("{root}/{tag}-build");
        std::fs::remove_dir_all(&dir).ok();
        let mut names: Vec<String> = vec!["тело".into()];
        let mut kinds = vec![Kind::Text];
        let mut groups = vec![0u32];
        for f in 1..fields {
            names.push(format!("поле{f}"));
            kinds.push(if f % 2 == 0 { Kind::Int64 } else { Kind::Keyword });
            groups.push(if arity > 1 { 1 } else { 0 });
        }
        let mut ix = Indexer::typed_stored(
            &dir,
            names,
            vec![1.0; fields],
            kinds,
            vec![stored; fields],
            10_000_000,
        );
        if arity > 1 {
            ix.set_groups(groups, vec![String::new(), "группа".into()]);
        }
        for d in from..from + n {
            let mut texts: Vec<String> = Vec::new();
            let mut cells: Vec<Cell> = Vec::new();
            texts.push(format!("слово{} общее тело{}", d % 991, d % 337));
            for f in 1..fields {
                for k in 0..arity.max(1) {
                    texts.push(if f % 2 == 0 {
                        format!("{}", d * 7 + k)
                    } else {
                        format!("м{}", (d + k) % 97)
                    });
                }
            }
            let mut at = 0usize;
            cells.push(Cell { field: 0, slot: 0, text: &texts[at] });
            at += 1;
            for f in 1..fields {
                for k in 0..arity.max(1) {
                    cells.push(Cell { field: f, slot: k as u32, text: &texts[at] });
                    at += 1;
                }
            }
            let arities: Vec<u32> = if arity > 1 { vec![1, arity as u32] } else { vec![1] };
            ix.add_cells(&format!("док:{d:09}"), &arities, &cells);
        }
        ix.flush_now();
        let seg = format!("{root}/{tag}");
        std::fs::remove_dir_all(&seg).ok();
        std::fs::rename(format!("{dir}/seg0000"), &seg).expect("сегмент");
        std::fs::remove_dir_all(&dir).ok();
        seg
    }

    #[test]
    fn the_estimate_covers_every_cell_of_the_matrix() {
        let root = std::env::temp_dir().join(format!("svindb-profile-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();
        let docs = 3_000usize;
        let mut worst = f64::MAX;
        let mut worst_cell = String::new();

        // Схемы: узкая без хранимого, широкая с хранимым, массив.
        // Мёртвых: нет и половина. Потоков сжатия: один и восемь.
        for &(fields, arity, stored) in &[(2usize, 1usize, false), (20, 1, true), (3, 24, false)] {
            for &dead in &[0usize, 2] {
                for &threads in &[1usize, 8] {
                    let tag = format!("f{fields}a{arity}s{stored}d{dead}t{threads}");
                    let a = build(&root, &format!("{tag}a"), 0, docs, fields, arity, stored);
                    let b = build(&root, &format!("{tag}b"), docs, docs, fields, arity, stored);
                    let (ia, ib) = (index::load(&a).expect("a"), index::load(&b).expect("b"));
                    if dead > 0 {
                        for x in [&ia, &ib] {
                            for k in 0..docs as u32 {
                                if k % dead as u32 == 0 {
                                    x.kill(k);
                                }
                            }
                        }
                    }
                    let estimate = peak_estimate(&[&ia, &ib]);
                    let out = format!("{root}/out-{tag}");
                    std::fs::remove_dir_all(&out).ok();
                    let mut got = None;
                    let peak = crate::alloc::peak(|| {
                        got = Some(
                            merge_as(&[&ia, &ib], None, &out, Limits { threads }).expect("слияние"),
                        );
                    });
                    let m = got.expect("итог");
                    if !m.peaks.is_empty() {
                        // Профиль включён — и тогда общий замер недействителен:
                        // отсечки фаз сбрасывают ту же высшую отметку, по
                        // которой считает внешний `peak`, и он видит лишь рост
                        // после последней из них. Сравнивать эти два числа
                        // между собой нельзя, поэтому проверка стороны
                        // неравенства здесь пропускается, а печатается разбивка
                        // — ровно то, ради чего профиль и включали.
                        let by: Vec<String> =
                            m.peaks.iter().map(|(n, b)| format!("{n} {b}")).collect();
                        eprintln!("{tag}: оценка {estimate} | {}", by.join(", "));
                    } else {
                        assert!(
                            estimate as usize >= peak,
                            "{tag}: оценка {estimate} Б ниже измеренного пика {peak} Б — \
                             узел взялся бы за работу, которая не влезает"
                        );
                        let ratio = estimate as f64 / peak.max(1) as f64;
                        if ratio < worst {
                            worst = ratio;
                            worst_cell = tag.clone();
                        }
                    }
                    for p in [&a, &b, &out] {
                        std::fs::remove_dir_all(p).ok();
                    }
                }
            }
        }
        // Запас не должен быть ни отрицательным, ни абсурдным: оценка, завышенная
        // на порядок, не защищает, а запрещает работу, которая поместилась бы.
        if worst < f64::MAX {
            assert!(worst >= 1.0, "худшая ячейка {worst_cell}: запас {worst:.3}");
            eprintln!("худшая ячейка {worst_cell}: запас {worst:.3}");
        }
        std::fs::remove_dir_all(&root).ok();
    }
}

#[cfg(test)]
mod peak_tests {
    use super::*;

    #[test]
    fn reclaim_does_not_average_nested_units_across_documents() {
        const UNITS: u32 = 100_000;
        let root =
            std::env::temp_dir().join(format!("svindb-skewed-nested-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        let mut writer = crate::Indexer::typed_stored(
            &root,
            vec!["body".into(), "items.value".into()],
            vec![1.0; 2],
            vec![crate::index::Kind::Text, crate::index::Kind::Int64],
            vec![false; 2],
            1000,
        );
        writer.set_groups(vec![0, 1], vec![String::new(), "items".into()]);
        let cells: Vec<_> =
            (0..UNITS).map(|slot| crate::indexer::Cell { field: 1, slot, text: "7" }).collect();
        writer.add_cells("large", &[1, UNITS], &cells);
        for doc in 0..99 {
            writer.add_cells(&format!("small-{doc}"), &[1, 0], &[]);
        }
        writer.flush_now();
        drop(writer);
        let source = crate::index::load(&format!("{root}/seg0000")).unwrap();
        let shape = Seg::of(&source);
        assert_eq!(shape.units, UNITS as u64);
        assert_eq!(shape.multi_values, 0, "тест должен проверять слоты, а не root multivalue");
        let required = peak_of(Shape {
            src_docs: 100,
            any_dead: true,
            groups: 1,
            out_docs: 1,
            units: UNITS as u64,
            fields: 2,
            ..Shape::default()
        });
        assert!(
            reclaim_part_peak(&source, 1) >= required,
            "неделимые слоты большого документа размазаны по 100 документам"
        );
        drop(source);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn root_multivalue_memory_is_not_estimated_as_one_scalar_per_document() {
        const VALUES: u64 = 1_000_000;
        let root = std::env::temp_dir().join(format!("svindb-multi-peak-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        let mut writer = crate::Indexer::typed_stored(
            &root,
            vec!["nums".into()],
            vec![1.0],
            vec![crate::index::Kind::Int64],
            vec![false],
            100,
        );
        let values = vec!["1".to_string(); VALUES as usize];
        let encoded = crate::indexer::encode_multi(&values);
        writer.add("big", &[&encoded]);
        for doc in 0..9 {
            writer.add(&doc.to_string(), &["1"]);
        }
        writer.flush_now();
        drop(writer);
        let source = crate::index::load(&format!("{root}/seg0000")).unwrap();
        let shape = Seg::of(&source);
        assert_eq!(shape.units, 10);
        assert_eq!(shape.multi_values, VALUES);
        let estimate = peak_estimate(&[&source]);
        assert!(estimate >= VALUES * PEAK_PER_MULTI_VALUE);
        assert!(
            reclaim_part_peak(&source, 1) >= VALUES * PEAK_PER_MULTI_VALUE,
            "массив одного документа размазан по всему корпусу"
        );
        let mut scalar = shape.clone();
        scalar.multi_values = 0;
        let window = Window::default();
        assert!(
            window.peak_with(&shape) >= window.peak_with(&scalar) + VALUES * PEAK_PER_MULTI_VALUE
        );
        let output = format!("{root}/merged");
        let measured = crate::alloc::peak(|| {
            merge_as(&[&source], None, &output, Limits { threads: 1 }).unwrap();
        });
        eprintln!("multivalue_values={VALUES} heap_peak={measured} estimate={estimate} legacy_estimate={}",
            estimate.saturating_sub(VALUES * PEAK_PER_MULTI_VALUE));
        assert!(estimate >= measured as u64, "оценка меньше измеренного heap peak");
        drop(source);
        std::fs::remove_dir_all(root).unwrap();
    }
    use crate::index::Kind;
    use crate::indexer::Indexer;

    /// Сегмент из одних поисковых полей, без хранимого.
    ///
    /// Хранимое выключено намеренно: перенос содержимого держит собранный кусок
    /// колонки в памяти целиком, и это отдельная величина с отдельной судьбой
    /// (см. `store::transfer`). Оставь её здесь — она заслонила бы собой ровно
    /// то, что меряется.
    fn seg(root: &str, name: &str, from: usize, n: usize) -> String {
        let dir = format!("{root}/{name}-build");
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed_stored(
            &dir,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            vec![Kind::Text, Kind::Text],
            vec![false, false],
            10_000_000,
        );
        for i in from..from + n {
            ix.add(
                // Длина идентификатора взята с настоящего корпуса: у `_id` в
                // виде URN десятки байт, и прежний путь платил за каждый ещё и
                // заголовком `String`.
                &format!("urn:svindb:документ:{i:09}:v1"),
                &[
                    &format!("загол{} загол{}", i % 97, i % 13),
                    &format!("сл{} сл{} сл{} общее тело {}", i % 991, i % 337, i % 53, i),
                ],
            );
        }
        ix.flush_now();
        let out = format!("{root}/{name}");
        std::fs::rename(format!("{dir}/seg0000"), &out).expect("сегмент перенесён");
        std::fs::remove_dir_all(&dir).ok();
        out
    }

    /// Наибольшее, что слияние держит **постоянно**, независимо от корпуса.
    ///
    /// Это не порог проверки, а число, от которого выбраны её размеры. Слияние
    /// копит списки в буфере на четыре мегабайта (`Vec::with_capacity(1 << 22)`
    /// в [`merge_as`]) и льёт потоки через [`POUR_BUF`]; вместе с мелочью это
    /// шесть миллионов байт — на корпусе в 250×2 документа замерено 6 098 977, и
    /// ниже высшая отметка не опускается ни на каком.
    ///
    /// Отсюда и слепое пятно, из-за которого прежние размеры проверки ничего не
    /// сторожили: величина, равная целому сегменту, до этой отметки просто не
    /// дотягивается, пока сегмент много мельче неё. На корпусе в 4 000×2
    /// документа контейнер выходит 210 080 Б — держи слияние хоть все его байты
    /// разом, пик не шелохнётся. Значит крупная точка обязана давать сегмент
    /// **того же порядка**, что и этот пол, иначе проверка меряет одни постоянные
    /// расходы.
    const FLOOR: usize = 6_000_000;

    /// Пик слияния не следует за размером сегмента.
    ///
    /// ## Что это ловит
    ///
    /// Слияние держало в памяти четыре вещи, линейные по размеру выхода: все
    /// идентификаторы отдельными строками, готовый блоб этих же
    /// идентификаторов, весь `pos.bin` и словарь в двух представлениях. Ни одна
    /// из них не отказывала и не портила вывод — они просто требовали столько
    /// же оперативной памяти, сколько весит сегмент, и упирали потолок
    /// `MAX_TARGET_DOCS` в пятьдесят миллионов.
    ///
    /// Переход на контейнер завёл этому семейству пятого родича, и не в самом
    /// слиянии: сводка сумм читала `seg.dat` целиком (`sums::compute`), то есть
    /// возвращала ровно то свойство, ради которого всё и делалось, — только в
    /// последней строке слияния, где смотреть уже никто не думал.
    ///
    /// ## Почему двумя точками и почему такими
    ///
    /// Одной точкой не проверить: постоянные расходы слияния (см. [`FLOOR`]) на
    /// маленьком сегменте больше всего остального. Поэтому берутся два корпуса,
    /// отличающихся вчетверо, и с них снимается **наклон** — постоянное
    /// вычитается само.
    ///
    /// Крупная точка выбрана так, чтобы сегмент дорос до [`FLOOR`]: 64 000×2
    /// документа дают контейнер 3 307 232 Б. Замерено делом: с чтением `seg.dat`
    /// целиком пик на этой точке был 8 068 161 Б против 6 586 495 Б без него, то
    /// есть разница видна и наклон уходит за девятнадцать байт на документ. На
    /// прежних 4 000 и 16 000 та же поломка не двигала пик вовсе: 6 129 050 и
    /// 6 224 379 Б с ней и без неё до последнего байта.
    ///
    /// ## Про порог
    ///
    /// Законно линейного по документам здесь единицы байт: полоса норм — байт на
    /// поле на документ, — карта перенумерации, `live.bin` и фильтр Блума в
    /// двенадцать бит. Замер даёт около четырёх; двенадцать — запас втрое, и
    /// вчетверо ниже одних только прежних идентификаторов (заголовок `String`
    /// плюс байты — под полсотни на документ).
    ///
    /// ## Про счётчик
    ///
    /// Считается запрошенное у распределителя, а не `RSS`: тот зависит от того,
    /// когда распределитель вернёт страницы системе, и мигал бы от прогона к
    /// прогону. Счётчик потоко-местный — ровно то, что нужно: все величины, о
    /// которых идёт речь, жили на нити слияния.
    #[test]
    fn the_merge_peak_does_not_follow_the_segment_size() {
        let root = std::env::temp_dir().join(format!("svindb-merge-peak-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        // Пик слияния и размер вышедшего сегмента: второе нужно затем, что вся
        // проверка держится на «сегмент вырос, а пик нет», и рост сегмента
        // обязан быть доказан, а не подразумеваться.
        let at = |docs: usize, tag: &str| -> (usize, u64) {
            let a = seg(&root, &format!("{tag}a"), 0, docs);
            let b = seg(&root, &format!("{tag}b"), docs, docs);
            let (ia, ib) = (index::load(&a).expect("a"), index::load(&b).expect("b"));
            let out = format!("{root}/{tag}-out");
            std::fs::remove_dir_all(&out).ok();
            let peak = crate::alloc::peak(|| {
                let done = merge(&[&ia, &ib], &out).expect("слияние");
                assert_eq!(done.docs, docs * 2, "слияние потеряло документы");
            });
            let size = std::fs::metadata(format!("{out}/{}", crate::container::FILE))
                .expect("контейнер слитого")
                .len();
            for d in [a, b, out] {
                std::fs::remove_dir_all(&d).ok();
            }
            (peak, size)
        };

        let (small, big) = (16_000usize, 64_000usize);
        let ((ps, cs), (pb, cb)) = (at(small, "s"), at(big, "b"));

        // Сегмент и правда вырос вчетверо — иначе сравнивать было бы нечего.
        assert!(cb > cs * 3, "корпус вырос вчетверо, а сегмент нет: {cs} Б против {cb} Б");
        // И крупная точка дотянулась до постоянных расходов — хотя бы до их
        // половины. Не дотянись — величина размером в целый сегмент спряталась бы
        // под ними, и проверка сторожила бы пустоту.
        assert!(
            cb as usize * 2 > FLOOR,
            "сегмент {cb} Б мельче половины постоянных расходов {FLOOR} Б: \
             под ними спрячется что угодно"
        );

        // Пик **не пропорционален**: корпус вчетверо, а высшая отметка на
        // считанные проценты. Держи слияние хоть один поток целиком — и это
        // отношение поползло бы к четырём.
        assert!(
            pb * 2 < ps * 3,
            "пик вырос с {ps} Б до {pb} Б на вчетверо большем корпусе — \
             он следует за размером сегмента"
        );

        // И то же самое числом: наклон по двум точкам.
        let per_doc = (pb.saturating_sub(ps)) as f64 / ((big - small) * 2) as f64;
        assert!(
            per_doc < 12.0,
            "пик слияния растёт на {per_doc:.1} Б/док: {small}×2 документов — {ps} Б \
             при сегменте {cs} Б, {big}×2 — {pb} Б при сегменте {cb} Б; \
             что-то снова копится целиком"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// Сегмент с **хранимым** полем: то, что сосед сверху выключает намеренно.
    pub(super) fn seg_stored(root: &str, name: &str, from: usize, n: usize) -> String {
        let dir = format!("{root}/{name}-build");
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed_stored(
            &dir,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            vec![Kind::Text, Kind::Text],
            vec![true, true],
            10_000_000,
        );
        for i in from..from + n {
            ix.add(
                &format!("urn:svindb:документ:{i:09}:v1"),
                &[
                    &format!("загол{} загол{}", i % 97, i % 13),
                    &format!("сл{} сл{} сл{} общее тело {}", i % 991, i % 337, i % 53, i),
                ],
            );
        }
        ix.flush_now();
        let out = format!("{root}/{name}");
        std::fs::rename(format!("{dir}/seg0000"), &out).expect("сегмент перенесён");
        std::fs::remove_dir_all(&dir).ok();
        out
    }

    /// Пик слияния **с хранимым** растёт не быстрее самого сегмента.
    ///
    /// ## Зачем отдельно от соседа сверху
    ///
    /// Тот меряет всё, кроме содержимого, и выключает его нарочно: собранный
    /// кусок колонки заслонил бы собой остальное. Но выключенное — не значит
    /// безобидное. У содержимого два пути, и они разной природы: перенос льёт
    /// куски в файл, пересборка собирает их буферами и кладёт готовыми. Второй
    /// достижим в обычной работе — перенос отказывается на мелких словарях и на
    /// сильно погашенных полях, то есть ровно там, где живёт update-heavy
    /// индекс.
    ///
    /// ## Почему порог опускается на переносе — и почему без этого проверка врала
    ///
    /// `plan_carry` требует от источника словарь крупнее `DICT_MIN`, а на
    /// колонке в пару мегабайт он зажат ровно в него. Без
    /// [`crate::store::lower_carry_floor`] перенос не срабатывает вовсе, и обе
    /// половины проверки идут через пересборку — давая, разумеется, один и тот
    /// же наклон. Ровно так и было: «оба пути держат содержимое одинаково» —
    /// вывод, сделанный из замера, который мерил один путь дважды.
    ///
    /// Отсюда сверка `packed`: у чистого переноса он ноль, у пересборки — число
    /// уложенных кусков. Она стоит здесь не ради полноты, а чтобы этот обман
    /// нельзя было повторить молча.
    ///
    /// ## Почему отношение, а не байты на документ
    ///
    /// Байты на документ зависят от корпуса: на коротком теле выходило 182
    /// Б/док, на теле вчетверо длиннее — 449. Величина, которая от корпуса не
    /// зависит, — во сколько раз пик обгоняет **сам сегмент**.
    ///
    /// Замер 12 августа 2026 после того, как обучающее сырьё стало отпускаться
    /// сразу за обучением, а очередь сжатия — ограничиваться байтами: перенос
    /// 0.09, пересборка 2.13. Порог — четыре, то есть почти вдвое к худшему.
    /// Уйдёт выше — значит завелась ещё одна копия целого куска, и её надо
    /// искать, а не поднимать порог.
    ///
    /// ## Про счётчик
    ///
    /// Считается запрошенное у распределителя ([`crate::alloc::peak`]), а не
    /// `RSS`: последний включает страницы отображённых источников, которые ядро
    /// забирает обратно, и мерил бы не потребность, а погоду. Счётчик
    /// потоко-местный, поэтому у параллельной ветки сжатия он видит не всё:
    /// тела, выделенные рабочими нитями, в него не попадают, а освобождение их
    /// — попадает. Числа двух ширин между собой построчно не сопоставимы.
    /// Оценка обязана быть сверху и на схеме с **массивом**.
    ///
    /// ## Почему это отдельная проверка, а не ещё одна ширина
    ///
    /// Соседи меряют схемы, где у каждого документа ровно один слот на поле.
    /// Там число единиц равно числу документов, и всякий член, линейный по
    /// единицам, неотличим от члена, линейного по документам, — формула их
    /// путает и всё равно попадает.
    ///
    /// Поле внутри массива это ломает: у документа не один элемент, а
    /// несколько, и сборка колонки заводит на **каждый элемент** восемь байт
    /// значения, байт признака и шестнадцать байт пары «где, что» — то есть
    /// два с половиной десятка байт на элемент, а не на документ. При арности
    /// сорок восемь это тысяча с лишним байт на документ там, где формула
    /// считает один.
    ///
    /// Если оценка окажется ниже настоящего пика, узел возьмётся за работу,
    /// которая не влезает, и его снимет ядро посреди неё — вместе со всеми
    /// соединениями. Поэтому проверка не про точность, а про сторону.
    #[test]
    fn the_estimate_stays_above_the_real_peak_on_an_array_schema() {
        use crate::indexer::Cell;
        let root = std::env::temp_dir().join(format!("svindb-peak-arr-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        // Арность растёт: так видно, следует ли оценка за числом единиц или
        // за одним лишь числом документов.
        for arity in [1usize, 8, 48] {
            let build = |tag: &str, from: usize, n: usize| -> String {
                let dir = format!("{root}/{tag}-build");
                std::fs::remove_dir_all(&dir).ok();
                let mut ix = Indexer::typed(
                    &dir,
                    vec!["тело".into(), "метка".into(), "счёт".into()],
                    vec![1.0, 1.0, 1.0],
                    vec![Kind::Text, Kind::Keyword, Kind::Int64],
                    10_000_000,
                );
                // Два колоночных поля внутри группы «отклики»: колонка строится
                // по единицам группы, а не по документам.
                ix.set_groups(vec![0, 1, 1], vec![String::new(), "отклики".into()]);
                for d in from..from + n {
                    let mut cells = vec![Cell {
                        field: 0, slot: 0, text: "документ тело слово"
                    }];
                    let marks: Vec<String> =
                        (0..arity).map(|k| format!("м{}", (d + k) % 97)).collect();
                    let counts: Vec<String> =
                        (0..arity).map(|k| format!("{}", d * 7 + k)).collect();
                    for k in 0..arity {
                        cells.push(Cell { field: 1, slot: k as u32, text: &marks[k] });
                        cells.push(Cell { field: 2, slot: k as u32, text: &counts[k] });
                    }
                    ix.add_cells(&format!("док:{d:09}"), &[1, arity as u32], &cells);
                }
                ix.flush_now();
                let out = format!("{root}/{tag}");
                std::fs::remove_dir_all(&out).ok();
                std::fs::rename(format!("{dir}/seg0000"), &out).expect("сегмент");
                std::fs::remove_dir_all(&dir).ok();
                out
            };
            let docs = 4_000usize;
            let a = build(&format!("a{arity}"), 0, docs);
            let b = build(&format!("b{arity}"), docs, docs);
            let (ia, ib) = (index::load(&a).expect("a"), index::load(&b).expect("b"));
            assert_eq!(ia.slots[1].units(), docs * arity, "единиц группы не столько");
            let estimate = peak_estimate(&[&ia, &ib]);
            let out = format!("{root}/out{arity}");
            std::fs::remove_dir_all(&out).ok();
            let peak = crate::alloc::peak(|| {
                merge(&[&ia, &ib], &out).expect("слияние");
            });
            assert!(
                estimate as usize >= peak,
                "арность {arity}: оценка {estimate} Б ниже измеренного пика {peak} Б — \
                 узел взялся бы за работу, которая не влезает"
            );
        }
        std::fs::remove_dir_all(&root).ok();
    }

    /// Оценка обязана быть **сверху** от настоящего пика — и на широкой схеме
    /// тоже.
    ///
    /// ## Чем это отличается от соседних проверок
    ///
    /// Соседи меряют, не растёт ли пик быстрее сегмента. Они верны, но слепы к
    /// одному: обе калиброваны на схеме из **двух** полей. Полоса норм стоит
    /// байт на документ на поле, и на восемнадцати полях это восемнадцать
    /// байт — против общей константы в двенадцать, которая должна была покрыть
    /// вообще всё. То есть оценка была занижена именно там, где слияние дороже
    /// всего, а проверить это было нечем.
    ///
    /// Здесь сверяется само обещание: `peak_estimate` против измеренного
    /// `alloc::peak` на схемах разной ширины.
    #[test]
    fn the_estimate_stays_above_the_real_peak_on_a_wide_schema() {
        let root = std::env::temp_dir().join(format!("svindb-peak-wide-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        // Одно и то же число документов при разной ширине схемы: так видно, что
        // оценка следует за шириной, а не за одним лишь размером.
        for nfields in [2usize, 8, 18] {
            let build = |tag: &str, from: usize, n: usize| -> String {
                let dir = format!("{root}/{tag}-build");
                std::fs::remove_dir_all(&dir).ok();
                let names: Vec<String> = (0..nfields).map(|f| format!("поле{f}")).collect();
                let mut ix = Indexer::typed_stored(
                    &dir,
                    names,
                    vec![1.0; nfields],
                    vec![Kind::Text; nfields],
                    vec![true; nfields],
                    10_000_000,
                );
                for i in from..from + n {
                    let vals: Vec<String> = (0..nfields)
                        .map(|f| format!("сл{} сл{} тело{} поле{f}", i % 991, i % 337, i))
                        .collect();
                    let refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
                    ix.add(&format!("док:{i:09}"), &refs);
                }
                ix.flush_now();
                let out = format!("{root}/{tag}");
                std::fs::remove_dir_all(&out).ok();
                std::fs::rename(format!("{dir}/seg0000"), &out).expect("сегмент");
                std::fs::remove_dir_all(&dir).ok();
                out
            };
            let docs = 6_000usize;
            let a = build(&format!("a{nfields}"), 0, docs);
            let b = build(&format!("b{nfields}"), docs, docs);
            let (ia, ib) = (index::load(&a).expect("a"), index::load(&b).expect("b"));
            let estimate = peak_estimate(&[&ia, &ib]);
            let out = format!("{root}/out{nfields}");
            std::fs::remove_dir_all(&out).ok();
            let peak = crate::alloc::peak(|| {
                merge(&[&ia, &ib], &out).expect("слияние");
            });
            assert!(
                estimate as usize >= peak,
                "{nfields} полей: оценка {estimate} Б ниже измеренного пика {peak} Б — \
                 узел взялся бы за работу, которая не влезает"
            );
            // И не абсурдно выше: оценка, завышенная на порядок, не защищает, а
            // запрещает работу, которая прекрасно поместилась бы. Именно из-за
            // этого уборка замороженного сегмента не запускалась никогда.
            assert!(
                estimate as usize <= peak.saturating_mul(60).max(64 << 20),
                "{nfields} полей: оценка {estimate} Б против пика {peak} Б — \
                 запас потерял смысл"
            );
            for d in [a, b, out] {
                std::fs::remove_dir_all(&d).ok();
            }
        }
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn the_merge_peak_with_stored_content_grows_no_faster_than_the_segment() {
        let root =
            std::env::temp_dir().join(format!("svindb-merge-peak-stored-{}", std::process::id()));
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        let at = |docs: usize, tag: &str, rebuild: bool| -> (usize, u64) {
            let a = seg_stored(&root, &format!("{tag}a"), 0, docs);
            let b = seg_stored(&root, &format!("{tag}b"), docs, docs);
            let (ia, ib) = (index::load(&a).expect("a"), index::load(&b).expect("b"));
            let out = format!("{root}/{tag}-out");
            std::fs::remove_dir_all(&out).ok();
            let mut packed = 0usize;
            let peak = crate::alloc::peak(|| {
                let _forced = rebuild.then(Rebuilding::on);
                // Перенос отказывается на мелком словаре: `plan_carry` требует
                // от источника словарь крупнее `DICT_MIN`, а на колонке в пару
                // мегабайт он зажат ровно в него. Без опущенного порога оба
                // «пути» шли бы через пересборку — что и было, и что дало
                // одинаковый наклон.
                let _floor = (!rebuild).then(|| crate::store::lower_carry_floor(2, 0));
                let done = merge(&[&ia, &ib], &out).expect("слияние");
                assert_eq!(done.docs, docs * 2, "слияние потеряло документы");
                packed = done.packed;
            });
            // Путь обязан быть тем, за который его выдают.
            if rebuild {
                assert!(packed > 0, "пересборка не уложила ни куска — мерился не тот путь");
            } else {
                assert_eq!(packed, 0, "перенос не сработал: {packed} кусков уложено заново");
            }
            let size = std::fs::metadata(format!("{out}/{}", crate::container::FILE))
                .expect("контейнер слитого")
                .len();
            for d in [a, b, out] {
                std::fs::remove_dir_all(&d).ok();
            }
            (peak, size)
        };

        // Вчетверо меньше соседа сверху: там размер выбран так, чтобы сегмент
        // дорос до постоянных расходов, а здесь считается **отношение** роста к
        // росту, и постоянное вычитается само. Замер на этих же размерах давал
        // те же 5.2 раза.
        let (small, big) = (8_000usize, 32_000usize);
        for (way, rebuild) in [("перенос", false), ("пересборка", true)] {
            let ((ps, cs), (pb, cb)) = (
                at(small, &format!("s{rebuild}"), rebuild),
                at(big, &format!("b{rebuild}"), rebuild),
            );
            // Сегмент и правда вырос — иначе отношение считать не от чего.
            assert!(
                cb > cs * 3,
                "{way}: корпус вырос вчетверо, а сегмент нет: {cs} Б против {cb} Б"
            );
            let grew_peak = pb.saturating_sub(ps) as f64;
            let grew_seg = cb.saturating_sub(cs) as f64;
            let times = grew_peak / grew_seg;
            assert!(
                times < 4.0,
                "{way}: пик обгоняет сегмент в {times:.1} раза — {small}×2 дали пик {ps} Б при \
                 сегменте {cs} Б, {big}×2 — пик {pb} Б при сегменте {cb} Б; \
                 завелась ещё одна копия целого куска"
            );
        }
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Потолок смещения на **пути слияния**, а не в предикате.
#[cfg(test)]
mod ceiling_wiring_tests {
    use super::*;
    use crate::index::Kind;
    use crate::indexer::Indexer;

    /// Обычный маленький сегмент из одного текстового поля.
    ///
    /// Без вложенных полей намеренно: полосы арности у такого нет, и опущенный
    /// потолок доходит до слов, а не упирается в `slots::encode` раньше.
    fn seg(root: &str, name: &str, from: usize) -> String {
        let dir = format!("{root}/{name}-build");
        std::fs::remove_dir_all(&dir).ok();
        let mut ix =
            Indexer::typed(&dir, vec!["text".into()], vec![1.0], vec![Kind::Text], 1_000_000);
        for i in from..from + 200 {
            ix.add(&format!("{name}{i}"), &[&format!("слово{i} общее тело документа")]);
        }
        ix.flush_now();
        let out = format!("{root}/{name}");
        std::fs::rename(format!("{dir}/seg0000"), &out).expect("сегмент перенесён");
        std::fs::remove_dir_all(&dir).ok();
        out
    }

    /// Охрана стоит **на пути записи** слияния, а не рядом с ним.
    ///
    /// ## Чего не проверяла проверка предиката
    ///
    /// `fits_offset` закрыт точными числами, и это правильно: края у него
    /// несущие. Но точные числа одинаковы и при вызванной охране, и при снятой:
    /// вычеркни все три строки из `merge` — и набор останется зелёным, а
    /// завёрнутое смещение ляжет в `terms.idx` при первом же укрупнении за
    /// четыре гигабайта. Собрать столько в проверке нечем, поэтому опускается
    /// потолок: два обычных двухсотдокументных сегмента, потолок в сотню байт —
    /// и слияние обязано отказать, назвав поток и выход.
    ///
    /// Перед этим те же сегменты сливаются при настоящем потолке: иначе
    /// проверка приняла бы за охрану любой другой отказ слияния.
    #[test]
    fn a_merge_refuses_the_moment_a_stream_crosses_the_ceiling() {
        let root = std::env::temp_dir().join("svindb-merge-ceiling");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        let a = seg(&root, "a", 0);
        let b = seg(&root, "b", 200);
        let (ia, ib) = (index::load(&a).unwrap(), index::load(&b).unwrap());

        let done = merge(&[&ia, &ib], &format!("{root}/mrg-ok"))
            .expect("при настоящем потолке те же сегменты обязаны слиться");
        assert_eq!(done.docs, 400, "слияние потеряло документы ещё до проверки потолка");

        let _low = crate::dict::lower_offset_ceiling(100);
        let msg = match merge(&[&ia, &ib], &format!("{root}/mrg")) {
            Ok(_) => panic!("охрана не на пути записи: смещение за потолком легло в опору"),
            Err(e) => e.to_string(),
        };
        // Какой из трёх потоков упрётся первым, задаёт корпус, и закреплять это
        // здесь незачем. Закрепляется другое: отказал именно потолок слова, а не
        // что-нибудь ещё, — и назвал поток, потолок и выход.
        //
        // Слово «сегмента» в образце не для красоты: под опущенным потолком
        // упирается и словарь выхода, а его отказ начинается со «словарь» — и по
        // одному «слова» неотличим от нашего. Проверка, которую закрывает чужой
        // отказ, доказывает не то, что написано в её имени.
        assert!(
            ["список сегмента", "позиции сегмента", "слова сегмента"]
                .iter()
                .any(|w| msg.contains(w)),
            "отказал не потолок потока слова: {msg}"
        );
        assert!(msg.contains("при потолке 100"), "отказ померил не тем потолком: {msg}");
        assert!(msg.contains("--target"), "отказ не подсказал выход: {msg}");
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Слияние сегментов с массивами: ординалы перенумеровываются.
#[cfg(test)]
mod slot_merge_tests {
    use super::*;
    use crate::index::Kind;
    use crate::indexer::{Cell, Indexer};

    fn seg(root: &str, name: &str, from: i64, n: i64) -> String {
        let dir = format!("{root}/{name}-build");
        std::fs::remove_dir_all(&dir).ok();
        let mut ix = Indexer::typed(
            &dir,
            vec!["text".into(), "emoji".into(), "count".into()],
            vec![1.0; 3],
            vec![Kind::Text, Kind::Keyword, Kind::Int32],
            1_000_000,
        );
        ix.set_groups(vec![0, 1, 1], vec![String::new(), "reactions".into()]);
        for d in from..from + n {
            // Число элементов гуляет: слияние обязано пережить и пустой массив.
            let k = (d % 3) as u32;
            let mut cells = vec![Cell { field: 0, slot: 0, text: "документ" }];
            let counts: Vec<String> = (0..k).map(|s| (d * 10 + s as i64).to_string()).collect();
            for s in 0..k {
                cells.push(Cell { field: 1, slot: s, text: if s == 0 { "up" } else { "fire" } });
                cells.push(Cell { field: 2, slot: s, text: &counts[s as usize] });
            }
            ix.add_cells(&format!("d{d}"), &[1, k], &cells);
        }
        ix.flush_now();
        let out = format!("{root}/{name}");
        std::fs::rename(format!("{dir}/seg0000"), &out).unwrap();
        std::fs::remove_dir_all(&dir).ok();
        out
    }

    /// После слияния каждый элемент несёт своё значение, а не соседское.
    ///
    /// Ординал принадлежит сегменту: перенеси его слияние как есть — и значения
    /// второго источника легли бы поверх первого. Проявилось бы это не отказом,
    /// а чужими числами у каждого элемента.
    #[test]
    fn ordinals_are_renumbered_and_values_follow_their_elements() {
        let root = std::env::temp_dir().join("svindb-merge-slots");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        let a = seg(&root, "a", 0, 300);
        let b = seg(&root, "b", 300, 300);
        let (ia, ib) = (index::load(&a).unwrap(), index::load(&b).unwrap());
        let out = format!("{root}/mrg");
        let res = merge(&[&ia, &ib], &out).expect("слияние сегментов с массивами");
        assert_eq!(res.docs, 600);

        let m = index::load(&out).expect("слитый открылся");
        let want_units: usize = (0..600i64).map(|d| (d % 3) as usize).sum();
        assert_eq!(m.slots[1].units(), want_units, "число слотов не сошлось");

        let col = m.fields[2].column.as_ref().expect("колонка счётчика");
        for d in 0..600u32 {
            let k = (d as i64 % 3) as u32;
            let (from, n) = m.slots[1].range(d);
            assert_eq!(n, k, "арность документа {d}");
            for s in 0..n {
                assert_eq!(
                    col.get(from + s),
                    Some(d as i64 * 10 + s as i64),
                    "документ {d}, элемент {s}"
                );
            }
        }
        assert_eq!(crate::check::check(&out, crate::check::Depth::Full).broken(), 0);
        std::fs::remove_dir_all(&root).ok();
    }

    /// Путь массива переживает слияние.
    ///
    /// Прежде слияние писало в заголовок пустой третий столбец, и путь пропадал
    /// при первом же укрупнении. Пустой путь у некорневой группы заголовок
    /// принимал, значит потеря была молчаливой, а запрос после неё не мог
    /// назвать группу ничем.
    #[test]
    fn the_array_path_survives_the_merge() {
        let root = std::env::temp_dir().join("svindb-merge-grouppath");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        let a = seg(&root, "a", 0, 30);
        let b = seg(&root, "b", 30, 30);
        let (ia, ib) = (index::load(&a).unwrap(), index::load(&b).unwrap());
        // У источников путь на месте — иначе проверка ниже доказывала бы лишь
        // то, что его нет ни там, ни там.
        assert_eq!(ia.group_paths[1], "reactions", "у источника путь потерян ещё до слияния");

        let out = format!("{root}/mrg");
        merge(&[&ia, &ib], &out).expect("слияние");
        let m = index::load(&out).expect("слитый открылся");
        assert_eq!(m.group_paths[1], "reactions", "путь массива не пережил слияние");
        assert_eq!(m.group_paths[0], "", "у корневой группы путь обязан быть пуст");
        std::fs::remove_dir_all(&root).ok();
    }

    /// Источники, расходящиеся в пути одного номера, не сливаются.
    ///
    /// Один номер, означающий в двух сегментах разные массивы, — это два разных
    /// индекса в одном каталоге. Слить их значило бы ответить по одной группе
    /// данными другой.
    #[test]
    fn sources_that_disagree_about_the_array_path_are_refused() {
        let root = std::env::temp_dir().join("svindb-merge-grouppath-clash");
        let root = root.to_string_lossy().to_string();
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).unwrap();

        let a = seg(&root, "a", 0, 30);
        let b = seg(&root, "b", 30, 30);
        // Путь второго подменяется в заголовке: так же он разошёлся бы, собери
        // его другая версия схемы.
        let head = format!("{b}/head.tsv");
        let text = std::fs::read_to_string(&head).unwrap().replace("reactions", "comments");
        std::fs::write(&head, text).unwrap();

        let (ia, ib) = (index::load(&a).unwrap(), index::load(&b).unwrap());
        assert_eq!(ib.group_paths[1], "comments", "подмена не доехала — проверять нечего");
        let says = match merge(&[&ia, &ib], &format!("{root}/mrg")) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("расхождение путей сошло с рук"),
        };
        assert!(
            says.contains("reactions") && says.contains("comments"),
            "отказ не назвал оба пути: {says}"
        );
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Перенос хранимого при слиянии: содержимое обязано совпасть с пересборкой.
///
/// ## Почему здесь нет побайтовой сверки с эталоном
///
/// Она не работает **по построению**. Перенесённый кусок сжат словарём
/// источника; собранный заново — словарём, обученным на слитой колонке. Байты
/// разные всегда, и это не порча, а смысл операции.
///
/// Взамен стоит дифференциальный оракул: старый путь никуда не делся (он за
/// [`rebuild_wanted`]), и два пути обязаны давать **одно и то же содержимое** на
/// каждом документе каждого поля. Оракул этот сильнее побайтовой сверки в том,
/// что ловит и перенумерацию, и потерю документа, и подмену словаря, — и слабее
/// в том, что не замечает разницы в раскладке. Раскладку закрывают проверки в
/// `store::transfer`.
#[cfg(test)]
mod carrying {
    use super::*;
    use crate::indexer::Indexer;
    use crate::segments::Segments;

    fn fresh(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-mcarry-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).expect("каталог");
        d.to_string_lossy().to_string()
    }

    /// Откуда сегмент берёт куски: контейнер, если заголовок его называет.
    ///
    /// Проверки здесь смотрят на куски сегмента — тела хранимого, словари,
    /// засечки, — а лежат они внутри `seg.dat`, а не отдельными файлами. Идти к
    /// ним тем же путём, каким идёт читатель, а не по имени файла в каталоге, —
    /// единственный способ не проверять устройство, которого больше нет.
    fn src_of(dir: &str) -> crate::container::SegmentSource {
        let head = index::read_head(dir).expect("заголовок сегмента");
        crate::container::SegmentSource::of(dir, head.container).expect("куски сегмента")
    }

    /// Кусок сегмента по имени. `None` — куска нет вовсе.
    ///
    /// «Куска нет» и «кусок пуст» здесь обязаны различаться: на этом стоит
    /// половина проверок ниже — таблицы словарей при одном словаре быть не
    /// должно, а пустое хранилище это законный кусок нулевой длины.
    fn piece(dir: &str, name: &str) -> Option<Vec<u8>> {
        let src = src_of(dir);
        let r = src.try_open(name)?;
        Some(r.slice(0, r.len_bytes()).to_vec())
    }

    /// Пересобирает контейнер сегмента, подменив один кусок. `None` — убрать.
    ///
    /// Портить сегмент в проверке иначе нечем: кусок лежит внутри `seg.dat`,
    /// длина его записана в оглавлении, а длина всего файла — в `head.tsv`.
    /// Правка байтов на месте годится только там, где длина не меняется, а
    /// проверкам нужно и укоротить засечки, и убрать суммы совсем. Поэтому
    /// контейнер собирается заново из тех же кусков — так подделка остаётся
    /// **правдоподобной**: сегмент открывается, и споткнуться о него обязан тот
    /// разбор, ради которого проверка и заведена, а не сторож контейнера.
    fn reforge(dir: &str, name: &str, body: Option<&[u8]>) {
        let path = format!("{dir}/{}", crate::container::FILE);
        let whole = std::fs::read(&path).expect("контейнер сегмента");
        let table = crate::container::Table::parse(&path, &whole).expect("оглавление");
        // Версия раскладки берётся у самого контейнера: подделывается кусок, а
        // не заголовок.
        let layout = u32::from_le_bytes(whole[12..16].try_into().expect("четыре байта"));
        let mut pieces: Vec<(String, Vec<u8>)> = table
            .members()
            .iter()
            .map(|m| (m.name.clone(), whole[m.off as usize..(m.off + m.len) as usize].to_vec()))
            .collect();
        match (pieces.iter().position(|(n, _)| n == name), body) {
            (Some(i), Some(b)) => pieces[i].1 = b.to_vec(),
            (Some(i), None) => {
                pieces.remove(i);
            }
            (None, Some(b)) => pieces.push((name.to_string(), b.to_vec())),
            (None, None) => panic!("куска {name} и так нет — убирать нечего"),
        }
        let mut w = crate::container::Writer::create(dir, layout).expect("новый контейнер");
        for (n, b) in pieces.iter() {
            w.put(n, b).expect("кусок");
        }
        let total = w.finish().expect("контейнер закрыт");
        // Заголовок обещает длину контейнера, и она изменилась. Оставь его как
        // был — и сегмент не открылся бы вовсе, то есть проверка ловила бы
        // расхождение длин вместо того, ради чего заведена.
        let head_path = format!("{dir}/head.tsv");
        let text = std::fs::read_to_string(&head_path).expect("заголовок");
        let fixed: String = text
            .lines()
            .map(|l| {
                if l.starts_with("container\t") {
                    format!("container\t{}\t{total}\n", crate::container::FILE)
                } else {
                    format!("{l}\n")
                }
            })
            .collect();
        std::fs::write(&head_path, fixed).expect("заголовок");
        crate::sums::write(dir).expect("сводка");
    }

    /// Индексатор с крупным хранимым полем, по которому не ищут.
    ///
    /// Искать по нему незачем: проверяется хранилище, а разбор десяти мегабайт
    /// текста на слова стоил бы вдвое дороже всего остального в проверке.
    fn writer(dir: &str, per_seg: u32) -> Indexer<crate::analyze::Simple> {
        let mut ix = Indexer::typed_positioned(
            dir,
            vec!["title".into(), "body".into()],
            vec![2.0, 1.0],
            vec![index::Kind::Text, index::Kind::Text],
            vec![true, true],
            vec![false, false],
            per_seg,
        );
        ix.set_searched(vec![true, false]);
        ix
    }

    /// Тело документа: около двух килобайт, и сжимается как настоящий текст.
    fn body(i: u32) -> String {
        (0..300).map(|k| format!("слово{} ", (i as usize * 7 + k) % 91)).collect()
    }

    /// Корпус с колонкой **за восемь мегабайт** на сегмент: настоящий порог.
    fn heavy(dir: &str, docs: u32, per_seg: u32) {
        let mut ix = writer(dir, per_seg);
        for i in 0..docs {
            ix.add(&format!("d{i:06}"), &[&format!("док{}", i % 97), &body(i)]);
        }
        ix.flush_now();
    }

    fn open(dir: &str) -> Segments {
        Segments::load(dir).expect("индекс открылся")
    }

    fn refs(segs: &Segments) -> Vec<&Index> {
        segs.list.iter().map(|a| a.as_ref()).collect()
    }

    /// Сливает в свежий каталог-держатель и публикует список, чтобы вывод можно
    /// было открыть как индекс.
    fn merged(sources: &[&Index], holder: &str, rebuild: bool) -> (Merged, String) {
        let out = format!("{holder}/mrg0000");
        let res = if rebuild {
            let _g = Rebuilding::on();
            merge(sources, &out).expect("слияние")
        } else {
            merge(sources, &out).expect("слияние")
        };
        crate::manifest::write(holder, &["mrg0000".to_string()]).expect("список");
        (res, out)
    }

    /// Оракул: два вывода обязаны отдавать одно и то же на каждом документе и
    /// каждом поле, и одно и то же пачкой.
    fn same_content(a: &str, b: &str) {
        let ia = index::load(a).expect("перенос открылся");
        let ib = index::load(b).expect("пересборка открылась");
        assert_eq!(ia.docs, ib.docs, "число документов разошлось");
        assert_eq!(ia.fields.len(), ib.fields.len(), "число полей разошлось");
        for d in 0..ia.docs as u32 {
            for f in 0..ia.fields.len() {
                assert_eq!(
                    ia.fetch(d, f),
                    ib.fetch(d, f),
                    "документ {d}, поле {} разошлось",
                    ia.fields[f].name
                );
            }
        }
        // И пачкой: у `Fetcher` свой путь к куску — группировка по кускам, свой
        // кеш, свои потоки, — и разъехаться с точечной выдачей он может
        // независимо.
        let ha = crate::manifest::name_of(a);
        let hb = crate::manifest::name_of(b);
        let (da, db) = (a.trim_end_matches(&ha), b.trim_end_matches(&hb));
        let (sa, sb) = (open(da.trim_end_matches('/')), open(db.trim_end_matches('/')));
        let docs: Vec<u64> = (0..ia.docs as u64).rev().step_by(7).collect();
        for f in 0..ia.fields.len() {
            for threads in [1usize, 4] {
                assert_eq!(
                    sa.fetcher(1 << 20).many(&docs, f, threads),
                    sb.fetcher(1 << 20).many(&docs, f, threads),
                    "пачка разошлась на поле {f} при {threads} потоках"
                );
            }
        }
    }

    /// Слияние просит ровно ту ширину, которую ему дали.
    ///
    /// ## Почему это проверяется, а не оставляется на совесть
    ///
    /// Сборка хранимого поднимает `available_parallelism()` потоков, то есть все
    /// ядра узла. Слияние — работа фоновая, и занимать ими весь узел оно не
    /// вправе: приём сжимает своё содержимое теми же ядрами. Незаметно вернуться
    /// к `store::build` ничего не стоит — вывод от числа потоков не зависит
    /// побайтово, — и проверка на содержимое такой возврат не увидела бы.
    /// Поэтому смотрится сам запрос.
    ///
    /// Пересборка, а не перенос: перенос кусков компрессор почти не зовёт, и
    /// мерять там нечего.
    #[test]
    fn a_merge_asks_for_the_width_it_was_given() {
        let root = fresh("threads");
        heavy(&root, 400, 200);
        let segs = open(&root);
        let r = refs(&segs);

        let narrow = fresh("threads-narrow");
        let res = {
            let _g = Rebuilding::on();
            merge_as(&r, None, &format!("{narrow}/mrg0000"), Limits { threads: 2 })
                .expect("слияние")
        };
        assert!(res.packed > 0, "хранимое не пересобиралось — смотреть не на что");
        assert_eq!(
            crate::store::LAST_THREADS.with(|c| c.get()),
            2,
            "слияние попросило не ту ширину, что ему задали"
        );

        // Без ограничения — сколько решит машина: `svindb merge` и стенды идут
        // на переднем плане, и сужать их незачем.
        let wide = fresh("threads-wide");
        {
            let _g = Rebuilding::on();
            merge(&r, &format!("{wide}/mrg0000")).expect("слияние");
        }
        let all = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert_eq!(
            crate::store::LAST_THREADS.with(|c| c.get()),
            all,
            "слияние без ограничения сузило себя само"
        );
        std::fs::remove_dir_all(&root).ok();
        std::fs::remove_dir_all(&narrow).ok();
        std::fs::remove_dir_all(&wide).ok();
    }

    /// Главная проверка: перенос и пересборка дают **одно содержимое**, а работы
    /// делают разное.
    ///
    /// Корпус здесь при **настоящем** пороге — колонка за восемь мегабайт на
    /// сегмент, — потому что заодно доказывается, что сторож против россыпи
    /// пропускает то, что должен: мелкое поле `title` в том же сегменте
    /// пересобирается, крупное `body` переносится.
    #[test]
    fn carrying_and_rebuilding_agree_on_every_document() {
        let src = fresh("oracle");
        heavy(&src, 4800, 2400);
        let segs = open(&src);
        assert_eq!(segs.list.len(), 2, "нужно ровно два сегмента");
        let r = refs(&segs);
        let big = r[0].fields.iter().position(|f| f.name == "body").expect("поле body");
        let st = r[0].fields[big].store.as_ref().expect("хранилище источника");
        assert!(
            st.blocks() >= crate::store::CARRY_MIN_BLOCKS
                && st.dict_bytes(0).is_some_and(|b| b > crate::store::DICT_MIN),
            "источник не дорос до настоящего порога: кусков {}, словарь {:?}",
            st.blocks(),
            st.dict_bytes(0)
        );

        let ha = fresh("oracle-a");
        let hb = fresh("oracle-b");
        let (ca, a) = merged(&r, &ha, false);
        let (cb, b) = merged(&r, &hb, true);

        assert_eq!(ca.docs, cb.docs);
        same_content(&a, &b);

        // Перенос не только наливает куски в контейнер, но и **сносит** файлы,
        // из которых наливал. Не снеси — сегмент держал бы каждый поток
        // хранимого дважды, файлом и куском: место на носителе и размер снимка
        // удваиваются, а число областей отображения возвращается к прежнему, то
        // есть пропадает ровно то, ради чего контейнер заведён. Само по себе это
        // не всплывает нигде: `check::inventory` знает имена `body.smrk`,
        // `body.sdic`, `body.scrc`, `body.sdix` и лишними их не назовёт, а сводка
        // сумм перечисляет и файлы каталога, и куски, и примет обе копии.
        //
        // Проверка стоит здесь, а не рядом с `a_merged_segment_is_one_file...`:
        // та идёт путём пересборки, где куски кладутся буферами, и до сноса не
        // доходит вовсе.
        assert_eq!(
            dir_names(&a),
            outside_names(),
            "перенос оставил в каталоге файлы, из которых налил куски"
        );

        // Работа при этом разная, и разница видна числом сжатых кусков.
        assert!(
            ca.packed * 20 < cb.packed,
            "перенос сжал {} кусков против {} у пересборки — переносить, значит, нечего",
            ca.packed,
            cb.packed
        );

        // Крупное поле перенесено байт в байт: вывод — побайтовая склейка тел
        // источников, а словари легли подряд.
        let stem = &r[0].fields[big].stem;
        let mut want_bin = Vec::new();
        let mut want_dic = Vec::new();
        let mut want_crc = Vec::new();
        for s in r.iter() {
            for (ext, into) in
                [("sbin", &mut want_bin), ("sdic", &mut want_dic), ("scrc", &mut want_crc)]
            {
                into.extend(piece(&s.dir, &format!("{stem}.{ext}")).expect(ext));
            }
        }
        assert_eq!(piece(&a, &format!("{stem}.sbin")).expect("sbin"), want_bin);
        assert_eq!(piece(&a, &format!("{stem}.sdic")).expect("sdic"), want_dic);
        assert_eq!(piece(&a, &format!("{stem}.scrc")).expect("scrc"), want_crc);
        let moved = crate::store::Store::open(&src_of(&a), stem).expect("перенесённое открылось");
        assert_eq!(moved.dicts(), 2, "словари источников не переехали");
        // А мелкое поле — пересобрано: сторож против россыпи его не пропустил.
        let small = r[0].fields.iter().position(|f| f.name == "title").expect("поле title");
        let tiny = crate::store::Store::open(&src_of(&a), &r[0].fields[small].stem).expect("title");
        assert_eq!(tiny.dicts(), 1, "мелкое поле перенесено — россыпь словарей прошла");

        // И целость слитого доказана не на слово: суммы распакованного сходятся.
        for i in 0..moved.blocks() {
            let (_, _, raw) = moved.chunk_at(i).expect("кусок читается");
            assert_eq!(moved.chunk_crc(i), Some(crate::wal::crc32(&raw)), "сумма куска {i}");
        }
        for d in [src, ha, hb] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Настоящий порог мелкие сегменты **не** пропускает, а опущенный пропускает.
    ///
    /// Этим и доказывается, что сторож стоит на пути и справляется с теми самыми
    /// числами, что в выпуске: сдвинь порог — и поведение меняется, оставь — и
    /// россыпь словарей не заводится.
    #[test]
    fn the_scatter_guard_measures_by_the_release_numbers() {
        let src = fresh("scatter");
        let mut ix = writer(&src, 300);
        for i in 0..600u32 {
            ix.add(&format!("d{i:04}"), &[&format!("док{}", i % 97), &body(i)]);
        }
        ix.flush_now();
        let segs = open(&src);
        let r = refs(&segs);
        let stem = r[0]
            .fields
            .iter()
            .find(|f| f.name == "body")
            .map(|f| f.stem.clone())
            .expect("поле body");

        let h1 = fresh("scatter-a");
        let (m1, o1) = merged(&r, &h1, false);
        let s1 = crate::store::Store::open(&src_of(&o1), &stem).expect("хранилище");
        assert_eq!(s1.dicts(), 1, "мелкие источники перенесены при настоящем пороге");
        assert!(m1.packed > 0, "пересборка не сжала ни куска");

        let h2 = fresh("scatter-b");
        let (m2, o2) = {
            let _low = crate::store::lower_carry_floor(2, 0);
            merged(&r, &h2, false)
        };
        let s2 = crate::store::Store::open(&src_of(&o2), &stem).expect("хранилище");
        assert_eq!(s2.dicts(), 2, "при опущенном пороге источники обязаны переехать");
        assert!(m2.packed < m1.packed, "опущенный порог ничего не изменил");
        same_content(&o1, &o2);
        for d in [src, h1, h2] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Мелкий корпус: годится только при опущенном пороге.
    fn light(dir: &str, docs: u32, per_seg: u32) {
        let mut ix = writer(dir, per_seg);
        for i in 0..docs {
            ix.add(&format!("d{i:05}"), &[&format!("док{}", i % 97), &body(i)]);
        }
        ix.flush_now();
    }

    /// На кусках без погашенных компрессор не зовётся **ни разу**.
    ///
    /// Это и есть выигрыш, выраженный числом, а не временем: время меряется
    /// машиной, а вызовы компрессора — самой работой. И тут же фальсификация:
    /// погаси один документ — и компрессор позовётся, потому что кусок с
    /// погашенным перенести как есть нельзя.
    #[test]
    fn a_clean_carry_never_calls_the_compressor() {
        // Порог опущен до одного куска: иначе мелкое поле `title` пересобиралось
        // бы, и ноль в счётчике проверить было бы нечем.
        let _low = crate::store::lower_carry_floor(1, 0);
        let src = fresh("nopack");
        light(&src, 1200, 400);
        let segs = open(&src);
        let r = refs(&segs);
        let clean = fresh("nopack-a");
        let (m, _) = merged(&r, &clean, false);
        assert_eq!(m.packed, 0, "перенос чистых кусков позвал компрессор {} раз", m.packed);

        // Один погашенный документ — и кусок, где он лежал, придётся пересжать.
        // Их два: этот документ есть и в `title`, и в `body`.
        assert!(r[0].kill(5), "документ обязан был быть живым");
        let dirty = fresh("nopack-b");
        let (md, od) = merged(&r, &dirty, false);
        assert!(md.packed > 0, "погашенный документ не заставил пересжать кусок");
        assert!(
            md.packed <= 2,
            "пересжато {} кусков — это уже пересборка, а не перенос",
            md.packed
        );
        // Содержимое при этом ровно то, что дала бы пересборка.
        let orc = fresh("nopack-c");
        let (_, oo) = merged(&r, &orc, true);
        same_content(&od, &oo);
        for d in [src, clean, dirty, orc] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Перенос повторяем: те же источники дают те же байты.
    ///
    /// Потоков у переноса нет вовсе — сжимает он в один, — но байты обязаны
    /// сойтись и с пересжатыми кусками, и с дописанными: зеркало проверки
    /// `store::parallel::the_number_of_threads_changes_nothing`, только для
    /// второго писателя.
    #[test]
    fn the_transfer_lays_the_same_bytes_twice() {
        let _low = crate::store::lower_carry_floor(2, 0);
        let src = fresh("twice");
        light(&src, 1500, 500);
        let segs = open(&src);
        let r = refs(&segs);
        assert!(r[0].kill(3) && r[1].kill(7), "нужны и погашенные, и чистые куски");
        let ha = fresh("twice-a");
        let hb = fresh("twice-b");
        let (ma, a) = merged(&r, &ha, false);
        let (mb, b) = merged(&r, &hb, false);
        assert!(ma.packed > 0 && ma.packed == mb.packed, "работа разошлась между заходами");
        let stem = r[0].fields.iter().find(|f| f.name == "body").map(|f| f.stem.clone()).unwrap();
        for ext in ["sbin", "smrk", "sdic", "scrc", "sdix"] {
            let x = piece(&a, &format!("{stem}.{ext}"));
            let y = piece(&b, &format!("{stem}.{ext}"));
            // Куски обязаны быть, а не просто одинаково отсутствовать: сравнение
            // двух пустот сошлось бы и у сегмента, в котором хранимого нет вовсе.
            assert!(x.is_some(), "куска {stem}.{ext} нет в первом переносе");
            assert_eq!(x, y, "{ext} разошёлся между двумя одинаковыми переносами");
        }
        for d in [src, ha, hb] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Порог плавности: грязных больше половины — собираем заново.
    ///
    /// Ровно тот случай, ради которого уборка зовёт слияние с **одним**
    /// источником: при доле мёртвых от `dead_fraction` и полутора сотнях
    /// документов на кусок грязными оказываются практически все куски, и
    /// пересжатие там и есть смысл операции.
    ///
    /// ## Чем меряется выбранный путь
    ///
    /// Словарём. У одного источника перенос кладёт в `.sdic` **его же** запись
    /// дословно, а пересборка обучает новую на колонке без погашенных. Счётчик
    /// сжатых кусков этого не различает: у пересборки их столько же, сколько
    /// кусков в выводе, а вывод после выброса мёртвых короче входа.
    #[test]
    fn more_than_half_dirty_goes_the_old_way() {
        let _low = crate::store::lower_carry_floor(2, 0);
        let src = fresh("threshold");
        light(&src, 900, 900);
        let segs = open(&src);
        let r = refs(&segs);
        let field = r[0].fields.iter().position(|f| f.name == "body").expect("поле body");
        let stem = r[0].fields[field].stem.clone();
        let st = r[0].fields[field].store.as_ref().expect("хранилище");
        let blocks = st.blocks();
        assert!(blocks >= 8, "кусков всего {blocks} — половину не разглядеть");
        let firsts: Vec<u32> = (0..blocks).map(|c| st.mark_at(c).expect("засечка").3).collect();
        let was = piece(&r[0].dir, &format!("{stem}.sdic")).expect("словарь источника");

        // Ровно половина кусков грязная — это ещё **не** повод пересобирать.
        for c in 0..blocks / 2 {
            assert!(r[0].kill(firsts[c * 2]), "документ обязан был быть живым");
        }
        let ha = fresh("threshold-a");
        let (ma, oa) = merged(&r, &ha, false);
        assert_eq!(
            piece(&oa, &format!("{stem}.sdic")).expect("словарь"),
            was,
            "на половине грязных словарь источника обязан переехать как есть"
        );
        assert!(ma.packed > 0, "грязные куски обязаны были пересжаться");
        // И содержимое при этом то же, что дал бы старый путь.
        let hr = fresh("threshold-r");
        let (_, orr) = merged(&r, &hr, true);
        same_content(&oa, &orr);

        // Ещё один грязный кусок — и путь меняется на пересборку.
        assert!(r[0].kill(firsts[1]), "документ обязан был быть живым");
        let hb = fresh("threshold-b");
        let (_, ob) = merged(&r, &hb, false);
        assert_ne!(
            piece(&ob, &format!("{stem}.sdic")).expect("словарь"),
            was,
            "перебрав половину, слияние всё ещё переносит"
        );
        // Содержимое и после смены пути то, чего ждали: живые по порядку.
        let ix = index::load(&ob).expect("вывод открылся");
        let mut at = 0u32;
        for i in 0..900u32 {
            if !r[0].is_live(i) {
                continue;
            }
            assert_eq!(
                ix.fetch(at, field).as_deref(),
                Some(body(i).as_bytes()),
                "документ {i} лёг под номером {at} не тем"
            );
            at += 1;
        }
        assert_eq!(at as usize, ix.docs, "живых оказалось не столько");
        for d in [src, ha, hb, hr] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Собирает сегмент **своей** схемой и переносит его наружу из каталога
    /// сборки: слить надо сегменты с разным составом полей, а один индексатор
    /// такого не пишет.
    fn seg(
        root: &str,
        name: &str,
        fields: &[(&str, index::Kind, u32)],
        rows: &[Vec<String>],
    ) -> String {
        let build = format!("{root}/{name}-build");
        std::fs::remove_dir_all(&build).ok();
        let n = fields.len();
        let mut ix = Indexer::typed_positioned(
            &build,
            fields.iter().map(|(f, _, _)| f.to_string()).collect(),
            vec![1.0; n],
            fields.iter().map(|(_, k, _)| *k).collect(),
            vec![true; n],
            vec![false; n],
            1_000_000,
        );
        ix.set_fids(fields.iter().map(|(_, _, f)| *f).collect());
        // Искать надо хотя бы по одному полю: без слов у сегмента нет ни
        // списков, ни норм, и открыть его будет нечем.
        // Числовое поле обязано остаться искомым: точное и искомое хранилища не
        // заводит — его значение печатает колонка, — а не искомое заводит.
        ix.set_searched(
            fields
                .iter()
                .map(|(f, k, _)| *f == "title" || !matches!(k, index::Kind::Text))
                .collect(),
        );
        for (i, row) in rows.iter().enumerate() {
            let v: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
            ix.add(&format!("{name}{i:05}"), &v);
        }
        ix.flush_now();
        let out = format!("{root}/{name}");
        std::fs::rename(format!("{build}/seg0000"), &out).expect("сегмент перенесён");
        std::fs::remove_dir_all(&build).ok();
        out
    }

    fn body_store(ix: &Index) -> (usize, &crate::store::Store) {
        let f = ix.fields.iter().position(|f| f.name == "body").expect("поле body");
        (f, ix.fields[f].store.as_ref().expect("хранилище body"))
    }

    /// Слитый сегмент обязан быть здоровым — а не только читаемым.
    fn sound(dir: &str) {
        let rep = crate::check::check(dir, crate::check::Depth::Full);
        let bad: Vec<String> = rep
            .findings
            .iter()
            .map(|f| format!("{} {}: {} / {}", f.scope, f.check, f.text, f.means))
            .collect();
        assert_eq!(rep.broken(), 0, "{bad:?}");
    }

    /// Неудобные формы переносятся так же, как пересобираются.
    ///
    /// ## Что здесь собрано в один вход
    ///
    /// Пустые значения; значение длиннее куска; источник, в котором поля нет
    /// вовсе; точное числовое поле, у которого хранилища нет по построению —
    /// значение печатает колонка; кусок, у которого погашены **все** документы;
    /// источник, погашенный целиком. Каждая из этих форм по отдельности —
    /// отдельная ветка в разборе, и любая из них, сработай она молча, съехала бы
    /// значениями на чужие номера.
    ///
    /// Судит их оракул: содержимое обязано совпасть с тем, что дала бы
    /// пересборка, на каждом документе и каждом поле. Плюс полная проверка
    /// целости слитого — она сверяет и плотность нумерации кусков, и суммы
    /// распакованного, то есть ровно то, что перенос мог бы испортить.
    #[test]
    fn awkward_shapes_carry_the_same_as_they_rebuild() {
        use crate::index::Kind;
        let _low = crate::store::lower_carry_floor(2, 0);
        let root = fresh("awkward");

        let a_rows: Vec<Vec<String>> = (0..300u32)
            .map(|i| {
                let b = match i % 50 {
                    0 => String::new(),
                    7 => "я".repeat(crate::store::BLOCK * 2),
                    _ => body(i),
                };
                vec![format!("док{}", i % 31), b]
            })
            .collect();
        let a = seg(&root, "a", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &a_rows);
        // Второй: поля `body` в нём нет вовсе.
        let b_rows: Vec<Vec<String>> =
            (0..200u32).map(|i| vec![format!("док{}", i % 31)]).collect();
        let b = seg(&root, "b", &[("title", Kind::Text, 0)], &b_rows);
        // Третий: и `body`, и точное число. У числа хранилища нет по построению.
        let c_rows: Vec<Vec<String>> = (0..300u32)
            .map(|i| vec![format!("док{}", i % 31), body(i + 7), (i * 3).to_string()])
            .collect();
        let c = seg(
            &root,
            "c",
            &[("title", Kind::Text, 0), ("body", Kind::Text, 1), ("price", Kind::Int32, 2)],
            &c_rows,
        );

        let ia = index::load(&a).expect("первый");
        let ib = index::load(&b).expect("второй");
        let ic = index::load(&c).expect("третий");
        assert!(ic.fields[2].store.is_none(), "у точного числа хранилища быть не должно");

        // Кусок, погашенный целиком: берём границы у самого хранилища.
        let (_, st) = body_store(&ic);
        assert!(st.blocks() >= 4, "кусков всего {} — гасить целиком нечего", st.blocks());
        let (lo, hi) = (st.mark_at(1).expect("засечка").3, st.mark_at(2).expect("засечка").3);
        for d in lo..hi {
            ic.kill(d);
        }
        // И источник, погашенный целиком.
        for d in 0..ib.docs as u32 {
            ib.kill(d);
        }

        let refs: Vec<&Index> = vec![&ia, &ib, &ic];
        let ha = fresh("awkward-a");
        let hb = fresh("awkward-b");
        let (ca, oa) = merged(&refs, &ha, false);
        let (cb, ob) = merged(&refs, &hb, true);
        assert_eq!(ca.docs, cb.docs, "число документов разошлось между путями");
        assert_eq!(ca.docs, a_rows.len() + c_rows.len() - (hi - lo) as usize);
        same_content(&oa, &ob);
        assert!(
            ca.packed < cb.packed,
            "переносить оказалось нечего: {} против {}",
            ca.packed,
            cb.packed
        );
        // Отрезков словарей ровно столько, сколько источников с хранилищем:
        // второй его не имеет вовсе и дописался в соседний отрезок.
        let moved = crate::store::Store::open(&src_of(&oa), "body").expect("перенесённое");
        assert_eq!(moved.dicts(), 2, "словари источников не переехали");
        sound(&oa);
        sound(&ob);
        for d in [root, ha, hb] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Хранилище без единого куска — не дыра в нумерации.
    ///
    /// Так выглядит поле, у которого `.smrk` пуст: `Store` открывается, а
    /// значений не отдаёт ни одного. Перенос обязан дописать на его документы
    /// пустые значения, а не пропустить их — пропуск съехал бы значениями
    /// следующего источника на чужие номера.
    #[test]
    fn a_store_without_a_single_chunk_is_not_a_gap() {
        use crate::index::Kind;
        let _low = crate::store::lower_carry_floor(2, 0);
        let root = fresh("nochunks");
        let rows: Vec<Vec<String>> =
            (0..300u32).map(|i| vec![format!("док{}", i % 31), body(i)]).collect();
        let a = seg(&root, "a", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        let b = seg(&root, "b", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        // Опустошаем хранилище первого: словарь на месте, кусков нет. Куски
        // остаются в оглавлении **пустыми**, а не пропадают: «куска нет» и
        // «кусок пуст» для читателя разные вещи, и проверяется именно вторая.
        for ext in ["sbin", "smrk", "scrc"] {
            reforge(&a, &format!("body.{ext}"), Some(&[]));
        }
        let ia = index::load(&a).expect("первый");
        let ib = index::load(&b).expect("второй");
        assert_eq!(body_store(&ia).1.blocks(), 0, "кусков обязано не остаться");

        let refs: Vec<&Index> = vec![&ia, &ib];
        let ha = fresh("nochunks-a");
        let hb = fresh("nochunks-b");
        let (ca, oa) = merged(&refs, &ha, false);
        let (_, ob) = merged(&refs, &hb, true);
        assert_eq!(ca.docs, 600);
        same_content(&oa, &ob);
        // Первые триста — пустые, вторые триста — свои.
        let m = index::load(&oa).expect("вывод");
        let f = m.fields.iter().position(|f| f.name == "body").expect("поле");
        for i in 0..300u32 {
            assert_eq!(m.fetch(i, f).as_deref(), Some(&[][..]), "документ {i} не пуст");
        }
        for i in 0..300u32 {
            assert_eq!(
                m.fetch(300 + i, f).as_deref(),
                Some(body(i).as_bytes()),
                "документ {} съехал",
                300 + i
            );
        }
        for d in [root, ha, hb] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Распущенный отрезок дописывается в **соседний**, и живёт из них
    /// наибольший.
    ///
    /// ## Что проверяется
    ///
    /// Правило пересборки таблицы словарей целиком: какие отрезки распускаются
    /// (наименьшие — их дешевле всего пересжать), куда деваются их куски (в
    /// соседний отрезок, обычным сжатием) и уцелел ли инвариант «словарь владеет
    /// одним непрерывным отрезком» — без него поиск словаря двоичным поиском по
    /// первым кускам перестал бы работать, и `.sdix` был бы отвергнут на
    /// открытии.
    ///
    /// Потолок таблицы опущен: настоящий пробивается двумя с половиной сотнями
    /// источников слияния, а правило то же самое на четырёх. Уцелевший словарь
    /// назван побайтово — иначе «распустили не тех» прошло бы незамеченным.
    #[test]
    fn coalescing_folds_the_smallest_runs_into_a_neighbour() {
        use crate::index::Kind;
        let _low = crate::store::lower_carry_floor(2, 0);
        let _cnt = crate::store::lower_dict_counts(2, 1);
        let root = fresh("coalesce");
        let mut dirs = Vec::new();
        for (k, n) in [(0usize, 120u32), (1, 200), (2, 400), (3, 260)] {
            let rows: Vec<Vec<String>> =
                (0..n).map(|i| vec![format!("док{}", i % 31), body(i + k as u32 * 977)]).collect();
            dirs.push(seg(
                &root,
                &format!("s{k}"),
                &[("title", Kind::Text, 0), ("body", Kind::Text, 1)],
                &rows,
            ));
        }
        let loaded: Vec<Index> = dirs.iter().map(|d| index::load(d).expect("источник")).collect();
        let refs: Vec<&Index> = loaded.iter().collect();
        // Погашенные есть и у распускаемых отрезков, и у уцелевшего: без них
        // отбор живых при роспуске прошёл бы незамеченным.
        let mut killed = 0usize;
        for (k, s) in refs.iter().enumerate() {
            for d in [1u32, 11, 23] {
                if s.kill(d + k as u32) {
                    killed += 1;
                }
            }
        }
        assert_eq!(killed, 12, "гасить надо было в каждом источнике");

        let ha = fresh("coalesce-a");
        let hb = fresh("coalesce-b");
        let (ca, oa) = merged(&refs, &ha, false);
        let (_, ob) = merged(&refs, &hb, true);
        assert_eq!(ca.docs, 980 - killed);
        same_content(&oa, &ob);

        let moved = crate::store::Store::open(&src_of(&oa), "body").expect("перенесённое");
        assert_eq!(moved.dicts(), 1, "таблицу не свернули до одного отрезка");
        // Уцелел наибольший — третий источник, четыреста документов.
        assert_eq!(
            piece(&oa, "body.sdic").expect("словарь"),
            piece(&dirs[2], "body.sdic").expect("словарь источника"),
            "уцелел не наибольший отрезок"
        );
        assert!(piece(&oa, "body.sdix").is_none(), "при одном словаре таблицы быть не должно");
        sound(&oa);
        for d in [root, ha, hb] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Перенос, накрывший не столько документов, — отказ, а не публикация.
    ///
    /// ## Почему это последний рубеж
    ///
    /// Съехавшая нумерация ничего не роняет: файлы целые, кратности соблюдены,
    /// суммы сходятся — просто содержимое возвращается **чужое**. Ни `sums.tsv`,
    /// ни суммы кусков этого не видят: они считаются уже по съехавшему. Видно
    /// это ровно одним числом — сколько документов накрыл перенос против того,
    /// сколько их в слитом сегменте.
    ///
    /// Проверяется прямым вызовом с заведомо неверным числом: настоящей порчи,
    /// которая дала бы расхождение, в исправном коде нет по построению — на то и
    /// рубеж.
    #[test]
    fn a_transfer_covering_the_wrong_number_of_documents_is_refused() {
        use crate::index::Kind;
        let _low = crate::store::lower_carry_floor(2, 0);
        let root = fresh("count");
        let rows: Vec<Vec<String>> =
            (0..300u32).map(|i| vec![format!("док{}", i % 31), body(i)]).collect();
        let a = seg(&root, "a", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        let b = seg(&root, "b", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        let ia = index::load(&a).expect("первый");
        let ib = index::load(&b).expect("второй");
        let refs: Vec<&Index> = vec![&ia, &ib];
        let plan = Plan::build(&refs).expect("план полей");
        let f =
            (0..plan.fids.len()).find(|i| plan.field(&refs, *i).name == "body").expect("поле body");
        let carry = plan_carry(&refs, &plan, f, &[], false).expect("перенос запланирован");

        let out = fresh("count-out");
        carry_store(&refs, &plan, f, &[], false, &carry, &out, "body", 600)
            .expect("настоящее число документов обязано приниматься");
        let e = match carry_store(&refs, &plan, f, &[], false, &carry, &out, "body", 601) {
            Ok(_) => panic!("перенос накрыл не столько документов и промолчал"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "получили: {e}");
        assert!(e.to_string().contains("накрыл 600"), "отказ не назвал числа: {e}");
        for d in [root, out] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Хранилище, оборвавшееся раньше документов, добирается пустыми.
    ///
    /// ## Зачем эта ветка есть
    ///
    /// Собранное [`crate::store::build`] хранилище накрывает **все** документы
    /// сегмента: значение эмитируется на каждый, пусть и пустое. Но опираться на
    /// это молча нельзя: усечённый `.smrk` даёт хранилище, которое открывается,
    /// читается и кончается раньше сегмента. Перенеси его как есть — и документы
    /// следующего источника съехали бы на освободившиеся номера, а сверка по
    /// числу документов поймала бы это уже отказом, потеряв слияние.
    ///
    /// Здесь та же форма разбирается **дописыванием**: хвост добирается пустыми
    /// значениями, ровно как это делал старый путь.
    #[test]
    fn a_store_that_stops_short_is_filled_with_blanks() {
        use crate::index::Kind;
        let _low = crate::store::lower_carry_floor(2, 0);
        let root = fresh("short");
        let rows: Vec<Vec<String>> =
            (0..300u32).map(|i| vec![format!("док{}", i % 31), body(i)]).collect();
        let a = seg(&root, "a", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        let b = seg(&root, "b", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);

        // Отрезаем у первого последний кусок: словарь, тела и все прочие файлы
        // на месте, а засечек на один меньше.
        let marks = piece(&a, "body.smrk").expect("засечки");
        let crcs = piece(&a, "body.scrc").expect("суммы");
        let n = marks.len() / crate::store::MARK;
        assert!(n >= 4, "кусков всего {n} — отрезать нечего");
        reforge(&a, "body.smrk", Some(&marks[..(n - 1) * crate::store::MARK]));
        reforge(&a, "body.scrc", Some(&crcs[..(n - 1) * 4]));

        let ia = index::load(&a).expect("первый");
        let ib = index::load(&b).expect("второй");
        let (_, st) = body_store(&ia);
        assert_eq!(st.blocks(), n - 1, "кусок не отрезался");
        let last = st.mark_at(n - 2).expect("засечка").3 + st.chunk(n - 2).expect("кусок").1 as u32;
        assert!(last < 300, "хранилище всё ещё накрывает все документы — добирать нечего");

        let refs: Vec<&Index> = vec![&ia, &ib];
        let ha = fresh("short-a");
        let hb = fresh("short-b");
        let (ca, oa) = merged(&refs, &ha, false);
        let (_, ob) = merged(&refs, &hb, true);
        assert_eq!(ca.docs, 600, "документы потерялись");
        same_content(&oa, &ob);

        let m = index::load(&oa).expect("вывод");
        let f = m.fields.iter().position(|f| f.name == "body").expect("поле");
        for i in last..300 {
            assert_eq!(m.fetch(i, f).as_deref(), Some(&[][..]), "хвост {i} не пуст");
        }
        for i in 0..300u32 {
            assert_eq!(
                m.fetch(300 + i, f).as_deref(),
                Some(body(i).as_bytes()),
                "документ {} съехал",
                300 + i
            );
        }
        sound(&ob);
        for d in [root, ha, hb] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Сегмент раскладки 8 сливается рядом с сегментом раскладки 9.
    ///
    /// У восьмой нет сумм распакованного `.scrc`, а перенос их **копирует**, а
    /// не считает: пересчёт означал бы распаковку. Значит такой источник обязан
    /// пойти обычным путём — распаковаться и дописаться, — и содержимое от этого
    /// не измениться. Пропусти проверка этот случай, и перенос либо потерял бы
    /// суммы, либо записал бы чужие: полная проверка целости назвала бы слитый
    /// сегмент испорченным.
    #[test]
    fn a_layout_8_source_merges_beside_a_layout_9_one() {
        use crate::index::Kind;
        let _low = crate::store::lower_carry_floor(2, 0);
        let root = fresh("layout8");
        let rows: Vec<Vec<String>> =
            (0..300u32).map(|i| vec![format!("док{}", i % 31), body(i)]).collect();
        let a = seg(&root, "a", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        let b = seg(&root, "b", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        // Первый становится сегментом восьмой раскладки: сумм у неё не было.
        for stem in ["title", "body"] {
            reforge(&a, &format!("{stem}.scrc"), None);
        }
        let head = std::fs::read_to_string(format!("{a}/head.tsv")).expect("заголовок");
        let head = head.replace(&format!("layout\t{}", crate::index::LAYOUT), "layout\t8");
        std::fs::write(format!("{a}/head.tsv"), &head).expect("заголовок");

        let ia = index::load(&a).expect("восьмой раскладки");
        let ib = index::load(&b).expect("девятой раскладки");
        assert!(!body_store(&ia).1.has_crcs(), "сумм у восьмой раскладки быть не должно");
        assert!(body_store(&ib).1.has_crcs(), "у девятой суммы обязаны быть");

        // Погашенные есть у обоих: у восьмой раскладки они выпадут на проходе,
        // у девятой — при пересжатии грязного куска.
        let mut killed = 0usize;
        for d in [3u32, 44, 155] {
            killed += usize::from(ia.kill(d)) + usize::from(ib.kill(d + 1));
        }
        assert_eq!(killed, 6, "гасить надо было в обоих источниках");

        let refs: Vec<&Index> = vec![&ia, &ib];
        let ha = fresh("layout8-a");
        let hb = fresh("layout8-b");
        let (ca, oa) = merged(&refs, &ha, false);
        let (_, ob) = merged(&refs, &hb, true);
        assert_eq!(ca.docs, 600 - killed);
        same_content(&oa, &ob);
        // Переехал только тот, у кого суммы есть; у слитого они есть на все
        // куски — иначе полная проверка не сошлась бы.
        let moved = crate::store::Store::open(&src_of(&oa), "body").expect("перенесённое");
        assert!(moved.has_crcs(), "суммы у слитого пропали");
        sound(&oa);
        for d in [root, ha, hb] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Сегмент с таблицей словарей переживает снимок и восстановление.
    ///
    /// ## Почему это проверяется здесь
    ///
    /// `.sdix` — новый класс файла в каталоге сегмента, и появляется он **только**
    /// после переноса. Снимок перечисляет каталог целиком, так что попасть в
    /// архив таблица обязана сама собой; но цена ошибки тут не «файл потерялся»,
    /// а «поле молча читается чужими словарями»: без таблицы склеенные записи
    /// `.sdic` разбираются от нуля, и первый словарь сходит за единственный.
    #[test]
    fn a_segment_with_a_dictionary_table_survives_a_snapshot() {
        use crate::index::Kind;
        let _low = crate::store::lower_carry_floor(2, 0);
        let root = fresh("snap");
        let rows: Vec<Vec<String>> =
            (0..300u32).map(|i| vec![format!("док{}", i % 31), body(i)]).collect();
        let a = seg(&root, "a", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        let b = seg(&root, "b", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        let (ia, ib) = (index::load(&a).expect("a"), index::load(&b).expect("b"));
        let holder = fresh("snap-h");
        let (_, out) = merged(&[&ia, &ib], &holder, false);
        assert!(piece(&out, "body.sdix").is_some(), "таблицы нет — проверять нечего");

        let arc = format!("{root}/arc");
        crate::snapshot::create(&holder, &arc, "s1", crate::codec::Effort::Fast)
            .expect("снимок снят");
        let back = format!("{root}/back");
        crate::snapshot::restore(&format!("{arc}/s1"), &back).expect("снимок восстановлен");

        let was = index::load(&out).expect("исходный");
        let now = index::load(&format!("{back}/{}", crate::manifest::name_of(&out)))
            .expect("восстановленный");
        assert_eq!(now.docs, was.docs);
        let f = now.fields.iter().position(|f| f.name == "body").expect("поле");
        let moved = crate::store::Store::open(&now.src, &now.fields[f].stem).expect("хранилище");
        assert_eq!(moved.dicts(), 2, "таблица словарей не пережила снимок");
        for d in 0..now.docs as u32 {
            assert_eq!(now.fetch(d, f), was.fetch(d, f), "документ {d} вернулся не тем");
        }
        for d in [root, holder] {
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Хранилище, оборвавшееся на порче посреди прохода, останавливает слияние.
    ///
    /// ## Разница, ради которой это заведено
    ///
    /// «Дальше ничего нет» и «дальше нечитаемо» — разные вещи. Проход по
    /// хранилищу отличает их признаком, а слияние их путало: получив `None`, оно
    /// добирало хвост пустыми значениями и отчитывалось успехом. Половина поля
    /// уходила в никуда, исходные сегменты сносила уборка, и восстанавливать было
    /// не из чего.
    ///
    /// Проверяются **оба** пути: и перенос, и пересборка. У переноса порченый
    /// источник и без того не проходит сторож — сумм кусков у него нет ровно
    /// потому, что мы их сняли, — и уходит проходом; у пересборки проход
    /// единственный.
    #[test]
    fn a_source_store_broken_mid_walk_stops_the_merge() {
        use crate::index::Kind;
        let _low = crate::store::lower_carry_floor(2, 0);
        let root = fresh("brokenwalk");
        let rows: Vec<Vec<String>> =
            (0..300u32).map(|i| vec![format!("док{}", i % 31), body(i)]).collect();
        let a = seg(&root, "a", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        let b = seg(&root, "b", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);

        // Порча в середине тел: часть кусков перестанет распаковываться.
        let mut raw = piece(&a, "body.sbin").expect("тела");
        let mid = raw.len() / 2;
        for k in 0..256 {
            raw[mid + k] ^= 0xff;
        }
        reforge(&a, "body.sbin", Some(&raw));
        // Суммы кусков сняты: с ними источник и так не переносился бы, но отказ
        // обязан прийти от прохода, а не от их отсутствия.
        reforge(&a, "body.scrc", None);

        let ia = index::load(&a).expect("порченый");
        let ib = index::load(&b).expect("целый");
        let refs: Vec<&Index> = vec![&ia, &ib];
        for rebuild in [false, true] {
            let h = fresh(if rebuild { "brokenwalk-r" } else { "brokenwalk-c" });
            let out = format!("{h}/mrg0000");
            let res = if rebuild {
                let _g = Rebuilding::on();
                merge(&refs, &out)
            } else {
                merge(&refs, &out)
            };
            let e = match res {
                Ok(_) => panic!("порча прошла как конец данных (пересборка: {rebuild})"),
                Err(e) => e,
            };
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "получили: {e}");
            assert!(e.to_string().contains("порч"), "отказ не про порчу: {e}");
            std::fs::remove_dir_all(&h).ok();
        }
        std::fs::remove_dir_all(&root).ok();
    }

    /// Гнутая таблица словарей останавливает слияние, а не проходит молча.
    ///
    /// ## Что было бы иначе
    ///
    /// `Store::open` отвергает гнутую `.sdix` именно затем, чтобы куски не
    /// читались чужим словарём. Но для слияния «хранилища нет» и «хранилище не
    /// открылось» выглядели одинаково: оно положило бы на место значений пустоту
    /// и отчиталось успехом. Исходные сегменты после публикации сносит уборка —
    /// восстанавливать было бы не из чего.
    #[test]
    fn a_bent_dictionary_table_stops_the_merge() {
        use crate::index::Kind;
        let _low = crate::store::lower_carry_floor(2, 0);
        let root = fresh("bentsdix");
        let rows: Vec<Vec<String>> =
            (0..300u32).map(|i| vec![format!("док{}", i % 31), body(i)]).collect();
        let a = seg(&root, "a", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);
        let b = seg(&root, "b", &[("title", Kind::Text, 0), ("body", Kind::Text, 1)], &rows);

        // Сначала сливаем как есть — так получается сегмент с таблицей.
        let ha = fresh("bentsdix-a");
        let (ia, ib) = (index::load(&a).expect("a"), index::load(&b).expect("b"));
        let (_, oa) = merged(&[&ia, &ib], &ha, false);
        let whole = piece(&oa, "body.sdix").expect("таблица заведена");
        assert!(index::load(&oa).is_ok(), "слитый обязан открываться");

        // Гнём сумму таблицы: строение цело, а сверка не сходится.
        let mut bent = whole.clone();
        let n = bent.len() - 1;
        bent[n] ^= 0xff;
        reforge(&oa, "body.sdix", Some(&bent));

        // Гнутая таблица останавливает дело **раньше** слияния: сегмент с
        // объявленным, но неоткрывшимся хранилищем не открывается вовсе.
        //
        // Прежде здесь ждали более слабого: сегмент открывался, а поле молча
        // оставалось без хранилища, и не пустить дальше должно было слияние.
        // Договор усилен потому, что до слияния такой сегмент успевал побывать в
        // выдаче — `fetch` вернул бы пустое на каждом документе, и отличить это
        // от «документ и правда пуст» нечем.
        let e = match index::load(&oa) {
            Ok(_) => panic!("сегмент с гнутой таблицей открылся, а не отказал"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "получили: {e}");
        assert!(e.to_string().contains("хранилище поля"), "отказ не про хранилище: {e}");

        // И слияние такого сегмента не начнётся: открыть его нечем.
        let hb = fresh("bentsdix-b");
        assert!(index::load(&oa).is_err(), "источник обязан оставаться неоткрываемым");

        // Возвращаем таблицу — и всё идёт как ни в чём не бывало.
        reforge(&oa, "body.sdix", Some(&whole));
        let good = index::load(&oa).expect("вернулось");
        let hc = fresh("bentsdix-c");
        let (m, oc) = merged(&[&good], &hc, false);
        assert_eq!(m.docs, 600);
        sound(&oc);
        for d in [root, ha, hb, hc] {
            std::fs::remove_dir_all(&d).ok();
        }
    }
}

/// Перелив хранимого в контейнер: чего в каталоге не хватает, тот и не готов.
///
/// ## Почему проверяется прямым вызовом, а не через слияние
///
/// Проверяется поведение [`pour_store`] на **неполном** наборе файлов, а
/// собрать такой набор слиянием нечем: файлы кладёт [`crate::store::Transfer`]
/// строкой выше, и увести один из них между переносом и переливом можно только
/// снаружи. Зато сам перелив — обычная функция от каталога и счёта уложенного, и
/// позвать её с любым набором стоит десяти строк.
#[cfg(test)]
mod pour_tests {
    use super::*;
    use crate::store::Made;

    fn fresh(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-mpour-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).expect("каталог");
        d.to_string_lossy().to_string()
    }

    /// Кладёт то же, что оставляет за собой перенос: тело и его спутники.
    fn lay(dir: &str, name: &str, exts: &[&str]) {
        for ext in exts {
            // Байты произвольные: перелив в них не заглядывает, ему важны длина
            // и само наличие.
            let body = format!("{name}.{ext} — {}", ext.len());
            std::fs::write(format!("{dir}/{name}.{ext}"), body.as_bytes()).expect("файл");
        }
    }

    /// Переливает и отдаёт имена кусков, оказавшихся в контейнере.
    fn poured(dir: &str, name: &str, made: &Made) -> std::io::Result<Vec<String>> {
        let mut pack = crate::container::Writer::create(dir, index::LAYOUT)?;
        pour_store(&mut pack, dir, name, made)?;
        pack.finish()?;
        let t = crate::container::Table::read(&format!("{dir}/{}", crate::container::FILE))?;
        Ok(t.members().iter().map(|m| m.name.clone()).collect())
    }

    fn made(blocks: usize, dicts: usize) -> Made {
        Made { docs: 100, blocks, dicts, packed: 0 }
    }

    /// Полный набор переезжает целиком, и в каталоге не остаётся ничего.
    #[test]
    fn a_full_set_moves_in_and_leaves_nothing_behind() {
        let dir = fresh("full");
        lay(&dir, "body", &["sbin", "smrk", "sdic", "scrc", "sdix"]);
        let got = poured(&dir, "body", &made(3, 2)).expect("перелив");
        assert_eq!(
            got,
            vec!["body.sbin", "body.scrc", "body.sdic", "body.sdix", "body.smrk"],
            "в контейнер попало не то"
        );
        assert_eq!(
            dir_names(&dir),
            vec![crate::container::FILE.to_string()],
            "перелив не снёс файлы, из которых наливал"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// При единственном словаре таблицы `.sdix` нет — и это не пропажа.
    #[test]
    fn a_single_dictionary_needs_no_table() {
        let dir = fresh("onedict");
        lay(&dir, "body", &["sbin", "smrk", "sdic", "scrc"]);
        let got = poured(&dir, "body", &made(3, 1)).expect("перелив");
        assert_eq!(got, vec!["body.sbin", "body.scrc", "body.sdic", "body.smrk"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Ни одного куска — переносить и правда нечего, и это успех.
    ///
    /// Так отвечает `Transfer::finish` на поле без единого значения: он сносит
    /// всё, что успел завести, и хранилища у поля не будет. Тот же исход, что у
    /// пересборки, когда `store::build` отдаёт `None`.
    #[test]
    fn no_chunks_at_all_is_not_a_loss() {
        let dir = fresh("nochunks");
        let got = poured(&dir, "body", &made(0, 0)).expect("перелив");
        assert!(got.is_empty(), "на пустом переносе в контейнер что-то попало: {got:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Пропажа любого спутника — отказ, и он называет путь.
    ///
    /// Прежде этого не видели вовсе: перелив спрашивал `Path::exists()` про одно
    /// только тело, а недостачу спутников проходил `continue`-ом. Сегмент
    /// объявлялся упакованным, файлы оставались россыпью, и поле молча отдавало
    /// пустоту на каждый документ.
    #[test]
    fn a_missing_companion_is_refused_by_name() {
        for gone in ["smrk", "sdic", "scrc", "sdix"] {
            let dir = fresh(&format!("gone-{gone}"));
            let all = ["sbin", "smrk", "sdic", "scrc", "sdix"];
            let keep: Vec<&str> = all.iter().copied().filter(|e| *e != gone).collect();
            lay(&dir, "body", &keep);
            let e = match poured(&dir, "body", &made(3, 2)) {
                Ok(v) => panic!("перелив без {gone} отчитался успехом: {v:?}"),
                Err(e) => e.to_string(),
            };
            assert!(e.contains(&format!("body.{gone}")), "отказ не назвал пропажу: {e}");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Пропажа самого тела — отказ, а не «переносить нечего».
    #[test]
    fn a_missing_body_is_refused() {
        let dir = fresh("nobody");
        lay(&dir, "body", &["smrk", "sdic", "scrc"]);
        let e = match poured(&dir, "body", &made(3, 1)) {
            Ok(v) => panic!("перелив без тела отчитался успехом: {v:?}"),
            Err(e) => e.to_string(),
        };
        assert!(e.contains("body.sbin"), "отказ не назвал тело: {e}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Пустое тело при уложенных кусках — тоже отказ: кусков без значений нет.
    #[test]
    fn an_empty_body_is_not_an_empty_field() {
        let dir = fresh("emptybody");
        lay(&dir, "body", &["smrk", "sdic", "scrc"]);
        std::fs::write(format!("{dir}/body.sbin"), []).expect("пустое тело");
        let e = match poured(&dir, "body", &made(3, 1)) {
            Ok(v) => panic!("пустое тело сошло за перенесённое: {v:?}"),
            Err(e) => e.to_string(),
        };
        assert!(e.contains("тело пусто"), "отказ не про пустоту: {e}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Пустой спутник — тоже отказ: засечкам и словарю взяться из ниоткуда нельзя.
    #[test]
    fn an_empty_companion_is_refused() {
        let dir = fresh("emptymrk");
        lay(&dir, "body", &["sbin", "sdic", "scrc"]);
        std::fs::write(format!("{dir}/body.smrk"), []).expect("пустые засечки");
        let e = match poured(&dir, "body", &made(3, 1)) {
            Ok(v) => panic!("пустые засечки сошли за перенесённые: {v:?}"),
            Err(e) => e.to_string(),
        };
        assert!(e.contains("кусок пуст"), "отказ не про пустоту: {e}");
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// Профиль частых обновлений отличается не названием, а размером реально
/// выбранной работы: одна догоняющая итерация должна убирать десятки мелких
/// сегментов, а не повторять прежние восьмёрки.
#[cfg(test)]
mod update_heavy_tests {
    use super::*;

    fn tiny_segments(n: usize) -> Vec<Seg> {
        (0..n)
            .map(|_| Seg { bytes: 4 * 1024, docs: 10, sig: 1, order: vec![0], ..Seg::default() })
            .collect()
    }

    #[test]
    fn profile_round_trips_and_picks_a_wide_tiny_segment_window() {
        assert_eq!(MergeKind::parse("update-heavy"), Some(MergeKind::UpdateHeavy));
        assert_eq!(MergeKind::UpdateHeavy.word(), "update_heavy");
        let update = MergePolicy::parse("update_heavy").expect("профиль");
        assert_eq!(update.kind(), MergeKind::UpdateHeavy);

        let segs = tiny_segments(128);
        let ordinary = plan_by(&segs, &MergePolicy::Tiered(Tier::default())).unwrap();
        let wide = plan_by(&segs, &update).unwrap();
        assert_eq!(ordinary.len(), 8, "обычный профиль изменил прежнюю ширину");
        assert_eq!(wide.len(), 32, "update-heavy не погасил россыпь широким окном");
    }
}

#[cfg(test)]
mod numbering_tests {
    use super::*;

    /// Нумерация выхода проверяется до работы, а не заворачивается молча.
    ///
    /// Собрать сегмент на четыре миллиарда документов, чтобы проверить это
    /// прогоном, нельзя ни на одной машине, — поэтому проверяется сама
    /// граница. Она и есть всё содержание: на единицу ниже работа берётся, на
    /// границе и выше отвергается с объяснением.
    ///
    /// Последний номер отдан метке «погашен» (`remap` ставит `u32::MAX` живому
    /// документу, которого нет), поэтому потолок на единицу строже
    /// разрядности. Ошибись здесь на единицу — и документ с последним номером
    /// стал бы невидимым молча.
    #[test]
    fn the_output_numbering_is_checked_before_any_work() {
        assert!(numbering_fits(0).is_ok(), "пустое слияние законно");
        assert!(numbering_fits(1_000_000_000).is_ok(), "миллиард обязан браться");
        assert!(numbering_fits(u32::MAX as usize - 1).is_ok(), "предпоследний номер ещё свой");
        let at = numbering_fits(u32::MAX as usize);
        assert!(at.is_err(), "на потолке номер совпал бы с меткой «погашен»");
        let over = numbering_fits(u32::MAX as usize + 1);
        assert!(over.is_err(), "за потолком номера заворачиваются");
        let why = over.unwrap_err();
        assert!(
            why.contains("4294967295") || why.contains("4294967294"),
            "отказ обязан назвать потолок числом: {why}"
        );
    }
}
