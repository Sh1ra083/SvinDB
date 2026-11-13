//! Разбор запроса HTTP/1.1 и сборка ответа.

use std::io::{self, BufRead, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Потолок тела запроса. Без него любой клиент может попросить нас выделить
/// столько памяти, сколько напишет в заголовке.
const MAX_BODY: usize = 32 * 1024 * 1024;
/// Небольшое превышение дочитывается и выбрасывается перед закрытием сокета.
///
/// Если закрыть сокет сразу после заголовка, обычный клиент ещё пишет тело и
/// получает `Broken pipe`, не успев прочитать уже отправленный ответ. Это тот
/// самый transport error, который под нагрузкой невозможно отличить от
/// принятой записи с потерянным ACK. Дочитывание не выделяет память под тело и
/// ограничено сверху: гигантский Content-Length по-прежнему закрывается сразу.
const MAX_DISCARD_BODY: usize = 64 * 1024 * 1024;

/// Потолок заголовков: их число и общая длина.
///
/// Тело ограничено давно, а заголовки не были ограничены вовсе — а это тот же
/// самый способ занять память узла, только через дверь, которую забыли
/// запереть: клиент шлёт миллион строк по килобайту и не отправляет пустую.
const MAX_HEADERS: usize = 128;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_REQUEST_LINE: usize = 8 * 1024;
const MAX_HEADER_LINE: usize = 8 * 1024;

/// Жёсткий верхний потолок тел, зарезервированных одновременно во всём процессе.
pub const MAX_INFLIGHT_BODY_BYTES: usize = 128 * 1024 * 1024;

/// Какую часть доступной памяти разрешено занять сырыми HTTP-телами.
///
/// Само тело — только первая копия. JSON-разбор, нормализация строк, построение
/// postings и передача буферу приёма кратковременно требуют ещё несколько его
/// размеров. Поэтому фиксированные 128 МиБ безопасны на большой машине, но
/// смертельны в cgroup на 1 ГиБ: четыре тела по 31 МиБ успевали пройти admission
/// одновременно и OOM-killer снимал весь узел. Одна тридцать вторая оставляет
/// место индексу, mmap-страницам и этому временному усилению; на 4 ГиБ и выше
/// сохраняется прежний потолок.
const INFLIGHT_BODY_MEMORY_DIVISOR: u64 = 32;
const MIN_INFLIGHT_BODY_BYTES: usize = 1024 * 1024;

pub fn inflight_body_limit(memory_bytes: Option<u64>) -> usize {
    memory_bytes.map_or(MAX_INFLIGHT_BODY_BYTES, |bytes| {
        usize::try_from(bytes / INFLIGHT_BODY_MEMORY_DIVISOR)
            .unwrap_or(usize::MAX)
            .clamp(MIN_INFLIGHT_BODY_BYTES, MAX_INFLIGHT_BODY_BYTES)
    })
}

pub struct Request {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
    /// Клиент попросил закрыть соединение после ответа.
    pub close: bool,
    /// Имена приведены к нижнему регистру, значения — **нет**.
    pub headers: Vec<(String, String)>,
    /// Зафиксированная длина тела для внутренних диагностических тестов и
    /// admission; наружу это поле не входит в HTTP API.
    pub(crate) body_len: usize,
}

impl Request {
    pub fn header(&self, lower_name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == lower_name).map(|(_, v)| v.as_str())
    }

    pub fn body_len(&self) -> usize {
        self.body_len
    }
}

/// Читающая половина сокета с абсолютным дедлайном текущей стадии запроса.
/// Общий бюджет памяти тел. Permit удерживает reservation до конца запроса.
#[derive(Debug)]
pub struct BodyBudget {
    used: AtomicUsize,
    max: usize,
}

#[derive(Debug)]
pub struct BodyPermit {
    budget: Arc<BodyBudget>,
    bytes: usize,
}

#[derive(Debug)]
pub struct BodyFull {
    pub want: usize,
    pub used: usize,
    pub max: usize,
}

impl BodyBudget {
    pub fn new(max: usize) -> BodyBudget {
        BodyBudget { used: AtomicUsize::new(0), max }
    }

    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<BodyPermit, BodyFull> {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = used.checked_add(bytes) else {
                return Err(BodyFull { want: bytes, used, max: self.max });
            };
            if next > self.max {
                return Err(BodyFull { want: bytes, used, max: self.max });
            }
            match self.used.compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return Ok(BodyPermit { budget: Arc::clone(self), bytes }),
                Err(now) => used = now,
            }
        }
    }
}

impl Drop for BodyPermit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// Что пришло из сокета.
pub enum Incoming {
    Request(Request),
    /// Запрос отвергнут ещё до тела; ответить этим кодом и закрыть соединение.
    Refused(u16, &'static str),
    /// Тело слегка превышает лимит. Его размер известен и достаточно мал,
    /// чтобы потоково выбросить перед закрытием, не устраивая клиенту reset.
    RefusedBody(u16, &'static str, usize),
    /// Соединение закрыто.
    Done,
}

/// Читает запрос по уже заведённому буферу — чтобы читать их подряд.
///
/// Заводить `BufReader` на каждый запрос нельзя: он забирает в себя байты,
/// прочитанные с опережением, и следующий запрос начинался бы с их потери.
///
/// Принимает любой `BufRead`, а не `BufReader<TcpStream>`, чтобы разбор
/// проверялся на байтах без сокета. Тест, повторяющий разбор у себя, проверял
/// бы свою копию: она разошлась бы с оригиналом и продолжила бы отчитываться
/// зелёным.
enum LineError {
    TooLong,
    Eof,
    Read(io::ErrorKind),
}

fn is_read_timeout(kind: io::ErrorKind) -> bool {
    matches!(kind, io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock)
}

fn bounded_line(reader: &mut impl BufRead, limit: usize) -> Result<Option<Vec<u8>>, LineError> {
    let mut out = Vec::new();
    loop {
        let buf = reader.fill_buf().map_err(|error| LineError::Read(error.kind()))?;
        if buf.is_empty() {
            return if out.is_empty() { Ok(None) } else { Err(LineError::Eof) };
        }
        let take = buf.iter().position(|b| *b == b'\n').map_or(buf.len(), |i| i + 1);
        if out.len().checked_add(take).is_none_or(|n| n > limit) {
            return Err(LineError::TooLong);
        }
        out.extend_from_slice(&buf[..take]);
        reader.consume(take);
        if out.last() == Some(&b'\n') {
            return Ok(Some(out));
        }
    }
}

fn text_line(bytes: Vec<u8>) -> Result<String, ()> {
    if !bytes.ends_with(b"\r\n") {
        return Err(());
    }
    String::from_utf8(bytes).map_err(|_| ())
}

/// Читает request line и headers, но не тело. Это позволяет проверить права и
/// получить глобальный memory permit до крупной аллокации.
pub fn read_head_from(reader: &mut impl BufRead) -> Incoming {
    let line = match bounded_line(reader, MAX_REQUEST_LINE) {
        Ok(None) => return Incoming::Done,
        Ok(Some(line)) => match text_line(line) {
            Ok(line) => line,
            Err(()) => return Incoming::Refused(400, "негодная строка запроса"),
        },
        Err(LineError::TooLong) => return Incoming::Refused(414, "строка запроса слишком длинная"),
        Err(LineError::Eof) => return Incoming::Refused(400, "строка запроса оборвалась"),
        Err(LineError::Read(kind)) if is_read_timeout(kind) => {
            return Incoming::Refused(408, "строка запроса не завершилась вовремя")
        }
        Err(LineError::Read(_)) => return Incoming::Refused(400, "ошибка чтения строки запроса"),
    };
    let parts: Vec<&str> = line.trim_end_matches(['\r', '\n']).split(' ').collect();
    if parts.len() != 3 || parts[0].is_empty() || parts[1].is_empty() || parts[2] != "HTTP/1.1" {
        return Incoming::Refused(400, "негодная строка запроса HTTP/1.1");
    }
    let method = parts[0].to_string();
    let path = parts[1].to_string();

    let mut len = None;
    let mut close = false;
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut spent = 0usize;
    loop {
        let raw = match bounded_line(reader, MAX_HEADER_LINE) {
            Ok(Some(line)) => line,
            Ok(None) => return Incoming::Refused(400, "заголовки оборвались"),
            Err(LineError::Eof) => return Incoming::Refused(400, "заголовки оборвались"),
            Err(LineError::TooLong) => {
                return Incoming::Refused(431, "строка заголовка слишком длинная")
            }
            Err(LineError::Read(kind)) if is_read_timeout(kind) => {
                return Incoming::Refused(408, "заголовки не завершились вовремя")
            }
            Err(LineError::Read(_)) => return Incoming::Refused(400, "ошибка чтения заголовков"),
        };
        spent = match spent.checked_add(raw.len()) {
            Some(n) if n <= MAX_HEADER_BYTES => n,
            _ => return Incoming::Refused(431, "заголовков слишком много"),
        };
        let h = match text_line(raw) {
            Ok(line) => line,
            Err(()) => return Incoming::Refused(400, "негодная строка заголовка"),
        };
        if h == "\r\n" {
            break;
        }
        if headers.len() >= MAX_HEADERS {
            return Incoming::Refused(431, "заголовков слишком много");
        }
        let Some((raw_name, value)) = h.trim_end_matches(['\r', '\n']).split_once(':') else {
            return Incoming::Refused(400, "у заголовка нет двоеточия");
        };
        // Пробел перед двоеточием запрещён RFC 9112. Принимать его опасно:
        // proxy может проигнорировать такой Content-Length, а backend — нет.
        if raw_name != raw_name.trim() {
            return Incoming::Refused(400, "пробел перед двоеточием заголовка");
        }
        let name = raw_name.to_ascii_lowercase();
        if name.is_empty()
            || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
        {
            return Incoming::Refused(400, "негодное имя заголовка");
        }
        if value.bytes().any(|b| (b < 0x20 && b != b'\t') || b == 0x7f) {
            return Incoming::Refused(400, "управляющий байт в значении заголовка");
        }
        let value = value.trim().to_string();
        match name.as_str() {
            "content-length" => {
                if len.is_some() || value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                    return Incoming::Refused(400, "неоднозначный Content-Length");
                }
                len = match value.parse::<usize>() {
                    Ok(n) => Some(n),
                    Err(_) => return Incoming::Refused(400, "негодный Content-Length"),
                };
            }
            "transfer-encoding" => {
                return Incoming::Refused(400, "Transfer-Encoding не поддерживается")
            }
            "connection" => close = value.eq_ignore_ascii_case("close"),
            _ => {}
        }
        headers.push((name, value));
    }
    let len = len.unwrap_or(0);
    if len > MAX_BODY {
        return if len <= MAX_DISCARD_BODY {
            Incoming::RefusedBody(413, "тело больше допустимого", len)
        } else {
            Incoming::Refused(413, "тело больше допустимого")
        };
    }

    Incoming::Request(Request { method, path, body: Vec::new(), close, headers, body_len: len })
}

/// Дочитывает отвергнутое тело без аллокации его размера.
pub fn discard_body_from(reader: &mut impl BufRead, mut len: usize) -> bool {
    let mut scratch = [0u8; 64 * 1024];
    while len > 0 {
        let take = len.min(scratch.len());
        if reader.read_exact(&mut scratch[..take]).is_err() {
            return false;
        }
        len -= take;
    }
    true
}

pub fn read_body_from(reader: &mut impl BufRead, request: &mut Request) -> bool {
    let mut body = Vec::new();
    if body.try_reserve_exact(request.body_len).is_err() {
        return false;
    }
    body.resize(request.body_len, 0);
    if request.body_len > 0 && reader.read_exact(&mut body).is_err() {
        return false;
    }
    request.body = body;
    true
}

#[cfg(test)]
pub fn read_from(reader: &mut impl BufRead) -> Incoming {
    let mut request = match read_head_from(reader) {
        Incoming::Request(request) => request,
        other => return other,
    };
    if !read_body_from(reader, &mut request) {
        return Incoming::Done;
    }
    Incoming::Request(request)
}

thread_local! {
    /// Заголовки, которые уйдут со **следующим** ответом этого работника.
    ///
    /// Через нить, а не через сигнатуру, и это не лень. Заголовки CORS зависят
    /// от `Origin` конкретного запроса, а отвечает узел из трёх десятков мест.
    /// Протаскивать их параметром через каждую точку значило бы, что однажды
    /// добавленная точка ответит без них — а незаметно пропавший заголовок
    /// CORS выглядит как поломка браузера, и искать её будут неделю.
    ///
    /// Работник обслуживает соединения по очереди и всегда ставит значение
    /// перед разбором запроса, так что чужого здесь не бывает.
    static EXTRA: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

/// Задаёт добавочные заголовки для ответов этого работника.
pub fn set_extra(headers: String) {
    EXTRA.with(|e| *e.borrow_mut() = headers);
}

/// Adds an internal response header without replacing CORS headers.
pub fn append_extra(headers: &str) {
    EXTRA.with(|extra| extra.borrow_mut().push_str(headers));
}

thread_local! {
    /// Код последнего ответа этого работника.
    ///
    /// Через нить, а не через возврат, и по той же причине, что и `EXTRA`
    /// выше: отвечает узел из трёх десятков мест, и код, протянутый обратно
    /// параметром, однажды окажется забыт в новой точке — а метрика,
    /// потерявшая часть ответов, хуже отсутствующей, потому что ей верят.
    ///
    /// Работник обслуживает запросы по очереди и читает значение сразу после
    /// возврата из обработчика, так что чужого здесь не бывает.
    static CODE: std::cell::Cell<u16> = const { std::cell::Cell::new(0) };
}

/// Код последнего ответа этого работника. Ноль — ответа не было.
pub fn last_code() -> u16 {
    CODE.with(std::cell::Cell::get)
}

/// Забывает код: зовётся перед обработкой, чтобы «ответа не было» отличалось
/// от «ответ был у прошлого запроса».
pub fn forget_code() {
    CODE.with(|c| c.set(0));
}

/// Отвечает; `keep` — оставлять ли соединение открытым.
pub fn respond_with(stream: &mut crate::wire::Wire, code: u16, payload: &str, keep: bool) {
    respond_typed(stream, code, payload, keep, "application/json")
}

pub fn respond_typed(
    stream: &mut crate::wire::Wire,
    code: u16,
    payload: &str,
    keep: bool,
    kind: &str,
) {
    CODE.with(|c| c.set(code));
    let extra = EXTRA.with(|e| e.borrow().clone());
    let _ = write!(
        stream,
        "HTTP/1.1 {code} {}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: {}\r\n{extra}\r\n{payload}",
        reason(code),
        payload.len(),
        if keep { "keep-alive" } else { "close" }
    );
    let _ = stream.flush();
}

/// Отказ с заголовком `WWW-Authenticate`.
///
/// Отдельная точка, потому что 401 без этого заголовка — не полный ответ:
/// клиент обязан узнать, чем именно представляться, а браузер без него не
/// покажет окно входа.
pub fn respond_unauthorized(stream: &mut crate::wire::Wire, payload: &str, keep: bool) {
    CODE.with(|c| c.set(401));
    let extra = EXTRA.with(|e| e.borrow().clone());
    let _ = write!(
        stream,
        "HTTP/1.1 401 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         WWW-Authenticate: Basic realm=\"svindb\", charset=\"UTF-8\"\r\nConnection: {}\r\n{extra}\r\n{payload}",
        reason(401),
        payload.len(),
        if keep { "keep-alive" } else { "close" }
    );
    let _ = stream.flush();
}

/// Ответ без тела — для предполётного запроса CORS.
pub fn respond_empty(stream: &mut crate::wire::Wire, code: u16, keep: bool) {
    CODE.with(|c| c.set(code));
    let extra = EXTRA.with(|e| e.borrow().clone());
    let _ = write!(
        stream,
        "HTTP/1.1 {code} {}\r\nContent-Length: 0\r\nConnection: {}\r\n{extra}\r\n",
        reason(code),
        if keep { "keep-alive" } else { "close" }
    );
    let _ = stream.flush();
}

/// Причина по коду. Коды перечислены все, что узел вправе вернуть: клиенты
/// разбирают именно код, а слово читает человек в журнале.
fn reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        // Места на томе не осталось. Отдельный код, а не 503: 503 говорит
        // «попробуйте позже», а здесь позже само не пройдёт — нужен диск.
        507 => "Insufficient Storage",
        _ => "Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(raw: &str) -> Incoming {
        read_from(&mut std::io::BufReader::new(raw.as_bytes()))
    }

    fn want(raw: &str) -> Request {
        match read(raw) {
            Incoming::Request(r) => r,
            Incoming::Refused(c, why) | Incoming::RefusedBody(c, why, _) => {
                panic!("отказ {c}: {why}")
            }
            Incoming::Done => panic!("разбор оборвался"),
        }
    }

    #[test]
    fn a_header_value_keeps_its_case() {
        let r = want(
            "GET /_health HTTP/1.1\r\nAUTHORIZATION: Basic dXNlcjpQYVNz\r\n\
             X-Empty:\r\nContent-Length: 0\r\n\r\n",
        );
        assert_eq!(r.header("authorization"), Some("Basic dXNlcjpQYVNz"));
        // Имя ищется в нижнем регистре, как бы его ни написали.
        assert_eq!(r.header("x-empty"), Some(""));
        assert_eq!(r.method, "GET");
        assert_eq!(r.path, "/_health");
    }

    #[test]
    fn the_body_arrives_whole() {
        let r = want("POST /i/_doc HTTP/1.1\r\nContent-Length: 7\r\n\r\n{\"a\":1}");
        assert_eq!(r.body, b"{\"a\":1}");
    }

    /// Слишком большое тело — код, а не обрыв. Клиент обязан отличать «пачка
    /// великовата» от упавшего узла: первое чинится размером пачки, второе
    /// звонком дежурному.
    #[test]
    fn an_oversized_body_gets_a_code_not_a_dropped_connection() {
        let raw = format!("POST /i/_bulk HTTP/1.1\r\nContent-Length: {}\r\n\r\n", MAX_BODY + 1);
        match read(&raw) {
            Incoming::RefusedBody(413, _, n) => assert_eq!(n, MAX_BODY + 1),
            _ => panic!("тело больше потолка прошло молча"),
        }
    }

    /// Заголовки тоже ограничены: без этого узел занимают памятью через дверь,
    /// которую забыли запереть.
    #[test]
    fn endless_headers_are_refused() {
        let mut raw = String::from("GET / HTTP/1.1\r\n");
        for i in 0..MAX_HEADERS + 10 {
            raw.push_str(&format!("x-{i}: 1\r\n"));
        }
        raw.push_str("\r\n");
        match read(&raw) {
            Incoming::Refused(431, _) => {}
            _ => panic!("бесконечные заголовки прошли"),
        }
    }

    #[test]
    fn one_long_line_is_stopped_before_unbounded_growth() {
        let raw = format!("GET /{} HTTP/1.1\r\n\r\n", "x".repeat(MAX_REQUEST_LINE));
        assert!(matches!(read(&raw), Incoming::Refused(414, _)));
        let raw = format!("GET / HTTP/1.1\r\nx: {}\r\n\r\n", "x".repeat(MAX_HEADER_LINE));
        assert!(matches!(read(&raw), Incoming::Refused(431, _)));
    }

    #[test]
    fn ambiguous_framing_is_refused_and_not_reinterpreted() {
        for raw in [
            "POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx",
            "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
            "POST / HTTP/1.1\r\nContent-Length: nope\r\n\r\n",
            "POST / HTTP/1.1\r\nContent-Length : 1\r\n\r\nx",
        ] {
            assert!(matches!(read(raw), Incoming::Refused(400, _)), "прошло: {raw:?}");
        }
    }

    #[test]
    fn body_budget_is_global_and_released_by_the_permit() {
        let budget = Arc::new(BodyBudget::new(10));
        let first = budget.reserve(7).expect("первая бронь");
        let full = budget.reserve(4).expect_err("общий потолок превышен");
        assert_eq!((full.want, full.used, full.max), (4, 7, 10));
        let second = budget.reserve(3).expect("ровно до потолка");
        drop(first);
        assert!(budget.reserve(7).is_ok(), "освобождение permit не вернуло бюджет");
        drop(second);
    }

    #[test]
    fn cgroup_memory_reduces_the_global_body_budget_before_oom() {
        assert_eq!(inflight_body_limit(Some(1 << 30)), 32 << 20);
        assert_eq!(inflight_body_limit(Some(2 << 30)), 64 << 20);
        assert_eq!(inflight_body_limit(Some(4 << 30)), MAX_INFLIGHT_BODY_BYTES);
        assert_eq!(inflight_body_limit(Some(128 << 30)), MAX_INFLIGHT_BODY_BYTES);
        assert_eq!(inflight_body_limit(None), MAX_INFLIGHT_BODY_BYTES);
    }

    #[test]
    fn a_closed_connection_is_not_a_request() {
        assert!(matches!(read(""), Incoming::Done));
    }

    #[test]
    fn every_code_the_node_returns_has_a_word() {
        for c in
            [200u16, 201, 204, 400, 401, 403, 404, 405, 408, 409, 413, 414, 415, 429, 431, 500, 503]
        {
            assert_ne!(reason(c), "Error", "код {c} без причины");
        }
    }
}
