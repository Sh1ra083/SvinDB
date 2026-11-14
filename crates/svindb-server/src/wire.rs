//! Соединение с клиентом: одна ручка на чтение и на запись.
//!
//! ## Почему одна, а не две
//!
//! Работник делал `try_clone()` и держал **две независимые ручки** на один
//! сокет: читал через одну, отвечал в другую. Для голого TCP это работает —
//! ядро всё равно сводит их к одному соединению, — но для шифрованного нет:
//! сеанс TLS это счётчики блоков и ключи в одном месте, и две половины
//! разъедутся на первом же ответе.
//!
//! Поэтому переход на одну ручку — не подготовка к TLS, а его условие. Он
//! полезен и сам по себе: `try_clone` это лишний вызов ядра и лишний
//! дескриптор на каждое соединение, а при восьмидесяти работниках и keep-alive
//! их набирается заметно.
//!
//! ## Почему дедлайн **под** шифрованием
//!
//! Срок ждёт [`Wire::renew`] и отсчитывается от голого сокета, то есть **ниже**
//! TLS. Иначе одно логическое чтение приложения превращается в несколько чтений
//! из сокета — запись TLS приходит кусками, — и каждому досталось бы полное
//! ожидание заново: клиент, шлющий по байту, держал бы работника вечно, хотя
//! срок формально соблюдён.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

/// Сокет со сроком на чтение.
///
/// Срок общий на весь запрос, а не на каждое чтение: `set_read_timeout` ставит
/// его на **отдельный** вызов, и клиент, присылающий по байту раз в пятьдесят
/// девять секунд, укладывался бы в него бесконечно.
pub struct Deadline {
    stream: TcpStream,
    deadline: Instant,
    header_progress: Option<(Duration, bool)>,
}

impl Deadline {
    pub fn new(stream: TcpStream, timeout: Duration) -> Deadline {
        Deadline { stream, deadline: Instant::now() + timeout, header_progress: None }
    }

    pub fn renew(&mut self, timeout: Duration) {
        self.deadline = Instant::now() + timeout;
        self.header_progress = None;
    }

    /// Starts a request-header read with a long idle timeout and a short
    /// progress timeout after the first byte arrives. This avoids peeking a
    /// cloned descriptor (which races packet delivery) while still terminating
    /// a client that holds an incomplete header open.
    pub fn begin_header(&mut self, progress_timeout: Duration) {
        self.header_progress = Some((progress_timeout, false));
    }

    fn header_progressed(&mut self) {
        if let Some((progress_timeout, started)) = &mut self.header_progress {
            if !*started {
                *started = true;
                self.deadline = Instant::now() + *progress_timeout;
            }
        }
    }

    /// Отдельная ручка только для наблюдения за закрытием транспорта.
    ///
    /// Она не читает данные и не участвует в TLS-сеансе: `peek` смотрит лишь
    /// на состояние TCP. Поэтому счётчики и ключи шифрования по-прежнему живут
    /// в единственной основной ручке [`Wire`].
    fn transport_clone(&self) -> std::io::Result<TcpStream> {
        self.stream.try_clone()
    }
}

impl Read for Deadline {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "дедлайн запроса истёк"));
        }
        self.stream.set_read_timeout(Some(left.max(Duration::from_millis(1))))?;
        self.stream.read(buf)
    }
}

impl Write for Deadline {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

/// Соединение: голое либо шифрованное.
///
/// Перечислением, а не типажом-объектом: ветвей ровно две и третьей не
/// предвидится, а `dyn` стоил бы косвенного вызова на каждый `read` в разборе
/// заголовка — то есть на каждый байт запроса.
pub enum Wire {
    Plain(Deadline),
    #[cfg(feature = "tls")]
    /// В коробке: `ServerConnection` весит несколько килобайт, и без неё
    /// `Wire` раздувал бы каждое голое соединение до размера шифрованного.
    Tls(Box<rustls::StreamOwned<rustls::ServerConnection, Deadline>>),
}

impl Wire {
    pub fn plain(stream: TcpStream, timeout: Duration) -> Wire {
        Wire::Plain(Deadline::new(stream, timeout))
    }

    /// Продлевает срок чтения. Зовётся между запросами keep-alive и перед
    /// чтением тела: заголовок и тело — разные ожидания.
    pub fn renew(&mut self, timeout: Duration) {
        match self {
            Wire::Plain(d) => d.renew(timeout),
            #[cfg(feature = "tls")]
            Wire::Tls(s) => s.get_mut().renew(timeout),
        }
    }

    pub fn begin_header(&mut self, progress_timeout: Duration) {
        match self {
            Wire::Plain(d) => d.begin_header(progress_timeout),
            #[cfg(feature = "tls")]
            Wire::Tls(s) => s.get_mut().begin_header(progress_timeout),
        }
    }

    /// Клонирует нижележащий TCP-сокет для пассивного контроля disconnect.
    ///
    /// Клон нельзя использовать для чтения: у TLS это обошло бы расшифровку.
    /// Допустим только `peek`, который различает живое соединение и EOF.
    pub fn transport_clone(&self) -> std::io::Result<TcpStream> {
        match self {
            Wire::Plain(d) => d.transport_clone(),
            #[cfg(feature = "tls")]
            Wire::Tls(s) => s.get_ref().transport_clone(),
        }
    }
}

impl Read for Wire {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Wire::Plain(d) => {
                let read = d.read(buf)?;
                if read > 0 {
                    d.header_progressed();
                }
                Ok(read)
            }
            #[cfg(feature = "tls")]
            Wire::Tls(s) => {
                let read = s.read(buf)?;
                // TLS handshake records are not HTTP header progress. Start
                // the short deadline only after decrypted application bytes.
                if read > 0 {
                    s.get_mut().header_progressed();
                }
                Ok(read)
            }
        }
    }
}

impl Write for Wire {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Wire::Plain(d) => d.write(buf),
            #[cfg(feature = "tls")]
            Wire::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Wire::Plain(d) => d.flush(),
            #[cfg(feature = "tls")]
            Wire::Tls(s) => s.flush(),
        }
    }
}

#[cfg(feature = "tls")]
pub use tls::{accept, load, Acceptor};

/// Без признака сборки шифрования не бывает **как понятия**.
///
/// Пустое перечисление, а не `struct`: тогда `Option<Acceptor>` может быть
/// только `None`, и это проверяет сборщик. Заведи здесь пустышку — и ветку
/// «шифрованное соединение» пришлось бы писать заглушкой, которая молча
/// отвечала бы открытым текстом.
#[cfg(not(feature = "tls"))]
#[derive(Clone)]
pub enum Acceptor {}

#[cfg(feature = "tls")]
mod tls {
    use super::{Deadline, Wire};
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::time::Duration;

    /// Готовый к работе набор ключей.
    pub type Acceptor = Arc<rustls::ServerConfig>;

    /// Читает цепочку и ключ из PEM и собирает настройку сеанса.
    ///
    /// Разбор PEM — средствами самого `rustls` (`pki_types::pem`), отдельная
    /// зависимость для этого не нужна.
    pub fn load(cert: &str, key: &str) -> Result<Acceptor, String> {
        use rustls::pki_types::pem::PemObject;

        // Поставщик шифров выбирается **явно**.
        //
        // `ServerConfig::builder()` берёт умолчание процесса, а когда собраны
        // оба поставщика (`ring` и `aws-lc-rs`), умолчания нет — и он
        // **паникует**. Сборка выпуска идёт с `panic = "abort"`, так что это
        // смерть узла при старте без единого слова о причине.
        let provider = Arc::new(rustls::crypto::ring::default_provider());

        let certs: Vec<_> = rustls::pki_types::CertificateDer::pem_file_iter(cert)
            .map_err(|e| format!("сертификат {cert}: {e}"))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("сертификат {cert}: {e}"))?;
        if certs.is_empty() {
            return Err(format!("сертификат {cert}: в файле нет ни одного CERTIFICATE"));
        }
        let key = rustls::pki_types::PrivateKeyDer::from_pem_file(key)
            .map_err(|e| format!("ключ {key}: {e}"))?;

        let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("настройка TLS: {e}"))?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("сертификат и ключ не сходятся: {e}"))?;

        // Обязательно: curl и браузер предлагают h2 первым, и без явного
        // списка узел согласился бы на HTTP/2 — а разбирает он строку запроса.
        // Двоичные кадры h2 он принял бы за неё и отвечал бы 400 на всё.
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Arc::new(cfg))
    }

    /// Принимает соединение: рукопожатие идёт под тем же сроком, что и запрос.
    ///
    /// Срок здесь несущий: рукопожатие — это чтение из сокета, и клиент,
    /// открывший соединение и замолчавший, без него занял бы работника до конца
    /// жизни процесса. Ровно от этого и заведён [`Deadline`].
    pub fn accept(cfg: &Acceptor, stream: TcpStream, timeout: Duration) -> std::io::Result<Wire> {
        let mut conn = rustls::ServerConnection::new(Arc::clone(cfg))
            .map_err(|e| std::io::Error::other(format!("сеанс TLS: {e}")))?;
        let mut sock = Deadline::new(stream, timeout);
        // Рукопожатие доводится до конца **здесь**, а не лениво при первом
        // чтении: иначе его отказ выглядел бы как негодный запрос, и в журнале
        // вместо «клиент не тем сертификатом» стояло бы «тело не разобралось».
        while conn.is_handshaking() {
            conn.complete_io(&mut sock)?;
        }
        Ok(Wire::Tls(Box::new(rustls::StreamOwned::new(conn, sock))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;

    /// Одна ручка читает и пишет тот же сокет.
    ///
    /// Прежде их было две, и подмена ошиблась бы молча: голый TCP свёл бы обе
    /// к одному соединению, и проверка прошла бы при любой раскладке.
    #[test]
    fn one_handle_reads_and_writes_the_same_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("порт");
        let addr = listener.local_addr().expect("адрес");
        let client = std::thread::spawn(move || {
            let mut c = TcpStream::connect(addr).expect("соединение");
            c.write_all("вопрос\n".as_bytes()).expect("запрос");
            let mut back = String::new();
            BufReader::new(&mut c).read_line(&mut back).expect("ответ");
            back
        });

        let (sock, _) = listener.accept().expect("приём");
        let mut w = Wire::plain(sock, Duration::from_secs(5));
        let mut got = String::new();
        {
            let mut r = BufReader::new(&mut w);
            r.read_line(&mut got).expect("чтение");
        }
        assert_eq!(got, "вопрос\n");
        w.write_all("ответ\n".as_bytes()).expect("запись");
        w.flush().expect("сброс");
        assert_eq!(client.join().expect("нить"), "ответ\n");
    }

    /// Срок общий на весь обмен, а не на каждое чтение.
    ///
    /// `set_read_timeout` ставит его на **отдельный** вызов, и клиент,
    /// присылающий по байту чуть чаще срока, укладывался бы в него вечно.
    #[test]
    fn the_deadline_covers_the_whole_exchange() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("порт");
        let addr = listener.local_addr().expect("адрес");
        // Клиент открывает соединение и молчит, держа его открытым.
        let client = std::thread::spawn(move || {
            let c = TcpStream::connect(addr).expect("соединение");
            std::thread::sleep(Duration::from_millis(600));
            drop(c);
        });

        let (sock, _) = listener.accept().expect("приём");
        let mut w = Wire::plain(sock, Duration::from_millis(150));
        let began = Instant::now();
        let mut buf = [0u8; 16];
        let e = w.read(&mut buf).expect_err("молчание обязано истечь");
        assert!(
            matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock),
            "{e:?}"
        );
        assert!(began.elapsed() < Duration::from_millis(500), "ждали {:?}", began.elapsed());
        client.join().expect("нить");
    }

    /// Негодные файлы ключей — отказ со словами, а не паника.
    ///
    /// Сборка выпуска идёт с `panic = "abort"`: паника при старте — смерть узла
    /// без единого слова о причине, и разбираться пришлось бы по коду выхода.
    #[cfg(feature = "tls")]
    #[test]
    fn bad_key_material_is_refused_with_words() {
        let dir = std::env::temp_dir().join("svindb-wire-tls");
        std::fs::create_dir_all(&dir).expect("каталог");
        let junk = dir.join("junk.pem");
        std::fs::write(&junk, "это не PEM\n").expect("файл");
        let junk = junk.to_string_lossy().to_string();

        let e = load(&junk, &junk).expect_err("мусор принят");
        assert!(e.contains("сертификат"), "{e}");
        let e = load("/нет/такого", &junk).expect_err("отсутствующий файл принят");
        assert!(e.contains("сертификат"), "{e}");
    }
}
