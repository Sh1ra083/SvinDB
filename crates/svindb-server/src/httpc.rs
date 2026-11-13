//! HTTP-клиент: ровно столько, сколько нужно координатору шардов.
//!
//! ## Почему свой, а не крейт
//!
//! Нужен один запрос: `POST` с телом JSON, ответ целиком, таймаут. Обёртка вокруг
//! этого потянула бы в дерево асинхронную среду исполнения и десятки зависимостей
//! ради того, что здесь занимает сто строк. У проекта одна зависимость на всё
//! (`serde_json`, и та на границе), и менять это ради `POST` было бы странно.
//!
//! ## Чего он намеренно не умеет
//!
//! Ни TLS, ни `chunked`, ни перенаправлений, ни keep-alive. Он ходит к **своим**
//! шардам в доверенной сети, а не в интернет: их адреса заданы в настройках, и
//! отвечает им тот же движок. Всё, чего он не умеет, он называет отказом, а не
//! пытается угадать.
//!
//! TLS для связи между узлами придётся ставить проксировщиком, как и для входящих
//! запросов, — у узла его нет и здесь.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Ответ шарда.
pub struct Reply {
    pub code: u16,
    pub body: String,
}

/// Разбирает `host:port` и отдаёт адрес для соединения.
fn split(addr: &str) -> Result<(String, u16), String> {
    let (host, port) =
        addr.rsplit_once(':').ok_or_else(|| format!("адрес шарда {addr:?}: нужен host:port"))?;
    let port: u16 = port.parse().map_err(|_| format!("адрес шарда {addr:?}: порт не число"))?;
    if host.is_empty() {
        return Err(format!("адрес шарда {addr:?}: пустой хост"));
    }
    Ok((host.to_string(), port))
}

/// `POST` с телом JSON. `timeout` — и на соединение, и на чтение.
///
/// Тело ответа читается по `Content-Length`. Ответ без него — отказ, а не чтение
/// до закрытия: шард обязан говорить длину, и «читать, пока дают» на своём же
/// движке означало бы, что мы разбираем не то, что он послал.
pub fn post_json(addr: &str, path: &str, body: &str, timeout: Duration) -> Result<Reply, String> {
    let (host, port) = split(addr)?;
    // Перебираются **все** разрешённые адреса, а не первый.
    //
    // `localhost` на macOS разрешается сначала в `::1`, а узел по умолчанию
    // слушает `127.0.0.1`. Взять первый адрес значило бы отказать на самой
    // естественной записи, какую напишет человек: `--shards=localhost:19300`
    // возвращал «Connection refused» при живом и работающем шарде.
    let targets: Vec<_> = std::net::ToSocketAddrs::to_socket_addrs(&(host.as_str(), port))
        .map_err(|e| format!("{addr}: {e}"))?
        .collect();
    if targets.is_empty() {
        return Err(format!("{addr}: адрес не разрешился"));
    }
    let mut last = String::new();
    let mut sock = None;
    for t in targets.iter() {
        match TcpStream::connect_timeout(t, timeout) {
            Ok(s) => {
                sock = Some(s);
                break;
            }
            Err(e) => last = format!("{addr}: {e}"),
        }
    }
    let mut s = sock.ok_or(last)?;
    s.set_read_timeout(Some(timeout)).map_err(|e| format!("{addr}: {e}"))?;
    s.set_write_timeout(Some(timeout)).map_err(|e| format!("{addr}: {e}"))?;
    let _ = s.set_nodelay(true);

    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).map_err(|e| format!("{addr}: {e}"))?;
    s.flush().map_err(|e| format!("{addr}: {e}"))?;

    // Заголовки читаются побайтно до пустой строки: их немного, а буферизация
    // через `BufReader` здесь только мешала бы — тело всё равно читается по длине.
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    // Потолок на заголовки: шард своих не пришлёт, а без потолка отвечающий не
    // тем протоколом держал бы нас до таймаута, накапливая память.
    while head.len() < 16 * 1024 {
        match s.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(e) => return Err(format!("{addr}: {e}")),
        }
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8_lossy(&head).to_string();
    let mut lines = head.lines();
    let status = lines.next().unwrap_or_default();
    let code: u16 = status
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| format!("{addr}: не разобрана строка статуса {status:?}"))?;
    let len: usize = lines
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse().ok())?
        })
        .ok_or_else(|| format!("{addr}: ответ без Content-Length"))?;

    let mut body = vec![0u8; len];
    s.read_exact(&mut body).map_err(|e| format!("{addr}: тело не дочиталось: {e}"))?;
    Ok(Reply { code, body: String::from_utf8_lossy(&body).to_string() })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Адрес разбирается, а негодный — отказ с причиной.
    #[test]
    fn addresses_are_parsed_or_refused() {
        assert_eq!(split("localhost:19300").unwrap(), ("localhost".into(), 19300));
        assert_eq!(split("10.0.0.1:1").unwrap(), ("10.0.0.1".into(), 1));
        for bad in ["localhost", ":19300", "localhost:", "localhost:нет", "localhost:99999"] {
            assert!(split(bad).is_err(), "адрес {bad:?} принят");
        }
    }

    /// Недоступный шард — отказ, а не ожидание навсегда.
    #[test]
    fn an_unreachable_shard_fails_fast() {
        // Порт, который заведомо никто не слушает: занимаем и сразу отпускаем.
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let t = std::time::Instant::now();
        let e = post_json(&format!("127.0.0.1:{port}"), "/", "{}", Duration::from_millis(500))
            .err()
            .expect("соединение с закрытым портом удалось");
        assert!(t.elapsed() < Duration::from_secs(3), "отказ занял {:?}", t.elapsed());
        assert!(e.contains(&port.to_string()), "отказ не назвал адрес: {e}");
    }

    /// Ответ без `Content-Length` — отказ, а не чтение до закрытия.
    #[test]
    fn a_reply_without_a_length_is_refused() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut c, _)) = l.accept() {
                let mut junk = [0u8; 1024];
                let _ = c.read(&mut junk);
                let _ = c.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{}");
            }
        });
        let e = post_json(&format!("127.0.0.1:{port}"), "/", "{}", Duration::from_secs(2))
            .err()
            .expect("ответ без длины принят");
        assert!(e.contains("Content-Length"), "отказ не про длину: {e}");
    }

    /// Обычный круг: код и тело доходят целиком.
    #[test]
    fn a_normal_reply_arrives_whole() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let want = "{\"hits\":[1,2,3]}";
        std::thread::spawn(move || {
            if let Ok((mut c, _)) = l.accept() {
                let mut junk = [0u8; 4096];
                let _ = c.read(&mut junk);
                let _ = c.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{want}",
                        want.len()
                    )
                    .as_bytes(),
                );
            }
        });
        let r = post_json(&format!("127.0.0.1:{port}"), "/", "{}", Duration::from_secs(2))
            .expect("обычный ответ");
        assert_eq!(r.code, 200);
        assert_eq!(r.body, want);
    }
}
