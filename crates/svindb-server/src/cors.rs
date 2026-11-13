//! Заголовки доступа для запросов из браузера.

use crate::config::Cors;

/// Заголовки, которые уйдут с обычным ответом. Пусто — источник не разрешён
/// или CORS выключен.
pub fn headers(cfg: &Cors, origin: Option<&str>) -> String {
    let Some(allow) = allowed(cfg, origin) else { return String::new() };
    let mut out = format!("Access-Control-Allow-Origin: {allow}\r\n");
    if cfg.credentials {
        out.push_str("Access-Control-Allow-Credentials: true\r\n");
    }
    // `Vary` обязателен, когда ответ зависит от источника: без него
    // промежуточный кеш отдаст разрешение одного сайта другому.
    if allow != "*" {
        out.push_str("Vary: Origin\r\n");
    }
    out
}

/// Заголовки предполётного ответа. Он же говорит, какие методы и заголовки
/// позволены — браузер спрашивает **до** самого запроса и без ответа его не
/// пошлёт.
pub fn preflight(cfg: &Cors, origin: Option<&str>, ask_headers: Option<&str>) -> String {
    let mut out = headers(cfg, origin);
    if out.is_empty() {
        return out;
    }
    out.push_str("Access-Control-Allow-Methods: GET, POST, PUT, DELETE, HEAD, OPTIONS\r\n");
    // Отражаем запрошенные заголовки, а не перечисляем свои: у клиента может
    // быть `Authorization`, `Content-Type`, свой след запроса — перечислять их
    // здесь значило бы ломать доступ всякий раз, когда клиент добавит ещё один.
    let ask = ask_headers.unwrap_or("Authorization, Content-Type");
    out.push_str(&format!("Access-Control-Allow-Headers: {ask}\r\n"));
    out.push_str(&format!("Access-Control-Max-Age: {}\r\n", cfg.max_age));
    out
}

/// Источник небезопасного браузерного запроса должен быть либо тем же узлом,
/// либо явно разрешён политикой. Отсутствие `Origin` оставлено для CLI и
/// серверных клиентов; браузер при межсайтовом POST этот заголовок добавляет.
pub fn trusted_for_write(cfg: &Cors, origin: &str, host: Option<&str>) -> bool {
    // `*` годится для чтения публичного ответа, но не является доверием на
    // действие: иначе политика «читать может каждый» становилась CSRF-правом.
    if cfg.origins.iter().any(|allowed| allowed != "*" && allowed == origin) {
        return true;
    }
    let authority = origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))
        .and_then(|rest| rest.split('/').next());
    authority
        .zip(host)
        .is_some_and(|(origin_host, request_host)| origin_host.eq_ignore_ascii_case(request_host))
}

/// Что поставить в `Access-Control-Allow-Origin`.
fn allowed(cfg: &Cors, origin: Option<&str>) -> Option<String> {
    if cfg.origins.is_empty() {
        return None;
    }
    let origin = origin?;
    if cfg.origins.iter().any(|o| o == "*") {
        // При учётных данных звёздочка не годится, и разбор настроек такую пару
        // не пропускает; здесь остаётся вернуть сам источник, если он назван.
        return Some("*".to_string());
    }
    cfg.origins.iter().find(|o| *o == origin).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(origins: &[&str], credentials: bool) -> Cors {
        Cors { origins: origins.iter().map(|s| s.to_string()).collect(), credentials, max_age: 600 }
    }

    #[test]
    fn without_a_list_no_headers_go_out() {
        assert_eq!(headers(&cfg(&[], false), Some("https://свои.рф")), "");
    }

    #[test]
    fn a_stranger_gets_nothing() {
        let c = cfg(&["https://свои.рф"], false);
        assert_eq!(headers(&c, Some("https://чужие.рф")), "");
        // Совпадение точное: поддомен чужого сайта не проходит по началу строки.
        assert_eq!(headers(&c, Some("https://свои.рф.чужой.сайт")), "");
        assert!(headers(&c, Some("https://свои.рф")).contains("https://свои.рф"));
    }

    #[test]
    fn a_named_origin_gets_vary_so_caches_do_not_mix_answers() {
        let h = headers(&cfg(&["https://свои.рф"], true), Some("https://свои.рф"));
        assert!(h.contains("Vary: Origin"), "{h}");
        assert!(h.contains("Access-Control-Allow-Credentials: true"), "{h}");
    }

    /// Предполётный ответ отражает запрошенные заголовки: перечислять свои
    /// значило бы ломать доступ всякий раз, когда клиент добавит ещё один.
    #[test]
    fn the_preflight_reflects_what_was_asked() {
        let c = cfg(&["https://свои.рф"], false);
        let h = preflight(&c, Some("https://свои.рф"), Some("authorization, x-своё"));
        assert!(h.contains("Access-Control-Allow-Headers: authorization, x-своё"), "{h}");
        assert!(h.contains("Access-Control-Max-Age: 600"), "{h}");
        assert!(h.contains("Access-Control-Allow-Methods"), "{h}");
        // Чужому предполётный ответ не даёт ничего.
        assert_eq!(preflight(&c, Some("https://чужие.рф"), None), "");
    }

    #[test]
    fn a_star_answers_everyone_without_vary() {
        let h = headers(&cfg(&["*"], false), Some("https://кто-угодно.рф"));
        assert!(h.contains("Access-Control-Allow-Origin: *"), "{h}");
        assert!(!h.contains("Vary"), "звёздочка одна на всех — различать нечего");
    }

    #[test]
    fn unsafe_requests_accept_same_host_and_reject_foreign_origin() {
        let c = cfg(&[], false);
        assert!(trusted_for_write(&c, "https://db.example:9443", Some("db.example:9443")));
        assert!(!trusted_for_write(&c, "https://evil.example", Some("db.example:9443")));
        assert!(!trusted_for_write(&c, "null", Some("db.example:9443")));
        assert!(!trusted_for_write(
            &cfg(&["*"], false),
            "https://evil.example",
            Some("db.example:9443")
        ));
    }
}
