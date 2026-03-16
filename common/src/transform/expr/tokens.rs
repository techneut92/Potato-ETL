//! Tokenizer for the expression DSL.

// ── Token ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Ident(String),
    Int(i64),
    Float(f64),
    Str(String),
    /// `$name` — reference to a pipeline environment variable.
    EnvVar(String),
    Plus,
    Minus,
    Star,
    Slash,
    EqEq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    LParen,
    RParen,
    Comma,
    Dot,
    LBracket,
    RBracket,
    Eof,
}

// ── Tokenizer ─────────────────────────────────────────────────────────────────

pub fn tokenize(src: &str) -> anyhow::Result<Vec<Tok>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = src.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' | '\r' | '\n' => { i += 1; }
            '+' => { tokens.push(Tok::Plus);     i += 1; }
            '*' => { tokens.push(Tok::Star);     i += 1; }
            '/' => { tokens.push(Tok::Slash);    i += 1; }
            '(' => { tokens.push(Tok::LParen);   i += 1; }
            ')' => { tokens.push(Tok::RParen);   i += 1; }
            ',' => { tokens.push(Tok::Comma);    i += 1; }
            '.' => { tokens.push(Tok::Dot);      i += 1; }
            '[' => { tokens.push(Tok::LBracket); i += 1; }
            ']' => { tokens.push(Tok::RBracket); i += 1; }
            '-' => { tokens.push(Tok::Minus);    i += 1; }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Tok::Le); i += 2;
                } else {
                    tokens.push(Tok::Lt); i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Tok::Ge); i += 2;
                } else {
                    tokens.push(Tok::Gt); i += 1;
                }
            }
            '=' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Tok::EqEq); i += 2;
                } else {
                    // Single = also accepted as ==
                    tokens.push(Tok::EqEq); i += 1;
                }
            }
            '!' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Tok::Ne); i += 2;
                } else {
                    anyhow::bail!("unexpected '!' at position {i}");
                }
            }
            '"' | '\'' => {
                let quote = chars[i];
                i += 1;
                let mut s = String::new();
                while i < chars.len() && chars[i] != quote {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        i += 1;
                        s.push(match chars[i] { 'n' => '\n', 't' => '\t', c => c });
                    } else {
                        s.push(chars[i]);
                    }
                    i += 1;
                }
                if i >= chars.len() { anyhow::bail!("unterminated string literal"); }
                i += 1; // closing quote
                tokens.push(Tok::Str(s));
            }
            '$' => {
                i += 1;
                let mut s = String::new();
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    s.push(chars[i]);
                    i += 1;
                }
                if s.is_empty() { anyhow::bail!("expected identifier after '$' at position {i}"); }
                tokens.push(Tok::EnvVar(s));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() { i += 1; }
                let is_float = i < chars.len() && chars[i] == '.';
                if is_float {
                    i += 1;
                    while i < chars.len() && chars[i].is_ascii_digit() { i += 1; }
                    let s: String = chars[start..i].iter().collect();
                    tokens.push(Tok::Float(s.parse()?));
                } else {
                    let s: String = chars[start..i].iter().collect();
                    tokens.push(Tok::Int(s.parse()?));
                }
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                tokens.push(Tok::Ident(word));
            }
            other => anyhow::bail!("unexpected character '{other}' at position {i}"),
        }
    }

    tokens.push(Tok::Eof);
    Ok(tokens)
}
