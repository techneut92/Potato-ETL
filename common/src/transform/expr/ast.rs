//! AST types and recursive-descent parser.

use super::tokens::{Tok, tokenize};

// ── AST ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Expr {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
    Column(String),
    BinOp { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    Unary  { op: UnaryOp, operand: Box<Expr> },
    Call   { func: String, args: Vec<Expr> },
    /// `col[idx]` — used internally for parsed bracket indexing
    Index  { expr: Box<Expr>, index: Box<Expr> },
    /// `$name` — reference to a pipeline environment variable.
    EnvVar(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinOp { Add, Sub, Mul, Div, Eq, Ne, Lt, Le, Gt, Ge, And, Or }

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp { Neg, Not }

// ── Parser ────────────────────────────────────────────────────────────────────

struct Parser { tokens: Vec<Tok>, pos: usize }

impl Parser {
    fn peek(&self) -> &Tok { &self.tokens[self.pos] }

    fn advance(&mut self) -> &Tok {
        let t = &self.tokens[self.pos];
        if self.pos + 1 < self.tokens.len() { self.pos += 1; }
        t
    }

    fn expect_ident(&mut self) -> anyhow::Result<String> {
        match self.advance().clone() {
            Tok::Ident(s) => Ok(s),
            t => anyhow::bail!("expected identifier, got {t:?}"),
        }
    }

    fn eat(&mut self, expected: &Tok) -> bool {
        if self.peek() == expected {
            self.advance();
            true
        } else {
            false
        }
    }

    // or_expr = and_expr ('or' and_expr)*
    fn or_expr(&mut self) -> anyhow::Result<Expr> {
        let mut left = self.and_expr()?;
        while let Tok::Ident(kw) = self.peek().clone() {
            if kw.to_lowercase() != "or" { break; }
            self.advance();
            let right = self.and_expr()?;
            left = Expr::BinOp { op: BinOp::Or, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    // and_expr = not_expr ('and' not_expr)*
    fn and_expr(&mut self) -> anyhow::Result<Expr> {
        let mut left = self.not_expr()?;
        while let Tok::Ident(kw) = self.peek().clone() {
            if kw.to_lowercase() != "and" { break; }
            self.advance();
            let right = self.not_expr()?;
            left = Expr::BinOp { op: BinOp::And, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    // not_expr = 'not' not_expr | cmp_expr
    fn not_expr(&mut self) -> anyhow::Result<Expr> {
        if let Tok::Ident(kw) = self.peek().clone() {
            if kw.to_lowercase() == "not" {
                self.advance();
                let operand = self.not_expr()?;
                return Ok(Expr::Unary { op: UnaryOp::Not, operand: Box::new(operand) });
            }
        }
        self.cmp_expr()
    }

    // cmp_expr = add_expr (cmp_op add_expr)?
    fn cmp_expr(&mut self) -> anyhow::Result<Expr> {
        let left = self.add_expr()?;
        let op = match self.peek() {
            Tok::EqEq => BinOp::Eq,
            Tok::Ne    => BinOp::Ne,
            Tok::Lt    => BinOp::Lt,
            Tok::Le    => BinOp::Le,
            Tok::Gt    => BinOp::Gt,
            Tok::Ge    => BinOp::Ge,
            _ => return Ok(left),
        };
        self.advance();
        let right = self.add_expr()?;
        Ok(Expr::BinOp { op, left: Box::new(left), right: Box::new(right) })
    }

    // add_expr = mul_expr (('+' | '-') mul_expr)*
    fn add_expr(&mut self) -> anyhow::Result<Expr> {
        let mut left = self.mul_expr()?;
        loop {
            let op = match self.peek() {
                Tok::Plus  => BinOp::Add,
                Tok::Minus => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.mul_expr()?;
            left = Expr::BinOp { op, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    // mul_expr = unary_expr (('*' | '/') unary_expr)*
    fn mul_expr(&mut self) -> anyhow::Result<Expr> {
        let mut left = self.unary_expr()?;
        loop {
            let op = match self.peek() {
                Tok::Star  => BinOp::Mul,
                Tok::Slash => BinOp::Div,
                _ => break,
            };
            self.advance();
            let right = self.unary_expr()?;
            left = Expr::BinOp { op, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    // unary_expr = '-' unary_expr | primary
    fn unary_expr(&mut self) -> anyhow::Result<Expr> {
        if self.eat(&Tok::Minus) {
            let operand = self.unary_expr()?;
            return Ok(Expr::Unary { op: UnaryOp::Neg, operand: Box::new(operand) });
        }
        self.postfix()
    }

    // postfix: handles col[0] indexing after primary
    fn postfix(&mut self) -> anyhow::Result<Expr> {
        let mut expr = self.primary()?;
        loop {
            if self.eat(&Tok::LBracket) {
                let idx = self.or_expr()?;
                if !self.eat(&Tok::RBracket) {
                    anyhow::bail!("expected ']' after index expression");
                }
                expr = Expr::Index { expr: Box::new(expr), index: Box::new(idx) };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    // primary = literal | ident ['(' args ')'] | '(' expr ')'
    fn primary(&mut self) -> anyhow::Result<Expr> {
        match self.peek().clone() {
            Tok::Int(n) => { self.advance(); Ok(Expr::Int(n)) }
            Tok::Float(f) => { self.advance(); Ok(Expr::Float(f)) }
            Tok::Str(s) => { self.advance(); Ok(Expr::Str(s)) }
            Tok::LParen => {
                self.advance();
                let expr = self.or_expr()?;
                if !self.eat(&Tok::RParen) {
                    anyhow::bail!("expected ')'");
                }
                Ok(expr)
            }
            Tok::Ident(name) => {
                self.advance();
                // keywords
                match name.to_lowercase().as_str() {
                    "true"  => return Ok(Expr::Bool(true)),
                    "false" => return Ok(Expr::Bool(false)),
                    "null"  => return Ok(Expr::Null),
                    _       => {}
                }
                // function call?
                if self.eat(&Tok::LParen) {
                    let mut args = Vec::new();
                    if self.peek() != &Tok::RParen {
                        args.push(self.or_expr()?);
                        while self.eat(&Tok::Comma) {
                            args.push(self.or_expr()?);
                        }
                    }
                    if !self.eat(&Tok::RParen) {
                        anyhow::bail!("expected ')' after function arguments");
                    }
                    return Ok(Expr::Call { func: name.to_lowercase(), args });
                }
                // dot-chained column reference: `payload.user.id`
                let mut col = name;
                while self.eat(&Tok::Dot) {
                    let next = self.expect_ident()?;
                    col = format!("{col}.{next}");
                }
                Ok(Expr::Column(col))
            }
            Tok::EnvVar(name) => {
                self.advance();
                Ok(Expr::EnvVar(name))
            }
            other => anyhow::bail!("unexpected token in expression: {other:?}"),
        }
    }
}

/// Parse an expression string into an `Expr` AST.
pub fn parse(src: &str) -> anyhow::Result<Expr> {
    let tokens = tokenize(src)?;
    let mut p = Parser { tokens, pos: 0 };
    let expr = p.or_expr()?;
    if p.peek() != &Tok::Eof {
        anyhow::bail!("unexpected token after expression end: {:?}", p.peek());
    }
    Ok(expr)
}
