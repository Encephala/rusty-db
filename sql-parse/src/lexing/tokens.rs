pub enum Token {
    Whitespace,
    Select,
    Column(ColumnIdentifier),
    From,
    Table(String),
    Semicolon,
}

pub enum ColumnIdentifier {
    Asterisk,
    Identifier(Vec<String>),
}
