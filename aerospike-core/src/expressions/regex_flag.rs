//! Regex Bit Flags
/// Used to change the Regex Mode in Filters
pub enum RegexFlag {
    /// Use regex defaults.
    None = 0,
    /// Use POSIX Extended Regular Expression syntax when interpreting regex.
    Extended = 1,
    /// Do not differentiate case.
    Icase = 2,
    /// Do not report position of matches.
    Nosub = 4,
    /// Match-any-character operators don't match a newline.
    Newline = 8,
}
