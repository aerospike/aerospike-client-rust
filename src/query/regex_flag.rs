//! Regex Bit Flags
/// Used to change the Regex Mode in Filters
pub enum RegexFlag {
    /// Use regex defaults.
    NONE = 0,
    /// Use POSIX Extended Regular Expression syntax when interpreting regex.
    EXTENDED = 1,
    /// Do not differentiate case.
    ICASE = 2,
    /// Do not report position of matches.
    NOSUB = 3,
    /// Match-any-character operators don't match a newline.
    NEWLINE = 8,
}
