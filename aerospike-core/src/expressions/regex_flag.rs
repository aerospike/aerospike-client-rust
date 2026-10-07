//! Regex flags for [`regex_compare`](super::regex_compare).

crate::flags::bit_flags! {
    /// Flags that change how [`regex_compare`](super::regex_compare) interprets
    /// and matches its pattern. Combine them with `|`.
    ///
    /// ```
    /// use aerospike::RegexFlags;
    ///
    /// let flags = RegexFlags::ICASE | RegexFlags::NEWLINE;
    /// assert!(flags.contains(RegexFlags::ICASE));
    /// assert!(!flags.contains(RegexFlags::NOSUB));
    /// ```
    pub struct RegexFlags(i64);
    /// Use regex defaults.
    const NONE = 0;
    /// Use POSIX Extended Regular Expression syntax when interpreting the regex.
    const EXTENDED = 1;
    /// Do not differentiate case.
    const ICASE = 2;
    /// Do not report the position of matches.
    const NOSUB = 4;
    /// Match-any-character operators don't match a newline.
    const NEWLINE = 8;
}

#[cfg(test)]
mod tests {
    use super::RegexFlags;

    #[test]
    fn combines_and_reports_bits() {
        let mut f = RegexFlags::ICASE | RegexFlags::NEWLINE;
        assert_eq!(f.bits(), 10);
        assert!(f.contains(RegexFlags::ICASE));
        assert!(!f.contains(RegexFlags::EXTENDED));
        f |= RegexFlags::NOSUB;
        assert_eq!(f.bits(), 14);
        assert_eq!(RegexFlags::default(), RegexFlags::NONE);
        // A bit this client does not name still travels.
        let future = RegexFlags::ICASE | RegexFlags::from_bits(0x40);
        assert_eq!(future.bits(), 0x42);
        assert!(future.contains(RegexFlags::ICASE));
    }
}
