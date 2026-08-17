pub const CLUSTER_KEY_VERSION: &str = "v1";
pub const KEY_SEPARATOR: char = '|';
pub const SEGMENT_SEPARATOR: char = '/';
pub const WILDCARD_SEGMENT: &str = "*";
pub const UNKNOWN_SEGMENT: &str = "UNK";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Dim {
    Val(String),
    Unknown,
    Any,
}

impl Dim {
    fn as_segment(&self) -> String {
        match self {
            Self::Val(raw) => escape_segment(raw),
            Self::Unknown => UNKNOWN_SEGMENT.to_string(),
            Self::Any => WILDCARD_SEGMENT.to_string(),
        }
    }

    pub fn from_event_value(value: Option<&str>) -> Self {
        match value.map(str::trim).filter(|v| !v.is_empty()) {
            None => Self::Unknown,
            Some(v) if v == WILDCARD_SEGMENT || v.eq_ignore_ascii_case(UNKNOWN_SEGMENT) => {
                Self::Unknown
            }
            Some(v) => Self::Val(v.to_string()),
        }
    }
}

fn escape_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '|' => out.push_str("%7C"),
            '/' => out.push_str("%2F"),
            '*' => out.push_str("%2A"),
            _ => out.push(ch),
        }
    }
    out
}

fn unescape_segment(encoded: &str) -> Option<String> {
    let mut out = String::with_capacity(encoded.len());
    let mut chars = encoded.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            let hi = chars.next()?;
            let lo = chars.next()?;
            let byte = u8::from_str_radix(&format!("{hi}{lo}"), 16).ok()?;
            out.push(byte as char);
        } else {
            out.push(ch);
        }
    }
    Some(out)
}

fn parse_segment(s: &str) -> Option<Dim> {
    match s {
        WILDCARD_SEGMENT => Some(Dim::Any),
        UNKNOWN_SEGMENT => Some(Dim::Unknown),
        _ => unescape_segment(s).map(Dim::Val),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClusterKey {
    pub error_code: Dim,
    pub card_type: Dim,
    pub issuer: Dim,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeDepth {
    Root,
    Mid,
    Leaf,
}

impl ClusterKey {
    pub fn leaf(error_code: Dim, card_type: Dim, issuer: Dim) -> Self {
        Self {
            error_code,
            card_type,
            issuer,
        }
    }

    pub fn root(&self) -> Self {
        Self {
            error_code: self.error_code.clone(),
            card_type: Dim::Any,
            issuer: Dim::Any,
        }
    }

    pub fn mid(&self) -> Self {
        Self {
            error_code: self.error_code.clone(),
            card_type: self.card_type.clone(),
            issuer: Dim::Any,
        }
    }

    pub fn depth(&self) -> NodeDepth {
        match (&self.card_type, &self.issuer) {
            (Dim::Any, Dim::Any) => NodeDepth::Root,
            (_, Dim::Any) => NodeDepth::Mid,
            _ => NodeDepth::Leaf,
        }
    }

    pub fn chain(&self) -> Vec<ClusterKey> {
        match self.depth() {
            NodeDepth::Leaf => vec![self.root(), self.mid(), self.clone()],
            NodeDepth::Mid => vec![self.root(), self.clone()],
            NodeDepth::Root => vec![self.clone()],
        }
    }

    pub fn as_db(&self) -> String {
        let mut out = String::new();
        out.push_str(CLUSTER_KEY_VERSION);
        out.push(KEY_SEPARATOR);
        out.push_str(&self.error_code.as_segment());
        out.push(SEGMENT_SEPARATOR);
        out.push_str(&self.card_type.as_segment());
        out.push(SEGMENT_SEPARATOR);
        out.push_str(&self.issuer.as_segment());
        out
    }

    pub fn from_db(raw: &str) -> Option<Self> {
        let (version, rest) = raw.split_once(KEY_SEPARATOR)?;
        if version != CLUSTER_KEY_VERSION {
            return None;
        }
        let mut parts = rest.split(SEGMENT_SEPARATOR);
        let ec = parse_segment(parts.next()?)?;
        let ct = parse_segment(parts.next()?)?;
        let issuer = parse_segment(parts.next()?)?;
        Some(Self {
            error_code: ec,
            card_type: ct,
            issuer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Dim {
        Dim::Val(s.to_string())
    }

    #[test]
    fn leaf_roundtrips() {
        let key = ClusterKey::leaf(v("card_declined"), v("visa"), v("HDFC"));
        let raw = key.as_db();
        assert_eq!(raw, "v1|card_declined/visa/HDFC");
        assert_eq!(ClusterKey::from_db(&raw), Some(key));
    }

    #[test]
    fn chain_is_root_mid_leaf() {
        let chain = ClusterKey::leaf(v("card_declined"), v("visa"), v("HDFC")).chain();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].depth(), NodeDepth::Root);
        assert_eq!(chain[1].depth(), NodeDepth::Mid);
        assert_eq!(chain[2].depth(), NodeDepth::Leaf);
        assert_eq!(chain[0].as_db(), "v1|card_declined/*/*");
        assert_eq!(chain[1].as_db(), "v1|card_declined/visa/*");
        assert_eq!(chain[2].as_db(), "v1|card_declined/visa/HDFC");
    }

    #[test]
    fn delimiters_and_stars_are_percent_escaped() {
        let key = ClusterKey::leaf(v("05|bad/code"), v("pre*paid"), v("H|D*F/C"));
        let raw = key.as_db();
        assert!(raw.contains("%7C"));
        assert!(raw.contains("%2A"));
        assert!(raw.contains("%2F"));
        assert_eq!(ClusterKey::from_db(&raw), Some(key));
    }

    #[test]
    fn from_db_rejects_foreign_versions_and_wildcards() {
        assert!(ClusterKey::from_db("v2|a/b/c").is_none());
        assert_eq!(
            ClusterKey::from_db("v1|card_declined/*/*").map(|k| k.depth()),
            Some(NodeDepth::Root)
        );
    }

    #[test]
    fn from_event_value_normalizes_reserved_spellings() {
        assert_eq!(Dim::from_event_value(Some("  ")), Dim::Unknown);
        assert_eq!(Dim::from_event_value(None), Dim::Unknown);
        assert_eq!(Dim::from_event_value(Some("*")), Dim::Unknown);
        assert_eq!(Dim::from_event_value(Some("unk")), Dim::Unknown);
        assert_eq!(Dim::from_event_value(Some("HDFC")), Dim::Val("HDFC".into()));
    }
}
